"""Phase 2: Download PDFs via Patchright + auto VPN IP rotation (OpenVPN or WireGuard)."""
import base64
import os
import random
import time

from patchright.sync_api import sync_playwright

import db
from config import DB_PATH, PDF_BASE_DIR
from vpn import (
    BATCH_SIZE_BEFORE_ROTATE,
    detect_vpn_mode, get_vpn_configs,
    vpn_switch, vpn_down, bring_down_all_vpns,
    is_valid_pdf, is_block_response, build_pdf_path,
    set_vpn_mode,
)

TRIBUNAL_URL = "https://www.canlii.org/qc/qctaq/"


def download_batch(page, cases: list, db_path: str) -> tuple[int, int, bool]:
    """Download a batch of PDFs via page.evaluate fetch. Returns (success, no_pdf, was_blocked)."""
    success = 0
    no_pdf = 0

    for case in cases:
        pdf_url = case["pdf_url"]
        case_id = case["case_id"]
        case_db_id = case["id"]

        try:
            result = page.evaluate("""
                async (url) => {
                    try {
                        const resp = await fetch(url);
                        if (resp.status === 404) return {ok: false, status: 404};
                        if (!resp.ok) return {ok: false, status: resp.status};
                        const blob = await resp.blob();
                        const buffer = await blob.arrayBuffer();
                        const arr = new Uint8Array(buffer);
                        if (arr.length < 5 || String.fromCharCode(arr[0],arr[1],arr[2],arr[3],arr[4]) !== '%PDF-')
                            return {ok: false, status: resp.status, reason: 'not_pdf'};
                        let binary = '';
                        for (let i = 0; i < arr.length; i += 8192) {
                            binary += String.fromCharCode.apply(null, arr.subarray(i, i + 8192));
                        }
                        return {ok: true, data: btoa(binary), size: arr.length};
                    } catch(e) {
                        return {ok: false, status: 0, reason: e.message};
                    }
                }
            """, pdf_url)
        except Exception as e:
            db.mark_case_failed(db_path, case_db_id, str(e))
            continue

        if result.get("ok"):
            pdf_data = base64.b64decode(result["data"])
            target = build_pdf_path(PDF_BASE_DIR, case["tribunal"],
                                    case["year"], case["month"], case_id,
                                    title=case.get("title"))
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "wb") as f:
                f.write(pdf_data)
            db.mark_case_done(db_path, case_db_id, target, len(pdf_data))
            success += 1
            db.log_event(db_path, "download", f"{case_id}.pdf")

        elif result.get("status") == 404:
            db.mark_case_no_pdf(db_path, case_db_id)
            no_pdf += 1

        elif result.get("status") in (403, 429):
            # Blocked -- return remaining to pending
            conn = db._connect(db_path)
            conn.execute("UPDATE cases SET status='pending' WHERE id=?", (case_db_id,))
            conn.commit()
            conn.close()
            return success, no_pdf, True  # was_blocked

        else:
            db.mark_case_failed(db_path, case_db_id,
                                result.get("reason", f"status {result.get('status')}"))

        time.sleep(random.uniform(0.2, 0.5))  # tiny delay between downloads

    return success, no_pdf, False


# -- Main download loop --

def run_downloader(tribunal: str, workers: int = 1, delay: float = 0.3, batch_size: int = 100):
    """Main download loop with auto VPN rotation."""

    db_path = DB_PATH
    db.init_db(db_path)
    db.reset_downloading(db_path)

    # Clean up any stuck VPN from previous crashed runs
    bring_down_all_vpns()

    configs = get_vpn_configs()
    if not configs:
        print("No VPN configs found. Run without VPN rotation.")
        configs = [None]

    vpn_mode = detect_vpn_mode(configs) if configs[0] else "none"
    set_vpn_mode(vpn_mode)
    # Filter to only the relevant file type
    if vpn_mode == "openvpn":
        configs = [c for c in configs if c.endswith(".ovpn")]
    elif vpn_mode == "wireguard":
        configs = [c for c in configs if c.endswith(".conf")]

    config_idx = 0
    current_conf = None

    mode_label = "OpenVPN" if vpn_mode == "openvpn" else "WireGuard" if vpn_mode == "wireguard" else "No VPN"
    print(f"=== QCTAQ Scraper — Patchright + {mode_label} ===\n")

    # Connect to first working VPN -- try multiple configs
    if configs[0]:
        random.shuffle(configs)  # don't always hammer the same server
        for i, conf in enumerate(configs):
            print(f"Trying VPN {i+1}/{len(configs)}: {os.path.basename(conf)}")
            if vpn_switch(None, conf):
                current_conf = conf
                config_idx = configs.index(conf)
                print("  VPN connected + internet verified!")
                break
            print(f"  Skipping {os.path.basename(conf)} (no connectivity)")
        else:
            print("WARNING: All VPN configs failed. Cannot proceed safely.")
            print("Check your Proton VPN subscription or regenerate configs.")
            return

    # Launch browser
    print("\nOpening browser... solve the slider when it appears.")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.goto(TRIBUNAL_URL)
        print("Waiting 15s — solve the slider...")
        time.sleep(15)
        print(f"  Page: {page.title()}")

        print(f"\nDownloading: {BATCH_SIZE_BEFORE_ROTATE} PDFs per IP, then auto-rotate")
        print(f"VPN configs: {len(configs)} servers ({mode_label})\n")

        db.set_control(db_path, "state", "running")
        db.log_event(db_path, "resume", f"Downloader started: {tribunal}")

        session_start = time.time()
        session_count = 0
        rotations = 0

        try:
            while True:
                state = db.get_control(db_path, "state")
                if state == "paused":
                    time.sleep(1)
                    continue
                if state == "idle":
                    break

                # Get next batch
                cases = db.get_pending_cases(db_path, tribunal, limit=BATCH_SIZE_BEFORE_ROTATE)
                if not cases:
                    print("\nNo more pending cases. Done!")
                    break

                # Download batch
                success, no_pdf, was_blocked = download_batch(page, cases, db_path)
                session_count += success

                elapsed = time.time() - session_start
                rate = session_count / (elapsed / 3600) if elapsed > 0 else 0
                stats = db.get_tribunal_stats(db_path, tribunal)
                print(f"  [{tribunal}] {stats['done']}/{stats['total']} done | "
                      f"{session_count} this session | {rate:.0f}/hr | "
                      f"rotations: {rotations}")

                # Rotate VPN + fresh browser context
                if configs[0] is not None:
                    # Try up to len(configs) servers to find one that works
                    rotated = False
                    for _attempt in range(len(configs)):
                        config_idx = (config_idx + 1) % len(configs)
                        next_conf = configs[config_idx]
                        if vpn_switch(current_conf, next_conf):
                            current_conf = next_conf
                            rotations += 1
                            rotated = True
                            break
                        print(f"  Skipping {os.path.basename(next_conf)} (no connectivity)")
                        current_conf = None  # already brought down in vpn_switch rollback
                    if not rotated:
                        print("ERROR: No working VPN server found. Stopping.")
                        break

                    # Close old page, create fresh context (clears all cookies)
                    page.close()
                    context = browser.new_context()
                    page = context.new_page()

                    # Navigate with clean cookies on new IP
                    try:
                        page.goto(TRIBUNAL_URL, timeout=15000)
                    except Exception:
                        # Timeout on navigation -- try again
                        time.sleep(3)
                        try:
                            page.goto(TRIBUNAL_URL, timeout=15000)
                        except Exception:
                            pass
                    time.sleep(3)
                    # Check if slider appeared
                    try:
                        title = page.title()
                    except Exception:
                        title = ""
                    if "canlii" not in title.lower() and "tribunal" not in title.lower():
                        print("  Slider on new IP — waiting 10s to solve...")
                        time.sleep(10)

        except KeyboardInterrupt:
            print("\nStopping...")
        finally:
            db.set_control(db_path, "state", "idle")
            elapsed = time.time() - session_start
            rate = session_count / (elapsed / 3600) if elapsed > 0 else 0
            print(f"\nSession: {session_count} downloads in {elapsed/60:.1f}m ({rate:.0f}/hr)")
            print(f"VPN rotations: {rotations}")
            db.log_event(db_path, "resume", f"Downloader stopped. {session_count} downloads.")
            browser.close()

            # Disconnect VPN
            if current_conf:
                vpn_down(current_conf)
                print("VPN disconnected.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--tribunal", default="taq")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--delay", type=float, default=0.3)
    args = parser.parse_args()
    run_downloader(args.tribunal, args.workers, args.delay)
