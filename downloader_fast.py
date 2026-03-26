"""Phase 2 (fast): Optimized browser-based PDF downloads + auto VPN rotation.

Same approach as downloader.py (browser fetch) but optimized:
- Proactive VPN rotation every 18 PDFs
- Minimal delays between downloads (0.15-0.35s vs 0.2-0.5s)
- Faster VPN rotation recovery (1s vs 3-10s waits)
- No 15s startup wait
- Auto captcha solving via Gemini OCR
"""
import base64
import os
import random
import time

from patchright.sync_api import sync_playwright

import db
from captcha_solver import CaptchaSolver
from config import DB_PATH, PDF_BASE_DIR
from vpn import (
    BATCH_SIZE_BEFORE_ROTATE,
    detect_vpn_mode, get_vpn_configs,
    vpn_switch, vpn_down, bring_down_all_vpns,
    is_valid_pdf, is_block_response, build_pdf_path,
    set_vpn_mode, record_real_ip, get_real_ip, verify_vpn_ip,
    start_sudo_keepalive,
)

BATCH_SIZE = 18  # how many to fetch from DB per query
TRIBUNAL_URL = "https://www.canlii.org/qc/qctaq/"


def detect_and_solve_captcha(page, solver, max_attempts=3, force=False) -> bool:
    """Detect if a captcha is present and solve it with Gemini.

    Returns True if captcha was detected (solved or not), False if no captcha.
    If force=True, skip text detection and go straight to screenshot+Gemini.
    """
    for attempt in range(max_attempts):
        if not force:
            # Check page content for captcha indicators
            try:
                body_text = page.evaluate("document.body?.innerText || ''")
            except Exception:
                return False

            has_captcha = any(marker in body_text.lower() for marker in [
                "access is temporarily restricted",
                "unusual activity",
                "captcha",
                "security check",
                "restricted",
            ])

            if not has_captcha:
                return attempt > 0
        force = False  # only force the first attempt

        print(f"  CAPTCHA detected! Solving with Gemini (attempt {attempt + 1}/{max_attempts})...")

        # Dismiss cookie banner if present (it covers the ok button)
        try:
            for cookie_sel in [
                "button:has-text('Accept all cookies')",
                "button:has-text('Accept all')",
                "button:has-text('Accepter')",
                ".accept-all-cookies",
            ]:
                btn = page.locator(cookie_sel).first
                if btn.is_visible(timeout=500):
                    btn.click()
                    time.sleep(0.5)
                    break
        except Exception:
            pass

        # Take screenshot
        screenshot_path = os.path.join(os.getcwd(), "captcha_auto_solve.png")
        try:
            page.screenshot(path=screenshot_path)
        except Exception as e:
            print(f"  Screenshot failed: {e}")
            return True

        # Send to Gemini OCR
        text = solver.solve_visually(screenshot_path)
        if not text:
            print("  Gemini returned no text, refreshing...")
            page.reload()
            time.sleep(3)
            continue

        if text.upper() in ("NOCAPTCHA",):
            print("  Gemini says no captcha found, refreshing...")
            page.reload()
            time.sleep(3)
            continue

        print(f"  Gemini OCR: '{text}'")

        # Find input box and type the answer
        input_filled = page.evaluate("""
            (text) => {
                const selectors = [
                    "input[name='captcha']", "input[name='captchaCode']",
                    "input[id*='captcha']", "input[name*='captcha']",
                    "input[name='response']",
                    "#captcha-input", ".captcha-input",
                    "input[type='text']",
                    "form input:not([type='hidden']):not([type='submit']):not([type='button']):not([type='checkbox'])",
                ];
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el && el.offsetParent !== null) {
                        el.value = '';
                        el.value = text;
                        el.dispatchEvent(new Event('input', {bubbles: true}));
                        el.dispatchEvent(new Event('change', {bubbles: true}));
                        return true;
                    }
                }
                return false;
            }
        """, text)

        if not input_filled:
            print("  Could not find captcha input box")
            page.reload()
            time.sleep(3)
            continue

        # Click the OK/submit button using Playwright's native click
        clicked = False
        for selector in [
            "input[value='ok']", "input[value='OK']", "input[value='Ok']",
            "button:has-text('ok')", "button:has-text('OK')",
            "input[type='submit']", "button[type='submit']",
        ]:
            try:
                btn = page.locator(selector).first
                if btn.is_visible(timeout=1000):
                    btn.click()
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            # Fallback: submit form via JS
            page.evaluate("document.querySelector('form')?.submit()")

        time.sleep(3)  # wait for page to reload after submit

    return True  # captcha was present but we may not have solved it


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
            print(f"    FETCH ERROR {case_id}: {e}")
            db.mark_case_failed(db_path, case_db_id, str(e))
            continue

        if result.get("ok"):
            pdf_data = base64.b64decode(result["data"])
            target = build_pdf_path(PDF_BASE_DIR, case["tribunal"],
                                    case["year"], case["month"], case_id,
                                    title=case.get("title"))
            try:
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with open(target, "wb") as f:
                    f.write(pdf_data)
            except OSError as e:
                print(f"    DISK ERROR {case_id}: {e}")
                db.mark_case_failed(db_path, case_db_id, f"disk write error: {e}")
                continue
            db.mark_case_done(db_path, case_db_id, target, len(pdf_data))
            success += 1
            db.log_event(db_path, "download", f"{case_id}.pdf")

        elif result.get("status") == 404:
            print(f"    404 {case_id}")
            db.mark_case_no_pdf(db_path, case_db_id)
            no_pdf += 1

        elif result.get("status") in (403, 429):
            print(f"    BLOCKED {case_id}: {result.get('status')}")
            return success, no_pdf, True  # was_blocked

        else:
            reason = result.get("reason", f"status {result.get('status')}")
            print(f"    FAIL {case_id}: {reason}")
            db.mark_case_failed(db_path, case_db_id, reason)

        time.sleep(random.uniform(0.15, 0.35))  # moderate delay -- too fast triggers captchas

    return success, no_pdf, False


def run_downloader_fast(tribunal: str, **kwargs):
    """Optimized download loop with auto captcha solving."""

    db_path = DB_PATH
    db.init_db(db_path)
    print("Syncing DB with disk...")
    db.sync_with_disk(db_path, PDF_BASE_DIR, tribunal)
    db.reset_downloading(db_path)

    bring_down_all_vpns()
    start_sudo_keepalive()

    # Record bare-metal IP BEFORE connecting any VPN
    print("Recording real IP (before VPN)...")
    real_ip = record_real_ip()
    if real_ip:
        print(f"  Real IP: {real_ip} (will refuse to scrape on this IP)")
    else:
        print("  WARNING: Could not determine real IP. Will still verify VPN changes IP.")

    configs = get_vpn_configs()
    if not configs:
        print("ERROR: No VPN configs found. Refusing to run without VPN (protects your real IP).")
        return

    vpn_mode = detect_vpn_mode(configs) if configs[0] else "none"
    set_vpn_mode(vpn_mode)
    if vpn_mode == "openvpn":
        configs = [c for c in configs if c.endswith(".ovpn")]
    elif vpn_mode == "wireguard":
        configs = [c for c in configs if c.endswith(".conf")]

    config_idx = 0
    current_conf = None
    tribunal_url = TRIBUNAL_URL

    # Initialize Gemini captcha solver
    solver = CaptchaSolver()
    if not solver.api_key:
        print("WARNING: No GOOGLE_API_KEY found. Captcha auto-solve disabled.")
        print("Set it in .env or environment to enable auto-solving.\n")

    mode_label = "OpenVPN" if vpn_mode == "openvpn" else "WireGuard" if vpn_mode == "wireguard" else "No VPN"
    print(f"=== Fast Downloader — Patchright + {mode_label} + Gemini captcha solver ===\n")

    # Connect to first working VPN
    if configs[0]:
        random.shuffle(configs)
        for i, conf in enumerate(configs):
            print(f"Trying VPN {i+1}/{len(configs)}: {os.path.basename(conf)}")
            if vpn_switch(None, conf):
                current_conf = conf
                config_idx = i
                print("  VPN connected + IP verified!")
                break
            print(f"  Skipping {os.path.basename(conf)} (no connectivity or IP unchanged)")
        else:
            print("ERROR: All VPN configs failed. Cannot proceed.")
            return

    # Final safety check -- refuse to run on real IP
    vpn_ip = verify_vpn_ip(timeout=5)
    if not vpn_ip:
        current_ip_check = get_real_ip()
        print(f"ERROR: VPN not active — still on real IP ({current_ip_check}). Aborting.")
        if current_conf:
            vpn_down(current_conf)
        return
    print(f"  Confirmed VPN IP: {vpn_ip}")

    # Launch browser
    print("\nOpening browser...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.goto(tribunal_url, timeout=15000)
        time.sleep(2)

        # No captcha check on initial load -- will trigger if 0 downloads returned

        print(f"\nDownloading: {BATCH_SIZE} PDFs per IP, then rotate proactively")
        print(f"VPN configs: {len(configs)} servers ({mode_label})\n")

        db.set_control(db_path, "state", "running")
        db.log_event(db_path, "resume", f"Fast downloader started: {tribunal}")

        session_start = time.time()
        session_count = 0
        last_session_count = 0
        stuck_count = 0
        rotations = 0
        ip_download_count = 0

        def rotate_vpn():
            """Rotate VPN and open fresh browser context. Returns (new_page, success)."""
            nonlocal current_conf, config_idx, rotations, page, ip_download_count
            ip_download_count = 0

            if configs[0] is None:
                return page, True

            rotated = False
            for _attempt in range(len(configs)):
                config_idx = (config_idx + 1) % len(configs)
                next_conf = configs[config_idx]
                if vpn_switch(current_conf, next_conf):
                    current_conf = next_conf
                    rotations += 1
                    rotated = True
                    break
                print(f"  Skipping {os.path.basename(next_conf)} (no connectivity or IP unchanged)")
                current_conf = None
            if not rotated:
                print("ERROR: No working VPN server found. Stopping.")
                return page, False

            # Double-check we're not on our real IP after rotation
            rotation_ip = verify_vpn_ip(timeout=5)
            if not rotation_ip:
                print("ERROR: VPN rotation failed — still on real IP. Stopping to protect you.")
                return page, False

            # Fresh browser context on new IP
            page.close()
            context = browser.new_context()
            page = context.new_page()

            print("  Loading tribunal page...", end=" ", flush=True)
            try:
                page.goto(tribunal_url, timeout=15000)
                print("ok")
            except Exception:
                print("retry...", end=" ", flush=True)
                time.sleep(2)
                try:
                    page.goto(tribunal_url, timeout=15000)
                    print("ok")
                except Exception:
                    print("failed (continuing anyway)")
            time.sleep(1)
            return page, True

        try:
            while True:
                state = db.get_control(db_path, "state")
                if state == "paused":
                    time.sleep(1)
                    continue
                if state == "idle":
                    break

                # Read year range + direction from DB controls
                yr_start = db.get_control(db_path, "year_start")
                yr_end = db.get_control(db_path, "year_end")
                direction = db.get_control(db_path, "direction") or "desc"
                cases = db.get_pending_cases(
                    db_path, tribunal, limit=BATCH_SIZE,
                    year_start=int(yr_start) if yr_start else None,
                    year_end=int(yr_end) if yr_end else None,
                    direction=direction,
                )
                if not cases:
                    print("\nNo more pending cases. Done!")
                    break

                print(f"  Fetching {len(cases)} PDFs ({cases[0]['year']}/{cases[0]['month']:02d})...", flush=True)
                success, no_pdf, was_blocked = download_batch(page, cases, db_path)
                session_count += success
                ip_download_count += success

                elapsed = time.time() - session_start
                rate = session_count / (elapsed / 3600) if elapsed > 0 else 0
                stats = db.get_tribunal_stats(db_path, tribunal)
                print(f"  [{tribunal}] {stats['done']}/{stats['total']} done | "
                      f"{session_count} this session | {rate:.0f}/hr | "
                      f"IP: {ip_download_count} | rotations: {rotations}")

                # Rotate VPN only when blocked or hitting the per-IP limit
                need_rotate = was_blocked or ip_download_count >= BATCH_SIZE_BEFORE_ROTATE

                if need_rotate:
                    page, ok = rotate_vpn()
                    if not ok:
                        break

        except KeyboardInterrupt:
            print("\nStopping...")
        finally:
            db.set_control(db_path, "state", "idle")
            elapsed = time.time() - session_start
            rate = session_count / (elapsed / 3600) if elapsed > 0 else 0
            print(f"\nSession: {session_count} downloads in {elapsed/60:.1f}m ({rate:.0f}/hr)")
            print(f"VPN rotations: {rotations}")
            db.log_event(db_path, "stop", f"Fast downloader stopped. {session_count} downloads.")
            browser.close()

            if current_conf:
                vpn_down(current_conf)
                print("VPN disconnected.")
