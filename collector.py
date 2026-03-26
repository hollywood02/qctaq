"""Phase 1: Collect case URLs from CanLII month listing pages using nodriver."""
import asyncio
import os
import random

import nodriver as uc

import db
from config import TRIBUNALS, DB_PATH, url_to_pdf_url, url_to_case_id


async def collect_month(tab, tribunal_code: str, year: int, month: int) -> int:
    """Collect all case URLs from a single month listing page. Returns count of new cases."""
    tribunal = TRIBUNALS[tribunal_code]
    url = tribunal["url_pattern"].format(year=year, month=month)

    await tab.get(url)
    await asyncio.sleep(3)

    # Check for block/captcha
    page_text = await tab.evaluate("document.body.innerText || ''", return_by_value=True)
    if page_text and any(w in str(page_text).lower() for w in ["captcha", "blocked", "restricted"]):
        print(f"  BLOCKED on {year}/{month}. Solve the CAPTCHA in the browser...")
        for i in range(10, 0, -1):
            await asyncio.sleep(1)
            if i % 10 == 0:
                print(f"    {i}s remaining...")
        # Retry
        await tab.get(url)
        await asyncio.sleep(3)

    # Scroll to trigger lazy loading
    total_height = await tab.evaluate("document.body.scrollHeight", return_by_value=True) or 1000
    current = 0
    step = 500
    while current < total_height:
        await tab.evaluate(f"window.scrollTo(0, {current})")
        await asyncio.sleep(0.4)
        current += step
        total_height = await tab.evaluate("document.body.scrollHeight", return_by_value=True) or total_height

    # Click "Show More" until no new results
    prev_count = 0
    stale_clicks = 0
    while stale_clicks < 3:
        links = await tab.query_selector_all(tribunal["case_selector"])
        current_count = len(links) if links else 0

        if current_count > prev_count:
            stale_clicks = 0
            prev_count = current_count
        else:
            stale_clicks += 1

        try:
            show_more = await tab.query_selector(tribunal["show_more_selector"])
            if show_more:
                await show_more.click()
                await asyncio.sleep(2)
            else:
                break
        except Exception:
            break

    # Extract all case URLs
    links = await tab.query_selector_all(tribunal["case_selector"])
    cases = []
    seen_urls = set()
    for link in (links or []):
        try:
            href = link.attrs.get("href", "") if hasattr(link, 'attrs') and link.attrs else ""
            if not href:
                href = await link.apply("(el) => el.href") or ""
            title = link.text or ""
        except Exception:
            continue
        if href and "/doc/" in href:
            # Ensure full URL (href may be relative)
            if href.startswith("/"):
                href = "https://www.canlii.org" + href
            if href not in seen_urls:
                seen_urls.add(href)
                case_id = url_to_case_id(href)
                cases.append({
                    "tribunal": tribunal_code,
                    "year": year,
                    "month": month,
                    "case_id": case_id,
                    "title": title.strip(),
                    "url": href,
                    "pdf_url": url_to_pdf_url(href),
                })

    if cases:
        db.insert_cases(DB_PATH, cases)
        # Fix month=0 for cases that were imported without month info
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        urls = [c["url"] for c in cases]
        for url in urls:
            conn.execute("UPDATE cases SET month=? WHERE url=? AND month=0", (month, url))
        conn.commit()
        conn.close()
        db.mark_month_collected(DB_PATH, tribunal_code, year, month, len(cases))
        db.log_event(DB_PATH, "collect_month", f"{tribunal_code} {year}/{month}: {len(cases)} cases")
    else:
        # Don't mark as collected if 0 cases -- might be a block
        db.log_event(DB_PATH, "collect_month", f"{tribunal_code} {year}/{month}: 0 cases (possibly blocked)")
    return len(cases)


async def collect_tribunal(tribunal_code: str, start_year: int = None, end_year: int = None,
                           force: bool = False):
    """Collect all uncollected months for a tribunal.

    Goes downwards (newest first). With force=True, re-verifies already collected months
    and only skips if the DB case count matches what CanLII shows.
    """
    tribunal = TRIBUNALS[tribunal_code]
    yr_start, yr_end = tribunal["year_range"]
    if start_year:
        yr_start = start_year
    if end_year:
        yr_end = end_year

    db.init_db(DB_PATH)

    mode = "FULL VERIFY" if force else "new only"
    print(f"Collecting URLs for {tribunal['name']} ({yr_start}-{yr_end}) [{mode}]")
    print("A browser will open. Solve the slider if prompted.\n")

    # Use Patchright's bundled Chromium if no system Chrome is found
    chrome_path = None
    pw_browsers = os.path.expanduser("~/Library/Caches/ms-playwright")
    if os.path.isdir(pw_browsers):
        for entry in sorted(os.listdir(pw_browsers), reverse=True):
            if entry.startswith("chromium-"):
                candidate = os.path.join(
                    pw_browsers, entry, "chrome-mac-arm64",
                    "Google Chrome for Testing.app", "Contents", "MacOS",
                    "Google Chrome for Testing",
                )
                if os.path.isfile(candidate):
                    chrome_path = candidate
                    break
    browser = await uc.start(
        browser_executable_path=chrome_path,
        browser_args=["--no-sandbox"],
    )
    tab = browser.main_tab

    # Initial navigation to solve DataDome
    await tab.get("https://www.canlii.org/qc/qctaq/")
    print("Waiting 5s for initial DataDome challenge...")
    for i in range(5, 0, -1):
        await asyncio.sleep(1)
        if i % 10 == 0:
            print(f"  {i}s remaining...")

    from datetime import datetime
    now = datetime.now()

    total_new = 0
    skipped = 0
    for year in range(yr_end, yr_start - 1, -1):
        for month in range(12, 0, -1):
            # Skip future months
            if year > now.year or (year == now.year and month > now.month):
                continue

            if not force and db.is_month_collected(DB_PATH, tribunal_code, year, month):
                continue

            # In force mode, check if DB already has cases for this month
            if force and db.is_month_collected(DB_PATH, tribunal_code, year, month):
                import sqlite3
                conn = sqlite3.connect(DB_PATH)
                stored_count = conn.execute(
                    "SELECT case_count FROM months_collected WHERE tribunal=? AND year=? AND month=?",
                    (tribunal_code, year, month)
                ).fetchone()
                conn.close()
                stored = stored_count[0] if stored_count else 0
                if stored > 0:
                    skipped += 1
                    continue
                print(f"  {year}/{month:02d}... RE-COLLECT (was marked but 0 cases)", end=" ", flush=True)
            else:
                print(f"  {year}/{month:02d}...", end=" ", flush=True)

            try:
                count = await collect_month(tab, tribunal_code, year, month)
                total_new += count
                print(f"{count} cases")
            except Exception as e:
                print(f"ERROR: {e}")
                db.log_event(DB_PATH, "error", f"collect {year}/{month}: {e}")

            await asyncio.sleep(random.uniform(5, 15))

    print(f"\nDone. Collected {total_new} new case URLs. Skipped {skipped} verified months.")


def run_collector(tribunal: str, start_year: int = None, end_year: int = None, force: bool = False):
    uc.loop().run_until_complete(collect_tribunal(tribunal, start_year, end_year, force=force))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--tribunal", default="taq")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    args = parser.parse_args()
    run_collector(args.tribunal, args.start_year, args.end_year)
