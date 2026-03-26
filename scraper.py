#!/usr/bin/env python3
"""QCTAQ Case Law Scraper — CLI entry point."""
import argparse
import sys

import db
from config import DB_PATH, TRIBUNALS


def cmd_collect(args):
    from collector import run_collector
    run_collector(args.tribunal, args.start_year, args.end_year, force=args.force)


def cmd_download(args):
    if args.fast:
        if args.workers and args.workers > 1:
            print("WARNING: --workers is ignored with --fast (single-process)")
        from downloader_fast import run_downloader_fast
        run_downloader_fast(args.tribunal)
    else:
        from downloader import run_downloader
        run_downloader(args.tribunal, args.workers, args.delay)


def cmd_run(args):
    from collector import run_collector
    print("=== Phase 1: Collecting URLs ===")
    run_collector(args.tribunal, args.start_year, args.end_year)
    print("\n=== Phase 2: Downloading PDFs ===")
    if args.fast:
        from downloader_fast import run_downloader_fast
        run_downloader_fast(args.tribunal)
    else:
        from downloader import run_downloader
        run_downloader(args.tribunal, args.workers, args.delay)


def cmd_dashboard(args):
    from dashboard import app
    from config import DASHBOARD_PORT
    app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=False)


def cmd_status(args):
    from config import PDF_BASE_DIR
    db.init_db(DB_PATH)
    db.sync_with_disk(DB_PATH, PDF_BASE_DIR, args.tribunal if hasattr(args, 'tribunal') else 'taq')
    stats = db.get_all_stats(DB_PATH)
    if not stats:
        print("No data yet. Run 'collect' first.")
        return
    print("=" * 60)
    print("QCTAQ CASE LAW SCRAPER — STATUS")
    print("=" * 60)
    grand_total = 0
    grand_done = 0
    for tribunal, s in sorted(stats.items()):
        total = s["total"]
        done = s["done"]
        pct = (done / total * 100) if total > 0 else 0
        grand_total += total
        grand_done += done
        print(f"  {tribunal.upper():6s}  {done:>8,} / {total:>8,}  ({pct:.1f}%)  "
              f"pending={s['pending']:,}  failed={s['failed']:,}  no_pdf={s['no_pdf']:,}")
    if grand_total > 0:
        pct = grand_done / grand_total * 100
        print(f"  {'TOTAL':6s}  {grand_done:>8,} / {grand_total:>8,}  ({pct:.1f}%)")
    print()


def cmd_retry(args):
    db.init_db(DB_PATH)
    count = db.retry_failed(DB_PATH, args.tribunal)
    print(f"Reset {count} failed cases to pending for {args.tribunal}")


def main():
    parser = argparse.ArgumentParser(description="QCTAQ Case Law Scraper")
    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("collect", help="Collect case URLs from CanLII")
    p.add_argument("--tribunal", default="taq", choices=TRIBUNALS.keys())
    p.add_argument("--start-year", type=int)
    p.add_argument("--end-year", type=int)
    p.add_argument("--force", action="store_true", help="Re-verify all months, not just uncollected")

    p = sub.add_parser("download", help="Download pending PDFs")
    p.add_argument("--tribunal", default="taq", choices=TRIBUNALS.keys())
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--delay", type=float, default=2.0)
    p.add_argument("--fast", action="store_true", help="Use direct HTTP (no browser)")

    p = sub.add_parser("run", help="Collect URLs then download PDFs")
    p.add_argument("--tribunal", default="taq", choices=TRIBUNALS.keys())
    p.add_argument("--start-year", type=int)
    p.add_argument("--end-year", type=int)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--delay", type=float, default=2.0)
    p.add_argument("--fast", action="store_true", help="Use direct HTTP (no browser)")

    sub.add_parser("dashboard", help="Launch web dashboard")
    sub.add_parser("status", help="Show download status")

    p = sub.add_parser("retry", help="Retry failed downloads")
    p.add_argument("--tribunal", default="taq", choices=TRIBUNALS.keys())

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    commands = {
        "collect": cmd_collect,
        "download": cmd_download,
        "run": cmd_run,
        "dashboard": cmd_dashboard,
        "status": cmd_status,
        "retry": cmd_retry,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
