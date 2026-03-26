import os
import re
import sqlite3

MAX_RETRIES = 3


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(db_path: str):
    conn = _connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cases (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            tribunal        TEXT NOT NULL,
            year            INTEGER NOT NULL,
            month           INTEGER NOT NULL,
            case_id         TEXT NOT NULL,
            title           TEXT,
            url             TEXT NOT NULL UNIQUE,
            pdf_url         TEXT NOT NULL,
            pdf_path        TEXT,
            status          TEXT NOT NULL DEFAULT 'pending',
            file_size       INTEGER,
            retry_count     INTEGER NOT NULL DEFAULT 0,
            last_error      TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            downloaded_at   TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(tribunal, status);
        CREATE INDEX IF NOT EXISTS idx_cases_tribunal_year ON cases(tribunal, year, month);
        CREATE TABLE IF NOT EXISTS months_collected (
            tribunal        TEXT NOT NULL,
            year            INTEGER NOT NULL,
            month           INTEGER NOT NULL,
            case_count      INTEGER,
            collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tribunal, year, month)
        );
        CREATE TABLE IF NOT EXISTS events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT NOT NULL,
            message         TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS control (
            key             TEXT PRIMARY KEY,
            value           TEXT
        );
    """)
    for key, default in [("state", "idle"), ("workers", "3"), ("tribunal", "taq")]:
        conn.execute("INSERT OR IGNORE INTO control (key, value) VALUES (?, ?)", (key, default))
    conn.commit()
    conn.close()


def insert_cases(db_path: str, cases: list[dict]):
    conn = _connect(db_path)
    conn.executemany(
        """INSERT OR IGNORE INTO cases
           (tribunal, year, month, case_id, title, url, pdf_url)
           VALUES (:tribunal, :year, :month, :case_id, :title, :url, :pdf_url)""",
        cases)
    conn.commit()
    conn.close()


def get_pending_cases(db_path: str, tribunal: str, limit: int = 100,
                      year_start: int | None = None, year_end: int | None = None,
                      direction: str = "asc") -> list[dict]:
    conn = _connect(db_path)
    where = "tribunal=? AND status IN ('pending', 'failed') AND retry_count < ?"
    params: list = [tribunal, MAX_RETRIES]
    if year_start is not None:
        where += " AND year >= ?"
        params.append(year_start)
    if year_end is not None:
        where += " AND year <= ?"
        params.append(year_end)
    sort = "ASC" if direction == "asc" else "DESC"
    order = f"ORDER BY (CASE WHEN status='pending' THEN 0 ELSE 1 END), year {sort}, month {sort}"
    rows = conn.execute(
        f"SELECT id, tribunal, year, month, case_id, title, url, pdf_url, status, retry_count "
        f"FROM cases WHERE {where} {order} LIMIT ?",
        params + [limit]).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_case_done(db_path: str, case_id: int, pdf_path: str, file_size: int):
    conn = _connect(db_path)
    conn.execute(
        "UPDATE cases SET status='done', pdf_path=?, file_size=?, downloaded_at=CURRENT_TIMESTAMP WHERE id=?",
        (pdf_path, file_size, case_id))
    conn.commit()
    conn.close()


def mark_case_failed(db_path: str, case_id: int, error: str):
    conn = _connect(db_path)
    conn.execute("UPDATE cases SET retry_count = retry_count + 1, last_error=? WHERE id=?", (error, case_id))
    row = conn.execute("SELECT retry_count FROM cases WHERE id=?", (case_id,)).fetchone()
    if row and row["retry_count"] >= MAX_RETRIES:
        conn.execute("UPDATE cases SET status='abandoned' WHERE id=?", (case_id,))
    else:
        conn.execute("UPDATE cases SET status='failed' WHERE id=?", (case_id,))
    conn.commit()
    conn.close()


def mark_case_no_pdf(db_path: str, case_id: int):
    conn = _connect(db_path)
    conn.execute("UPDATE cases SET status='no_pdf' WHERE id=?", (case_id,))
    conn.commit()
    conn.close()


def mark_cases_downloading(db_path: str, case_ids: list[int]):
    if not case_ids:
        return
    conn = _connect(db_path)
    placeholders = ",".join("?" for _ in case_ids)
    conn.execute(f"UPDATE cases SET status='downloading' WHERE id IN ({placeholders})", case_ids)
    conn.commit()
    conn.close()


def reset_downloading(db_path: str):
    conn = _connect(db_path)
    conn.execute("UPDATE cases SET status='pending' WHERE status='downloading'")
    conn.commit()
    conn.close()


def get_tribunal_stats(db_path: str, tribunal: str) -> dict:
    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT status, COUNT(*) as cnt FROM cases WHERE tribunal=? GROUP BY status",
        (tribunal,)).fetchall()
    conn.close()
    stats = {r["status"]: r["cnt"] for r in rows}
    for key in ("pending", "done", "failed", "no_pdf", "abandoned", "downloading"):
        stats.setdefault(key, 0)
    stats["total"] = sum(v for k, v in stats.items() if k != "total")
    return stats


def get_all_stats(db_path: str) -> dict[str, dict]:
    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT tribunal, status, COUNT(*) as cnt FROM cases GROUP BY tribunal, status").fetchall()
    conn.close()
    stats = {}
    for r in rows:
        t = r["tribunal"]
        if t not in stats:
            stats[t] = {"pending": 0, "done": 0, "failed": 0, "no_pdf": 0, "abandoned": 0, "downloading": 0}
        stats[t][r["status"]] = r["cnt"]
    for t in stats:
        stats[t]["total"] = sum(v for k, v in stats[t].items() if k != "total")
    return stats


def get_year_breakdown(db_path: str, tribunal: str) -> list[dict]:
    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT year, status, COUNT(*) as cnt FROM cases WHERE tribunal=? GROUP BY year, status ORDER BY year",
        (tribunal,)).fetchall()
    conn.close()
    breakdown = {}
    for r in rows:
        y = r["year"]
        if y not in breakdown:
            breakdown[y] = {"year": y, "done": 0, "pending": 0, "failed": 0, "total": 0}
        breakdown[y][r["status"]] = r["cnt"]
        breakdown[y]["total"] += r["cnt"]
    return sorted(breakdown.values(), key=lambda x: x["year"])


def get_month_breakdown(db_path: str, tribunal: str, year: int) -> list[dict]:
    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT month, status, COUNT(*) as cnt FROM cases WHERE tribunal=? AND year=? GROUP BY month, status ORDER BY month",
        (tribunal, year)).fetchall()
    conn.close()
    breakdown = {}
    for r in rows:
        m = r["month"]
        if m not in breakdown:
            breakdown[m] = {"month": m, "done": 0, "pending": 0, "failed": 0, "total": 0}
        breakdown[m][r["status"]] = r["cnt"]
        breakdown[m]["total"] += r["cnt"]
    return sorted(breakdown.values(), key=lambda x: x["month"])


def is_month_collected(db_path: str, tribunal: str, year: int, month: int) -> bool:
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT 1 FROM months_collected WHERE tribunal=? AND year=? AND month=?",
        (tribunal, year, month)).fetchone()
    conn.close()
    return row is not None


def mark_month_collected(db_path: str, tribunal: str, year: int, month: int, case_count: int):
    conn = _connect(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO months_collected (tribunal, year, month, case_count) VALUES (?, ?, ?, ?)",
        (tribunal, year, month, case_count))
    conn.commit()
    conn.close()


def log_event(db_path: str, event_type: str, message: str):
    conn = _connect(db_path)
    conn.execute("INSERT INTO events (event_type, message) VALUES (?, ?)", (event_type, message))
    conn.execute("DELETE FROM events WHERE id < (SELECT MAX(id) - 10000 FROM events)")
    conn.commit()
    conn.close()


def get_recent_events(db_path: str, limit: int = 50) -> list[dict]:
    conn = _connect(db_path)
    rows = conn.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_download_count(db_path: str, minutes: int = 5) -> int:
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM cases WHERE downloaded_at > datetime('now', ?)",
        (f"-{minutes} minutes",)).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def get_control(db_path: str, key: str) -> str:
    conn = _connect(db_path)
    row = conn.execute("SELECT value FROM control WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else None


def set_control(db_path: str, key: str, value: str):
    conn = _connect(db_path)
    conn.execute("INSERT OR REPLACE INTO control (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()


def sync_with_disk(db_path: str, pdf_base_dir: str, tribunal: str):
    """Sync DB status with what's actually on disk.

    - If a PDF exists on disk but DB says pending/failed -> mark done
    - If DB says done but PDF is missing from disk -> mark pending
    """
    conn = _connect(db_path)

    # Build set of case_ids that exist on disk
    disk_case_ids = {}  # case_id -> (path, size)
    tribunal_dir = os.path.join(pdf_base_dir, tribunal)
    if not os.path.isdir(tribunal_dir):
        conn.close()
        return

    for year_dir in os.listdir(tribunal_dir):
        year_path = os.path.join(tribunal_dir, year_dir)
        if not os.path.isdir(year_path):
            continue
        for month_dir in os.listdir(year_path):
            month_path = os.path.join(year_path, month_dir)
            if not os.path.isdir(month_path):
                continue
            for fname in os.listdir(month_path):
                if not fname.endswith(".pdf"):
                    continue
                fpath = os.path.join(month_path, fname)
                fsize = os.path.getsize(fpath)
                # Extract case_id from filename (e.g. "2025 QCTAQ 4689" or "2025 CanLII 80797")
                m = re.search(r'(\d{4})\s*(QCTAQ|CanLII|CANLII)\s*(\d+)', fname, re.IGNORECASE)
                if m:
                    cid = f"{m.group(1)}{m.group(2).lower()}{m.group(3)}"
                    disk_case_ids[cid] = (fpath, fsize)

    # Mark done cases whose PDF is missing -> pending
    rows = conn.execute(
        "SELECT id, case_id, pdf_path FROM cases WHERE tribunal=? AND status='done'",
        (tribunal,)).fetchall()
    reset_count = 0
    for r in rows:
        pdf_path = r["pdf_path"]
        if pdf_path and os.path.isfile(pdf_path):
            continue
        # Also check by case_id in disk set
        if r["case_id"] in disk_case_ids:
            path, size = disk_case_ids[r["case_id"]]
            conn.execute(
                "UPDATE cases SET pdf_path=?, file_size=? WHERE id=?",
                (path, size, r["id"]))
            continue
        conn.execute(
            "UPDATE cases SET status='pending', pdf_path=NULL, file_size=NULL, "
            "retry_count=0, downloaded_at=NULL WHERE id=?",
            (r["id"],))
        reset_count += 1

    # Mark pending/failed cases whose PDF exists on disk -> done
    rows = conn.execute(
        "SELECT id, case_id FROM cases WHERE tribunal=? AND status IN ('pending','failed','abandoned')",
        (tribunal,)).fetchall()
    found_count = 0
    for r in rows:
        if r["case_id"] in disk_case_ids:
            path, size = disk_case_ids[r["case_id"]]
            conn.execute(
                "UPDATE cases SET status='done', pdf_path=?, file_size=?, "
                "retry_count=0, downloaded_at=CURRENT_TIMESTAMP WHERE id=?",
                (path, size, r["id"]))
            found_count += 1

    conn.commit()
    conn.close()

    if reset_count or found_count:
        print(f"  Disk sync: {found_count} marked done (PDF on disk), {reset_count} reset to pending (PDF missing)")
    else:
        print("  Disk sync: DB matches disk")


def retry_failed(db_path: str, tribunal: str) -> int:
    conn = _connect(db_path)
    cursor = conn.execute(
        "UPDATE cases SET status='pending' WHERE tribunal=? AND status='failed' AND retry_count < ?",
        (tribunal, MAX_RETRIES))
    count = cursor.rowcount
    conn.commit()
    conn.close()
    return count
