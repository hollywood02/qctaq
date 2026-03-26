"""Web dashboard for QCTAQ Case Law Scraper."""
import json
import os
import signal
import subprocess
import time
from flask import Flask, render_template, jsonify, Response, request

import db
from config import DB_PATH, TRIBUNALS, BASE_DIR, DASHBOARD_PORT

app = Flask(__name__)
db.init_db(DB_PATH)

# Track subprocess PIDs
_processes = {"collector": None, "downloader": None}


@app.route("/")
def index():
    return render_template("index.html", tribunals=TRIBUNALS)


@app.route("/api/stats")
def api_stats():
    all_stats = db.get_all_stats(DB_PATH)
    return jsonify(all_stats)


@app.route("/api/stats/<tribunal>")
def api_tribunal_stats(tribunal):
    stats = db.get_tribunal_stats(DB_PATH, tribunal)
    breakdown = db.get_year_breakdown(DB_PATH, tribunal)
    return jsonify({"stats": stats, "years": breakdown})


@app.route("/api/stats/<tribunal>/<int:year>")
def api_year_months(tribunal, year):
    months = db.get_month_breakdown(DB_PATH, tribunal, year)
    return jsonify({"year": year, "months": months})


@app.route("/api/events")
def api_events():
    events = db.get_recent_events(DB_PATH, limit=50)
    return jsonify(events)


@app.route("/api/speed")
def api_speed():
    count_5m = db.get_recent_download_count(DB_PATH, 5)
    rate_per_hour = count_5m * 12
    return jsonify({"downloads_5m": count_5m, "rate_per_hour": rate_per_hour})


@app.route("/api/control", methods=["GET"])
def api_get_control():
    state = db.get_control(DB_PATH, "state")
    workers = db.get_control(DB_PATH, "workers")
    tribunal = db.get_control(DB_PATH, "tribunal")
    year_start = db.get_control(DB_PATH, "year_start") or ""
    year_end = db.get_control(DB_PATH, "year_end") or ""
    direction = db.get_control(DB_PATH, "direction") or "asc"
    return jsonify({
        "state": state, "workers": workers, "tribunal": tribunal,
        "year_start": year_start, "year_end": year_end, "direction": direction,
    })


@app.route("/api/control", methods=["POST"])
def api_set_control():
    data = request.json
    if "state" in data:
        db.set_control(DB_PATH, "state", data["state"])
    if "workers" in data:
        db.set_control(DB_PATH, "workers", str(data["workers"]))
    if "tribunal" in data:
        db.set_control(DB_PATH, "tribunal", data["tribunal"])
    if "year_start" in data:
        db.set_control(DB_PATH, "year_start", str(data["year_start"]) if data["year_start"] else "")
    if "year_end" in data:
        db.set_control(DB_PATH, "year_end", str(data["year_end"]) if data["year_end"] else "")
    if "direction" in data:
        db.set_control(DB_PATH, "direction", data["direction"])
    return jsonify({"ok": True})


@app.route("/api/retry/<tribunal>", methods=["POST"])
def api_retry(tribunal):
    count = db.retry_failed(DB_PATH, tribunal)
    return jsonify({"reset": count})


@app.route("/api/start/<process_type>", methods=["POST"])
def api_start_process(process_type):
    """Start collector or downloader as subprocess."""
    if process_type not in ("collector", "downloader"):
        return jsonify({"error": "Invalid process type"}), 400

    # Kill existing if running
    if _processes[process_type] and _processes[process_type].poll() is None:
        os.killpg(os.getpgid(_processes[process_type].pid), signal.SIGTERM)
        _processes[process_type] = None

    data = request.json or {}
    tribunal = data.get("tribunal", "taq")
    python = os.path.join(BASE_DIR, ".venv_qctaq", "bin", "python")
    scraper = os.path.join(BASE_DIR, "scraper.py")

    if process_type == "collector":
        start_year = data.get("start_year", "1998")
        end_year = data.get("end_year", "2026")
        cmd = [python, scraper, "collect", "--tribunal", tribunal,
               "--start-year", str(start_year), "--end-year", str(end_year)]
    else:
        workers = data.get("workers", "5")
        delay = data.get("delay", "2")
        cmd = [python, scraper, "download", "--tribunal", tribunal,
               "--workers", str(workers), "--delay", str(delay)]

    proc = subprocess.Popen(cmd, cwd=BASE_DIR, preexec_fn=os.setsid,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    _processes[process_type] = proc
    return jsonify({"ok": True, "pid": proc.pid})


@app.route("/api/stop/<process_type>", methods=["POST"])
def api_stop_process(process_type):
    """Stop collector or downloader."""
    if process_type not in ("collector", "downloader"):
        return jsonify({"error": "Invalid process type"}), 400
    proc = _processes.get(process_type)
    if proc and proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        _processes[process_type] = None
        return jsonify({"ok": True, "stopped": True})
    return jsonify({"ok": True, "stopped": False})


@app.route("/api/processes")
def api_processes():
    """Check which processes are running."""
    result = {}
    for name, proc in _processes.items():
        if proc and proc.poll() is None:
            result[name] = {"running": True, "pid": proc.pid}
        else:
            result[name] = {"running": False}
    return jsonify(result)


@app.route("/api/stream")
def api_stream():
    def generate():
        while True:
            try:
                all_stats = db.get_all_stats(DB_PATH)
                speed = db.get_recent_download_count(DB_PATH, 5)
                control_state = db.get_control(DB_PATH, "state")
                events = db.get_recent_events(DB_PATH, limit=10)
                data = {
                    "stats": all_stats,
                    "speed_5m": speed,
                    "rate_per_hour": speed * 12,
                    "state": control_state,
                    "events": events,
                }
                yield f"data: {json.dumps(data)}\n\n"
            except Exception:
                pass
            time.sleep(2)
    return Response(generate(), mimetype="text/event-stream")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=False, threaded=True)
