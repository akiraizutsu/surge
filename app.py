"""Flask application for Momentum Dashboard."""

import threading
from flask import Flask, jsonify, render_template, request

from screener import run_screening
from database import (
    init_db, save_session, save_results, save_value_gap_results,
    get_sessions, get_session_results,
    add_to_watchlist, remove_from_watchlist, get_watchlist,
    save_breadth, get_breadth,
    get_latest_sessions_by_index,
)

app = Flask(__name__)
init_db()

ALL_INDICES = ["sp500", "nasdaq100", "nikkei225"]

# In-memory state for screening jobs
_state = {
    "running": False,
    "progress_message": "",
    "progress_pct": 0,
    "result": None,       # single-index result (legacy)
    "results": {},        # all-index results: {index_name: result}
    "error": None,
}
_lock = threading.Lock()


def _progress_callback(message, pct):
    with _lock:
        _state["progress_message"] = message
        _state["progress_pct"] = pct


def _make_all_progress_cb(index_label, offset, span):
    """Create a progress callback scoped to one index within an all-index run."""
    def cb(message, pct):
        adjusted = offset + int(pct * span / 100)
        with _lock:
            _state["progress_message"] = f"[{index_label}] {message}"
            _state["progress_pct"] = adjusted
    return cb


def _run_screening_job(index, top_n):
    with _lock:
        _state["running"] = True
        _state["error"] = None
        _state["progress_pct"] = 0
        _state["progress_message"] = "Starting..."

    try:
        if index == "all":
            _run_all_indices(top_n)
        else:
            _run_single_index(index, top_n)

        with _lock:
            _state["running"] = False
            _state["progress_pct"] = 100
            _state["progress_message"] = "Complete!"
    except Exception as e:
        with _lock:
            _state["running"] = False
            _state["error"] = str(e)
            _state["progress_message"] = f"Error: {e}"


def _run_single_index(index, top_n):
    result = run_screening(index=index, top_n=top_n, progress_cb=_progress_callback)
    with _lock:
        _state["result"] = result
        _state["results"][index] = result

    session_id = save_session({
        "index_name": index,
        "top_n": top_n,
        "total_screened": result["total_screened"],
        "generated_at": result["generated_at"],
    })
    save_results(session_id, result["momentum_ranking"])
    if result.get("value_gap_ranking"):
        save_value_gap_results(session_id, result["value_gap_ranking"])

    # Save market breadth history
    if result.get("breadth"):
        save_breadth(index, result["breadth"])


def _run_all_indices(top_n):
    span_per = 100 // len(ALL_INDICES)
    for i, idx in enumerate(ALL_INDICES):
        offset = i * span_per
        label = {"sp500": "S&P 500", "nasdaq100": "NASDAQ 100", "nikkei225": "日経225"}[idx]
        cb = _make_all_progress_cb(label, offset, span_per)
        result = run_screening(index=idx, top_n=top_n, progress_cb=cb)
        with _lock:
            _state["results"][idx] = result
            # Keep the latest as the single result for backward compat
            _state["result"] = result

        session_id = save_session({
            "index_name": idx,
            "top_n": top_n,
            "total_screened": result["total_screened"],
            "generated_at": result["generated_at"],
        })
        save_results(session_id, result["momentum_ranking"])
        if result.get("value_gap_ranking"):
            save_value_gap_results(session_id, result["value_gap_ranking"])

        # Save market breadth history
        if result.get("breadth"):
            save_breadth(idx, result["breadth"])


@app.route("/")
def index():
    return render_template("index.html")


@app.post("/api/screen")
def start_screening():
    with _lock:
        if _state["running"]:
            return jsonify({"error": "Screening already in progress"}), 409

    data = request.get_json(silent=True) or {}
    index = data.get("index", "sp500")
    top_n = data.get("top_n", 20)

    if index not in ("sp500", "nasdaq100", "nikkei225", "all"):
        return jsonify({"error": "Invalid index. Use 'sp500', 'nasdaq100', 'nikkei225', or 'all'"}), 400
    if not (1 <= top_n <= 100):
        return jsonify({"error": "top_n must be between 1 and 100"}), 400

    thread = threading.Thread(target=_run_screening_job, args=(index, top_n), daemon=True)
    thread.start()

    return jsonify({"status": "started", "index": index, "top_n": top_n})


@app.get("/api/status")
def get_status():
    with _lock:
        has_result = _state["result"] is not None
    # Check DB if memory is empty and not currently running
    if not has_result and not _state["running"]:
        db_results = get_latest_sessions_by_index()
        has_result = bool(db_results)
    with _lock:
        return jsonify({
            "running": _state["running"],
            "progress_message": _state["progress_message"],
            "progress_pct": _state["progress_pct"],
            "error": _state["error"],
            "has_result": has_result,
        })


@app.get("/api/result")
def get_result():
    with _lock:
        if _state["result"] is None:
            return jsonify({"error": "No results available"}), 404
        return jsonify(_state["result"])


@app.get("/api/results")
def get_all_results():
    """Get results for all screened indices. Falls back to DB if memory is empty."""
    with _lock:
        if _state["results"]:
            return jsonify(_state["results"])

    # Restore from DB after restart/deploy
    db_results = get_latest_sessions_by_index()
    if db_results:
        with _lock:
            _state["results"] = db_results
            # Set latest as single result for backward compat
            first_key = next(iter(db_results))
            _state["result"] = db_results[first_key]
        return jsonify(db_results)

    return jsonify({"error": "No results available"}), 404


# ── Watchlist ──

@app.get("/api/watchlist")
def api_get_watchlist():
    return jsonify(get_watchlist())


@app.post("/api/watchlist")
def api_add_watchlist():
    data = request.get_json(silent=True) or {}
    ticker = data.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker is required"}), 400
    add_to_watchlist(ticker)
    return jsonify({"status": "added", "ticker": ticker}), 201


@app.delete("/api/watchlist/<ticker>")
def api_remove_watchlist(ticker):
    remove_from_watchlist(ticker.upper())
    return jsonify({"status": "removed", "ticker": ticker.upper()})


# ── Breadth ──

@app.get("/api/breadth/<index_name>")
def api_get_breadth(index_name):
    if index_name not in ("sp500", "nasdaq100", "nikkei225"):
        return jsonify({"error": "Invalid index"}), 400
    data = get_breadth(index_name, days=90)
    return jsonify({"index_name": index_name, "data": data})


# ── History ──

@app.get("/api/history")
def api_get_history():
    return jsonify(get_sessions(limit=20))


@app.get("/api/history/<int:session_id>")
def api_get_history_detail(session_id):
    results = get_session_results(session_id)
    if not results:
        return jsonify({"error": "Session not found"}), 404
    return jsonify(results)


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", debug=False, port=port)
