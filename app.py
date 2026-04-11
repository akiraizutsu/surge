"""Flask application for Momentum Dashboard."""

import json
import os
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template, request, session, redirect, url_for

# Load .env for local development (no-op on Railway where vars are injected)
_dotenv = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_dotenv):
    with open(_dotenv) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ[_k.strip()] = _v.strip()

import requests
import yfinance as yf

from screener import run_screening
import database
from database import (
    init_db, save_session, save_results, save_value_gap_results,
    get_sessions, get_session_results,
    add_to_watchlist, remove_from_watchlist, get_watchlist,
    save_breadth, get_breadth,
    get_latest_sessions_by_index,
    save_cf_cache, get_cf_cache, clear_cf_cache,
    get_edinet_cached_companies, save_edinet_companies,
    get_edinet_cached_financials, save_edinet_financials,
    get_stock_explain,
    save_backtest_result, get_backtest_results,
    get_unread_events, get_all_events, mark_events_read, mark_all_events_read,
)
import backtest_service
from scoring_service import WEIGHT_PRESETS
import capital_allocation_service
import us_advanced_service
import data_quality_service
import auth_service
from auth_service import (
    current_user, current_user_id, is_owner,
    login_required, owner_required,
    verify_login, login_session, logout_session,
)
import notes_service
import rate_limit_service
import llm_service
import scheduler_service
import cf_analysis_service
from cf_analysis_service import build_cf_payload

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "surge-dev-fallback-key-change-in-prod")
init_db()


# ── Authentication ────────────────────────────────────────────────────────────

@app.before_request
def require_login():
    """Block unauthenticated access. Uses new per-user auth from auth_service.

    Exempt: /login, /logout, /static/*, and the Railway health check /api/status.
    """
    if request.endpoint in ("login", "logout", "static"):
        return
    # Allow Railway health check to pass without auth
    if request.path == "/api/status":
        return
    if "user_id" not in session:
        if request.path.startswith("/api/"):
            return jsonify({"error": "Unauthorized"}), 401
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        pw = request.form.get("password", "")
        user = verify_login(username, pw)
        if user:
            login_session(user)
            return redirect(url_for("japan"))
        error = "ユーザー名またはパスワードが違います"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    logout_session()
    return redirect(url_for("login"))


# ── Auth API ──────────────────────────────────────────────────────────────

@app.get("/api/auth/me")
def api_auth_me():
    user = current_user()
    if not user:
        return jsonify({"error": "not logged in"}), 401
    return jsonify(user)


@app.post("/api/auth/consent")
def api_auth_consent():
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "not logged in"}), 401
    auth_service.set_consent(uid)
    return jsonify({"status": "ok"})


@app.post("/api/auth/change_password")
def api_auth_change_password():
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "not logged in"}), 401
    data = request.get_json(silent=True) or {}
    current = data.get("current", "")
    new_pw = data.get("new", "")
    if len(new_pw) < 4:
        return jsonify({"error": "new password too short"}), 400
    ok = auth_service.change_password(uid, current, new_pw)
    if not ok:
        return jsonify({"error": "current password incorrect"}), 400
    return jsonify({"status": "ok"})


# ── Research Notes API ────────────────────────────────────────────────────

@app.get("/api/notes")
def api_list_notes():
    uid = current_user_id()
    ticker = request.args.get("ticker")
    pinned = request.args.get("pinned", "0") == "1"
    limit = request.args.get("limit", 50, type=int)
    notes = notes_service.list_user_notes(
        user_id=uid, ticker=ticker, pinned_only=pinned, limit=limit
    )
    return jsonify(notes)


@app.post("/api/notes")
def api_create_note():
    uid = current_user_id()
    data = request.get_json(silent=True) or {}
    title = (data.get("title") or "").strip()
    answer = (data.get("answer") or "").strip()
    if not title or not answer:
        return jsonify({"error": "title and answer are required"}), 400
    note_id = notes_service.create_note(
        user_id=uid,
        title=title,
        question=data.get("question", ""),
        answer=answer,
        tickers=data.get("tickers"),
        tags=data.get("tags"),
        index_name=data.get("index_name"),
        llm_model=data.get("llm_model"),
        tool_calls=data.get("tool_calls"),
    )
    if not note_id:
        return jsonify({"error": "failed to create note"}), 500
    return jsonify({"id": note_id, "status": "created"}), 201


@app.get("/api/notes/<int:note_id>")
def api_get_note(note_id):
    uid = current_user_id()
    note = notes_service.get_note(note_id, uid)
    if not note:
        return jsonify({"error": "not found"}), 404
    return jsonify(note)


@app.patch("/api/notes/<int:note_id>")
def api_update_note(note_id):
    uid = current_user_id()
    data = request.get_json(silent=True) or {}
    allowed_keys = {"title", "question", "answer", "tickers", "tags", "is_pinned"}
    updates = {k: v for k, v in data.items() if k in allowed_keys}
    if not updates:
        return jsonify({"error": "no valid fields"}), 400
    ok = notes_service.update_note(note_id, uid, **updates)
    if not ok:
        return jsonify({"error": "not found or not updated"}), 404
    return jsonify({"status": "ok"})


@app.delete("/api/notes/<int:note_id>")
def api_delete_note(note_id):
    uid = current_user_id()
    ok = notes_service.delete_note(note_id, uid)
    if not ok:
        return jsonify({"error": "not found"}), 404
    return jsonify({"status": "deleted"})


@app.post("/api/notes/<int:note_id>/pin")
def api_toggle_pin(note_id):
    uid = current_user_id()
    ok = notes_service.toggle_pin(note_id, uid)
    if not ok:
        return jsonify({"error": "not found"}), 404
    return jsonify({"status": "ok"})


# ── LLM Chat API ───────────────────────────────────────────────────────────

@app.get("/api/chat/usage")
def api_chat_usage():
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "not logged in"}), 401
    usage = rate_limit_service.get_usage_today(uid)
    return jsonify(usage)


@app.post("/api/chat")
def api_chat():
    """Streaming chat endpoint. Returns NDJSON (one JSON object per line)."""
    uid = current_user_id()
    if uid is None:
        return jsonify({"error": "not logged in"}), 401

    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    history = data.get("history") or []
    use_pro = bool(data.get("use_pro", False))

    if not message:
        return jsonify({"error": "message is required"}), 400

    def generate():
        try:
            ai = llm_service.AnalystAI(user_id=uid)
            # Block non-owner Pro mode attempts
            if use_pro and not ai.is_owner:
                yield json.dumps({"type": "error", "error": "Pro mode is owner only"}, ensure_ascii=False) + "\n"
                return
            for chunk in ai.chat_stream(message, history=history, use_pro=use_pro):
                yield json.dumps(chunk, ensure_ascii=False) + "\n"
        except Exception as e:
            yield json.dumps({"type": "error", "error": f"chat failed: {type(e).__name__}: {e}"}, ensure_ascii=False) + "\n"

    return app.response_class(
        generate(),
        mimetype="application/x-ndjson",
        headers={
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
        },
    )


# ── Admin API (owner only) ─────────────────────────────────────────────

@app.get("/api/admin/users")
def api_admin_users():
    if not is_owner():
        return jsonify({"error": "owner only"}), 403
    return jsonify(auth_service.list_all_users())


@app.get("/api/admin/usage")
def api_admin_usage():
    if not is_owner():
        return jsonify({"error": "owner only"}), 403
    from datetime import datetime, timezone, timedelta
    jst = timezone(timedelta(hours=9))
    today = datetime.now(jst).strftime("%Y-%m-%d")
    return jsonify({
        "date": today,
        "users": database.get_all_users_usage(today),
        "global": rate_limit_service.get_global_cost_summary(),
    })


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
_screening_thread = None  # Track the screening thread


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
        _state["last_index"] = index
        _state["last_top_n"] = top_n

    try:
        if index == "all":
            _run_all_indices(top_n)
        elif index == "us_all":
            _run_us_indices(top_n)
        elif index == "japan_all":
            _run_japan_indices(top_n)
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

    regime_json = json.dumps(result["regime"], ensure_ascii=False) if result.get("regime") else None
    session_id = save_session({
        "index_name": index,
        "top_n": top_n,
        "total_screened": result["total_screened"],
        "generated_at": result["generated_at"],
        "regime_json": regime_json,
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

        regime_json = json.dumps(result["regime"], ensure_ascii=False) if result.get("regime") else None
        session_id = save_session({
            "index_name": idx,
            "top_n": top_n,
            "total_screened": result["total_screened"],
            "generated_at": result["generated_at"],
            "regime_json": regime_json,
        })
        save_results(session_id, result["momentum_ranking"])
        if result.get("value_gap_ranking"):
            save_value_gap_results(session_id, result["value_gap_ranking"])

        # Save market breadth history
        if result.get("breadth"):
            save_breadth(idx, result["breadth"])


def _run_japan_indices(top_n):
    """Run screening for Nikkei 225 and Growth 250."""
    japan_indices = ["nikkei225", "growth250"]
    span_per = 100 // len(japan_indices)
    for i, idx in enumerate(japan_indices):
        offset = i * span_per
        label = {"nikkei225": "日経225", "growth250": "グロース250"}[idx]
        cb = _make_all_progress_cb(label, offset, span_per)
        result = run_screening(index=idx, top_n=top_n, progress_cb=cb)
        with _lock:
            _state["results"][idx] = result
            _state["result"] = result

        regime_json = json.dumps(result["regime"], ensure_ascii=False) if result.get("regime") else None
        session_id = save_session({
            "index_name": idx,
            "top_n": top_n,
            "total_screened": result["total_screened"],
            "generated_at": result["generated_at"],
            "regime_json": regime_json,
        })
        save_results(session_id, result["momentum_ranking"])
        if result.get("value_gap_ranking"):
            save_value_gap_results(session_id, result["value_gap_ranking"])
        if result.get("breadth"):
            save_breadth(idx, result["breadth"])


def _run_us_indices(top_n):
    """Run screening for S&P 500 and NASDAQ 100 only."""
    us_indices = ["sp500", "nasdaq100"]
    span_per = 100 // len(us_indices)
    for i, idx in enumerate(us_indices):
        offset = i * span_per
        label = {"sp500": "S&P 500", "nasdaq100": "NASDAQ 100"}[idx]
        cb = _make_all_progress_cb(label, offset, span_per)
        result = run_screening(index=idx, top_n=top_n, progress_cb=cb)
        with _lock:
            _state["results"][idx] = result
            _state["result"] = result

        regime_json = json.dumps(result["regime"], ensure_ascii=False) if result.get("regime") else None
        session_id = save_session({
            "index_name": idx,
            "top_n": top_n,
            "total_screened": result["total_screened"],
            "generated_at": result["generated_at"],
            "regime_json": regime_json,
        })
        save_results(session_id, result["momentum_ranking"])
        if result.get("value_gap_ranking"):
            save_value_gap_results(session_id, result["value_gap_ranking"])
        if result.get("breadth"):
            save_breadth(idx, result["breadth"])


@app.route("/")
def japan():
    return render_template("japan.html")


@app.route("/us")
def us():
    return render_template("us.html")


@app.route("/howto")
def howto():
    return render_template("howto.html")


@app.post("/api/screen")
def start_screening():
    global _screening_thread
    with _lock:
        # If state says running but thread is dead, reset the stale state
        if _state["running"] and (_screening_thread is None or not _screening_thread.is_alive()):
            _state["running"] = False
            _state["error"] = "前回のスクリーニングが異常終了しました。再実行してください。"
        if _state["running"]:
            return jsonify({"error": "Screening already in progress"}), 409

    data = request.get_json(silent=True) or {}
    index = data.get("index", "sp500")
    top_n = data.get("top_n", 20)

    if index not in ("sp500", "nasdaq100", "nikkei225", "growth250", "all", "us_all", "japan_all"):
        return jsonify({"error": "Invalid index"}), 400
    if not (1 <= top_n <= 100):
        return jsonify({"error": "top_n must be between 1 and 100"}), 400

    _screening_thread = threading.Thread(target=_run_screening_job, args=(index, top_n), daemon=True)
    _screening_thread.start()

    return jsonify({"status": "started", "index": index, "top_n": top_n})


def _trigger_scheduled_screening(index_key, label):
    """Called by scheduler_service at each slot fire time.

    Skips quietly if a screening is already running so we don't stomp a
    manual run or an overlapping scheduled run.
    """
    global _screening_thread
    with _lock:
        if _state["running"] and _screening_thread is not None and _screening_thread.is_alive():
            print(f"[scheduler] {label}: skipped — screening already running")
            return
        # Reset any stale state
        if _state["running"]:
            _state["running"] = False
            _state["error"] = None
    top_n = _state.get("last_top_n") or 20
    print(f"[scheduler] {label}: starting {index_key} (top_n={top_n})")
    _screening_thread = threading.Thread(
        target=_run_screening_job, args=(index_key, top_n), daemon=True
    )
    _screening_thread.start()


# Start the scheduler once at import time. Safe to call multiple times
# (scheduler_service guards against double-start).
scheduler_service.start_scheduler(_trigger_scheduled_screening)


@app.get("/api/status")
def get_status():
    with _lock:
        has_result = _state["result"] is not None
    # Check DB if memory is empty and not currently running
    if not has_result and not _state["running"]:
        db_results = get_latest_sessions_by_index()
        has_result = bool(db_results)
    with _lock:
        resp = {
            "running": _state["running"],
            "progress_message": _state["progress_message"],
            "progress_pct": _state["progress_pct"],
            "error": _state["error"],
            "has_result": has_result,
        }
        if _state["error"]:
            resp["last_index"] = _state.get("last_index")
            resp["last_top_n"] = _state.get("last_top_n")
        return jsonify(resp)


@app.post("/api/clear_error")
def api_clear_error():
    with _lock:
        _state["running"] = False
        _state["error"] = None
        _state["progress_message"] = ""
        _state["progress_pct"] = 0
    return jsonify({"status": "cleared"})


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


@app.get("/api/stock/<ticker>/explain")
def stock_explain(ticker):
    """Return score breakdown, selection reason tags, and confirmation questions.

    First checks in-memory results (fast, fresh screening), falls back to DB
    (persists across restarts). Returns 404 if ticker has never been screened.
    """
    ticker = ticker.upper()

    # ── Try in-memory first ──────────────────────────────────────────────────
    with _lock:
        all_results = dict(_state["results"])

    for idx_data in all_results.values():
        ranking = idx_data.get("momentum_ranking", [])
        for stock in ranking:
            if stock.get("ticker") == ticker:
                return jsonify({
                    "ticker": ticker,
                    "score_components": stock.get("score_components", []),
                    "tags": stock.get("tags", []),
                    "questions": stock.get("questions", []),
                    "source": "memory",
                })

    # ── Fall back to DB ──────────────────────────────────────────────────────
    explain = get_stock_explain(ticker)
    if explain:
        explain["ticker"] = ticker
        explain["source"] = "db"
        return jsonify(explain)

    return jsonify({"error": f"{ticker} not found in recent screening results"}), 404


# ── Watchlist (per-user) ──

@app.get("/api/watchlist")
def api_get_watchlist():
    return jsonify(get_watchlist(user_id=current_user_id()))


@app.post("/api/watchlist")
def api_add_watchlist():
    data = request.get_json(silent=True) or {}
    ticker = data.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker is required"}), 400
    add_to_watchlist(ticker, user_id=current_user_id())
    return jsonify({"status": "added", "ticker": ticker}), 201


@app.delete("/api/watchlist/<ticker>")
def api_remove_watchlist(ticker):
    remove_from_watchlist(ticker.upper(), user_id=current_user_id())
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


# ── Sprint 4: Backtest ────────────────────────────────────────────────────────

@app.post("/api/backtest/run")
def api_backtest_run():
    """Run a backtest for a past screening session.

    Body JSON: { session_id, horizon_days, top_n }
    """
    body = request.get_json(force=True, silent=True) or {}
    session_id   = int(body.get("session_id", 0))
    horizon_days = int(body.get("horizon_days", 20))
    top_n        = int(body.get("top_n", 20))

    # Validate
    if session_id <= 0:
        return jsonify({"error": "session_id required"}), 400
    if horizon_days not in backtest_service.VALID_HORIZONS:
        return jsonify({"error": f"horizon_days must be one of {backtest_service.VALID_HORIZONS}"}), 400

    # Load session metadata + results from DB
    sessions = get_sessions(limit=200)
    sess = next((s for s in sessions if s["id"] == session_id), None)
    if not sess:
        return jsonify({"error": "Session not found"}), 404

    results = get_session_results(session_id)
    if not results:
        return jsonify({"error": "No results for this session"}), 404

    # Run in foreground (blocking) — typical yfinance call takes 1-5s
    bt = backtest_service.run_backtest(
        session_id=session_id,
        horizon_days=horizon_days,
        top_n=top_n,
        index_name=sess["index_name"],
        session_date_str=sess["generated_at"],
        results=results,
    )

    # Persist if successful
    if bt["stats"] is not None:
        save_backtest_result(bt)

    return jsonify(bt)


@app.get("/api/backtest/results")
def api_backtest_results():
    """List past backtest results, optionally filtered by session_id."""
    session_id = request.args.get("session_id", type=int)
    rows = get_backtest_results(session_id=session_id, limit=50)
    return jsonify(rows)


# ── Sprint 4: Weight Presets ──────────────────────────────────────────────────

@app.get("/api/weight_presets")
def api_weight_presets():
    """Return available weight presets for client-side re-scoring."""
    return jsonify(WEIGHT_PRESETS)


# ── Sprint 5: Seed Score + Capital Allocation ─────────────────────────────────

@app.get("/api/stock/<ticker>/capital_allocation")
def api_capital_allocation(ticker):
    """Return capital allocation score for a ticker (live yfinance fetch)."""
    try:
        info = yf.Ticker(ticker).info
        result = capital_allocation_service.compute_capital_allocation(info)
        result["ticker"] = ticker
        result["component_labels"] = capital_allocation_service.COMPONENT_LABELS
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/api/stock/<ticker>/seed_score")
def api_seed_score(ticker):
    """Return seed score for a ticker (live yfinance fetch). Japan stocks only."""
    import seed_score_service
    try:
        info = yf.Ticker(ticker).info
        # Build minimal technicals from info
        current_price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
        high_52w = info.get("fiftyTwoWeekHigh") or current_price
        dist_from_high = round((current_price / high_52w - 1) * 100, 2) if high_52w > 0 else 0
        technicals = {
            "dist_from_high": dist_from_high,
            "ret_1m": None,
        }
        result = seed_score_service.compute_seed_score(info, technicals)
        result["ticker"] = ticker
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Sprint 7: US Advanced Signals ────────────────────────────────────────────

@app.get("/api/stock/<ticker>/us_advanced")
def api_us_advanced(ticker):
    """Return US advanced signals for a ticker (EPS revision, institutional flow, earnings drift, options)."""
    ticker = ticker.upper()
    try:
        info = yf.Ticker(ticker).info
        # Build merged dict with all needed fields
        merged = dict(info)
        merged["ret_1m"]             = None  # not available from live info alone
        merged["ret_1w"]             = None
        merged["days_to_earnings"]   = None
        merged["short_pct_of_float"] = info.get("shortPercentOfFloat")
        merged["short_change_pct"]   = None
        result = us_advanced_service.compute_us_advanced(merged)
        result["ticker"] = ticker
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Data Quality ─────────────────────────────────────────────────────────────

@app.get("/api/data_quality/status")
def api_data_quality_status():
    """Return data source health summary."""
    return jsonify(data_quality_service.get_status_summary())


# ── Sprint 6: Watchlist Events / Change Detection ────────────────────────────

@app.get("/api/watchlist/events")
def api_watchlist_events():
    """Return recent watchlist events (change alerts). ?unread_only=1 filters unread."""
    index_name = request.args.get("index")
    unread_only = request.args.get("unread_only", "0") == "1"
    limit = request.args.get("limit", 100, type=int)
    if unread_only:
        events = get_unread_events(index_name=index_name, limit=limit)
    else:
        events = get_all_events(index_name=index_name, limit=limit)
    return jsonify(events)


@app.post("/api/watchlist/events/read")
def api_mark_events_read():
    """Mark events as read. Body: { ids: [1,2,3] } or { all: true, index: 'nikkei225' }"""
    body = request.get_json(silent=True) or {}
    if body.get("all"):
        mark_all_events_read(index_name=body.get("index"))
    else:
        ids = [int(i) for i in body.get("ids", []) if str(i).isdigit()]
        mark_events_read(ids)
    return jsonify({"status": "ok"})


@app.get("/api/watchlist/events/unread_count")
def api_unread_event_count():
    """Return count of unread events per index."""
    from database import _connect
    conn = _connect()
    rows = conn.execute(
        "SELECT index_name, COUNT(*) as cnt FROM watchlist_events WHERE is_read = 0 GROUP BY index_name"
    ).fetchall()
    conn.close()
    return jsonify({r["index_name"]: r["cnt"] for r in rows})


# ── CF Analysis (EDINET DB) ──
# Heavy lifting (EDINET / J-Quants fetch, timeline & summary calculations)
# now lives in cf_analysis_service.build_cf_payload. This route only
# handles the cache policy.

_CF_CACHE_DAYS = 7


@app.get("/api/cf_analysis/<ticker>")
def api_cf_analysis(ticker):
    """Return CF analysis for a Nikkei225 ticker. Cached for 7 days."""
    ticker = ticker.upper()
    if not ticker.endswith(".T"):
        ticker = ticker + ".T"

    cached = get_cf_cache(ticker)
    if cached:
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                cached["fetched_at"].replace("Z", "+00:00").replace(" ", "T") + "+00:00"
                if "+" not in cached["fetched_at"] else cached["fetched_at"]
            )).days
        except Exception:
            age = 99
        if age < _CF_CACHE_DAYS:
            return jsonify(json.loads(cached["data"]))

    data = build_cf_payload(ticker)
    if not data or not data.get("timeline"):
        return jsonify({"error": "CFデータを取得できませんでした"}), 404

    save_cf_cache(ticker, json.dumps(data, ensure_ascii=False))
    return jsonify(data)


@app.delete("/api/cf_cache/clear/<ticker>")
def api_clear_cf_cache(ticker):
    """Clear cached CF data for a ticker."""
    ticker = ticker.upper()
    if not ticker.endswith(".T"):
        ticker = ticker + ".T"
    clear_cf_cache(ticker)
    return jsonify({"status": "cleared", "ticker": ticker})


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", debug=False, port=port)
