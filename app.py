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
from database import (
    init_db, save_session, save_results, save_value_gap_results,
    get_sessions, get_session_results,
    add_to_watchlist, remove_from_watchlist, get_watchlist,
    save_breadth, get_breadth,
    get_latest_sessions_by_index,
    save_cf_cache, get_cf_cache, clear_cf_cache,
    get_edinet_cached_companies, save_edinet_companies,
    get_edinet_cached_financials, save_edinet_financials,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "surge-dev-fallback-key-change-in-prod")
init_db()


# ── Authentication ────────────────────────────────────────────────────────────

@app.before_request
def require_login():
    """Block unauthenticated access to all routes except /login and /static."""
    if request.endpoint in ("login", "logout", "static"):
        return
    if not session.get("authenticated"):
        if request.path.startswith("/api/"):
            return jsonify({"error": "Unauthorized"}), 401
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        pw = request.form.get("password", "")
        correct = os.environ.get("SURGE_PASSWORD", "")
        if correct and pw == correct:
            session["authenticated"] = True
            session.permanent = True
            return redirect(url_for("index"))
        error = "パスワードが違います"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

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


# ── CF Analysis (EDINET DB) ──

_EDINETDB_BASE = "https://edinetdb.jp/v1"
_UNIT_DIV = 100_000_000   # 億円
_edinet_code_cache = {}   # {sec_code: edinet_code}  in-process cache


def _edinet_headers():
    return {"X-API-Key": os.environ.get("EDINETDB_API_KEY", "")}


def _edinet_get(path, params=None):
    try:
        r = requests.get(
            f"{_EDINETDB_BASE}{path}",
            params=params,
            headers=_edinet_headers(),
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def sec_code_to_edinet(sec_code):
    """Find EDINET company code from 4-digit security code.
    EDINET DB stores sec_code as 5-digit with trailing 0 (e.g. '67580' for '6758').
    Also caches company name as side-effect.
    """
    if sec_code in _edinet_code_cache:
        return _edinet_code_cache[sec_code]

    data = _edinet_get("/search", {"q": sec_code})
    if not data:
        return None

    results = data.get("data") or data.get("results") or (data if isinstance(data, list) else [])
    for company in results:
        sc = str(company.get("sec_code") or company.get("security_code") or "")
        # sec_code in DB is 5-digit (e.g. "67580"); drop trailing 0 to match "6758"
        if sc == sec_code or sc[:-1] == sec_code or sc == sec_code + "0":
            code = company.get("edinet_code")
            if code:
                _edinet_code_cache[sec_code] = code
                # cache company name too
                name = company.get("name_ja") or company.get("name") or ""
                _edinet_code_cache[sec_code + "_name"] = name
                return code
    return None


def fetch_financials(edinet_code):
    """Fetch annual CF data from EDINET DB (up to 6 fiscal years)."""
    return _edinet_get(f"/companies/{edinet_code}/financials")


def fetch_quarterly(edinet_code):
    """Fetch quarterly CF data from EDINET DB."""
    return _edinet_get(f"/companies/{edinet_code}/financials", {"period": "quarterly"})


def _sc(v):
    """Scale raw JPY → 億円."""
    return round(v / _UNIT_DIV, 1) if v is not None else None


def _extract_rows(raw_data):
    """Pull the list of period records — edinetdb uses root key 'data'."""
    if raw_data is None:
        return []
    if isinstance(raw_data, list):
        return raw_data
    return raw_data.get("data") or raw_data.get("results") or []


def build_timeline(annual_rows):
    """Build annual CF timeline from EDINET DB records.
    Rows have: fiscal_year, cf_operating, cf_investing, cf_financing, cash
    No capex field → FCF = operating CF (most conservative, universally comparable).
    """
    timeline = []
    for item in sorted(annual_rows, key=lambda x: x.get("fiscal_year") or 0):
        op  = item.get("cf_operating")
        inv = item.get("cf_investing")
        fin = item.get("cf_financing")
        # FCF = operating CF (EDINET DB has no separate capex field)
        fcf = op

        timeline.append({
            "period":       str(item.get("fiscal_year", "")),
            "operating_cf": _sc(op),
            "investing_cf": _sc(inv),
            "financing_cf": _sc(fin),
            "capex":        None,  # not provided by edinetdb
            "fcf":          _sc(fcf),
        })
    return timeline


def _calc_fcf_trend(values):
    if len(values) < 2:
        return "データ不足"
    delta = values[-1] - values[-2]
    ratio = abs(delta) / (abs(values[-2]) + 1)
    if ratio < 0.05:
        return "横ばい"
    return "増加" if delta > 0 else "減少"


def calc_summary(timeline):
    recent3 = timeline[-3:] if len(timeline) >= 3 else timeline
    fcf_vals = [e["fcf"] for e in recent3 if e.get("fcf") is not None]
    op_vals  = [e["operating_cf"] for e in recent3 if e.get("operating_cf") is not None]
    return {
        "avg_fcf_3y":          round(sum(fcf_vals) / len(fcf_vals), 1) if fcf_vals else None,
        "avg_operating_cf_3y": round(sum(op_vals)  / len(op_vals),  1) if op_vals  else None,
        "latest_fcf":          timeline[-1]["fcf"]          if timeline else None,
        "latest_operating_cf": timeline[-1]["operating_cf"] if timeline else None,
        "fcf_trend":           _calc_fcf_trend(fcf_vals)   if len(fcf_vals) >= 2 else "データ不足",
    }


def calc_ma_capacity(timeline, annual_rows):
    """M&A capacity using edinetdb 'cash' field from latest annual row."""
    fcf_vals = [e["fcf"] for e in timeline[-3:] if e.get("fcf") is not None]
    avg_fcf = (sum(fcf_vals) / len(fcf_vals)) if fcf_vals else None

    # edinetdb provides 'cash' (現金及び現金同等物) in the annual financials
    latest = sorted(annual_rows, key=lambda x: x.get("fiscal_year") or 0)[-1] if annual_rows else {}
    cash_raw = latest.get("cash")
    net_cash = _sc(cash_raw) if cash_raw is not None else None

    return {
        "net_cash":    net_cash,
        "annual_fcf":  round(avg_fcf, 1) if avg_fcf is not None else None,
        "capacity_3y": round(net_cash + avg_fcf * 3, 1) if (net_cash is not None and avg_fcf is not None) else None,
        "capacity_5y": round(net_cash + avg_fcf * 5, 1) if (net_cash is not None and avg_fcf is not None) else None,
    }


def _build_cf_payload(ticker):
    """Fetch CF data via EDINET DB API and build the response dict."""
    if not os.environ.get("EDINETDB_API_KEY"):
        return {"error": "EDINETDB_API_KEY が設定されていません"}

    sec_code = ticker.replace(".T", "").replace(".t", "")

    # 1. EDINET code lookup
    edinet_code = sec_code_to_edinet(sec_code)
    if not edinet_code:
        return None

    # 2. Annual financials
    annual_raw = fetch_financials(edinet_code)
    if not annual_raw:
        return None
    annual_rows = _extract_rows(annual_raw)
    # Company name: prefer cached name from search, else fallback
    company_name = _edinet_code_cache.get(sec_code + "_name") or ticker

    # 3. Quarterly financials
    quarterly_raw = fetch_quarterly(edinet_code)
    quarterly_rows = _extract_rows(quarterly_raw)

    # 4. Build timeline
    timeline = build_timeline(annual_rows)
    if not timeline:
        return None

    # 5. Quarterly list (last 8 quarters)
    # EDINET quarterly CF is cumulative from fiscal year start → convert to incremental
    quarterly = []
    prev_fy, prev_op_cum = None, 0
    for item in sorted(quarterly_rows, key=lambda x: (x.get("fiscal_year") or 0, x.get("quarter") or 0)):
        fy = item.get("fiscal_year")
        q  = item.get("quarter", "")
        op_cum = item.get("cf_operating")

        if fy != prev_fy:          # new fiscal year → reset cumulative base
            prev_fy, prev_op_cum = fy, 0

        op_incr = (op_cum - prev_op_cum) if op_cum is not None else None
        prev_op_cum = op_cum or prev_op_cum

        quarterly.append({
            "period":       f"{fy}Q{q}",
            "operating_cf": _sc(op_incr),
            "fcf":          _sc(op_incr),
        })
    quarterly = quarterly[-8:]

    # 6. Summary & M&A capacity
    summary     = calc_summary(timeline)
    ma_capacity = calc_ma_capacity(timeline, annual_rows)

    return {
        "ticker":       ticker,
        "company_name": company_name,
        "edinet_code":  edinet_code,
        "currency":     "JPY",
        "unit":         "億円",
        "timeline":     timeline,
        "quarterly":    quarterly,
        "summary":      summary,
        "ma_capacity":  ma_capacity,
    }


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

    data = _build_cf_payload(ticker)
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
