"""Surge - SQLite Database Layer"""

import os
import sqlite3

# Railway Volume: mount at /data, fallback to local for dev
_data_dir = os.environ.get("DATA_DIR", os.path.dirname(__file__))
DB_PATH = os.path.join(_data_dir, "surge.db")


def _connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = _connect()
    with conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS screening_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_name TEXT NOT NULL,
                top_n INTEGER NOT NULL,
                total_screened INTEGER NOT NULL,
                generated_at TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS screening_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES screening_sessions(id),
                rank INTEGER,
                ticker TEXT,
                name TEXT,
                sector TEXT,
                price REAL,
                momentum_score REAL,
                ret_1d REAL,
                ret_1w REAL,
                ret_1m REAL,
                ret_3m REAL,
                vol_ratio REAL,
                ma50_dev REAL,
                ma200_dev REAL,
                macd_hist_pct REAL,
                rsi REAL,
                golden_cross INTEGER,
                overheat INTEGER,
                market_cap_b REAL,
                pe_trailing REAL,
                pe_forward REAL,
                pb REAL,
                dividend_yield REAL,
                revenue_growth REAL,
                earnings_growth REAL,
                eps REAL,
                target_price REAL,
                recommendation TEXT,
                sector_etf TEXT,
                rs_1m REAL,
                rs_3m REAL,
                short_pct_of_float REAL,
                short_ratio REAL,
                shares_short INTEGER,
                shares_short_prior_month INTEGER,
                float_shares INTEGER,
                short_change_pct REAL,
                squeeze_score REAL,
                high_52w REAL,
                low_52w REAL,
                dist_from_high REAL,
                bb_width REAL,
                earnings_date TEXT,
                days_to_earnings INTEGER
            );

            CREATE TABLE IF NOT EXISTS value_gap_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES screening_sessions(id),
                rank INTEGER,
                ticker TEXT,
                name TEXT,
                sector TEXT,
                price REAL,
                target_price REAL,
                target_gap_pct REAL,
                value_gap_score REAL,
                ret_1m REAL,
                ret_3m REAL,
                rsi REAL,
                pe_forward REAL,
                pe_trailing REAL,
                pb REAL,
                eps_growth REAL,
                revenue_growth REAL,
                recommendation TEXT,
                market_cap_b REAL,
                dividend_yield REAL,
                eps REAL,
                ma50_dev REAL,
                ma200_dev REAL
            );

            CREATE TABLE IF NOT EXISTS market_breadth (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_name TEXT NOT NULL,
                date TEXT NOT NULL,
                advances INTEGER,
                declines INTEGER,
                unchanged INTEGER,
                ad_diff INTEGER,
                adl REAL,
                breadth_pct REAL,
                UNIQUE(index_name, date)
            );

            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL UNIQUE,
                added_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS cf_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL UNIQUE,
                data TEXT NOT NULL,
                fetched_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS edinet_company_cache (
                sec_code TEXT PRIMARY KEY,
                edinet_code TEXT,
                industry TEXT,
                fetched_at TEXT DEFAULT (datetime('now')),
                latest_financials_json TEXT,
                fin_fetched_at TEXT
            );

            CREATE TABLE IF NOT EXISTS score_components (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_id INTEGER NOT NULL REFERENCES screening_results(id) ON DELETE CASCADE,
                component_name TEXT NOT NULL,
                label TEXT,
                raw_value REAL,
                percentile_value REAL,
                weighted_score REAL
            );

            CREATE TABLE IF NOT EXISTS stock_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_id INTEGER NOT NULL REFERENCES screening_results(id) ON DELETE CASCADE,
                ticker TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                confidence REAL,
                reason_text TEXT
            );

            CREATE TABLE IF NOT EXISTS stock_questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_id INTEGER NOT NULL REFERENCES screening_results(id) ON DELETE CASCADE,
                ticker TEXT NOT NULL,
                question_text TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS backtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES screening_sessions(id),
                horizon_days INTEGER NOT NULL,
                top_n INTEGER NOT NULL,
                benchmark_ticker TEXT,
                avg_return REAL,
                median_return REAL,
                win_rate REAL,
                benchmark_return REAL,
                excess_return REAL,
                sample_size INTEGER,
                detail_json TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS data_source_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL UNIQUE,
                last_success_at TEXT,
                last_failure_at TEXT,
                last_error TEXT,
                health_status TEXT DEFAULT 'unknown',
                metadata_json TEXT
            );

            CREATE TABLE IF NOT EXISTS watchlist_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                index_name TEXT,
                event_type TEXT NOT NULL,
                payload_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                is_read INTEGER DEFAULT 0
            );

            -- LLM Phase 1: multi-user auth, research notes, usage tracking
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                avatar_emoji TEXT DEFAULT '👤',
                role TEXT NOT NULL DEFAULT 'user',
                created_at TEXT DEFAULT (datetime('now')),
                last_login_at TEXT,
                consent_given_at TEXT
            );

            CREATE TABLE IF NOT EXISTS research_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                question TEXT,
                answer TEXT NOT NULL,
                tickers_json TEXT,
                tags_json TEXT,
                index_name TEXT,
                llm_model TEXT,
                is_pinned INTEGER DEFAULT 0,
                tool_calls_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS user_usage (
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                date TEXT NOT NULL,
                request_count INTEGER DEFAULT 0,
                tokens_in INTEGER DEFAULT 0,
                tokens_out INTEGER DEFAULT 0,
                cost_usd REAL DEFAULT 0,
                PRIMARY KEY (user_id, date)
            );

            CREATE INDEX IF NOT EXISTS idx_notes_user_created
                ON research_notes(user_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_notes_pinned
                ON research_notes(user_id, is_pinned DESC, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_screening_results_ticker
                ON screening_results(ticker, session_id DESC);
        """)

        # ── Schema migrations: add columns to existing tables ──────────────
        _add_column_if_missing(conn, "screening_results", "high_52w", "REAL")
        _add_column_if_missing(conn, "screening_results", "low_52w", "REAL")
        _add_column_if_missing(conn, "screening_results", "dist_from_high", "REAL")
        _add_column_if_missing(conn, "screening_results", "bb_width", "REAL")
        _add_column_if_missing(conn, "screening_results", "earnings_date", "TEXT")
        _add_column_if_missing(conn, "screening_results", "days_to_earnings", "INTEGER")
        _add_column_if_missing(conn, "edinet_company_cache", "latest_financials_json", "TEXT")
        _add_column_if_missing(conn, "edinet_company_cache", "fin_fetched_at", "TEXT")
        # Sprint 2: market regime stored per session
        _add_column_if_missing(conn, "screening_sessions", "regime_json", "TEXT")
        # Sprint 3: quality score and entry difficulty
        _add_column_if_missing(conn, "screening_results", "quality_score", "REAL")
        _add_column_if_missing(conn, "screening_results", "entry_difficulty", "TEXT")
        # Sprint 5: seed score and capital allocation
        _add_column_if_missing(conn, "screening_results", "seed_score", "REAL")
        _add_column_if_missing(conn, "screening_results", "capital_score", "REAL")
        _add_column_if_missing(conn, "screening_results", "capital_grade", "TEXT")
        # OBV & Drawdown analysis
        _add_column_if_missing(conn, "screening_results", "obv_slope", "REAL")
        _add_column_if_missing(conn, "screening_results", "obv_divergence", "TEXT")
        _add_column_if_missing(conn, "screening_results", "max_drawdown_3m", "REAL")
        _add_column_if_missing(conn, "screening_results", "current_drawdown", "REAL")
        # ADX (trend strength)
        _add_column_if_missing(conn, "screening_results", "adx", "REAL")

        # Smart Watchlist: custom alert rules per watchlist entry
        _add_column_if_missing(conn, "watchlist", "alert_rules_json", "TEXT")

        # LLM Phase 1: user_id columns for per-user data isolation
        # DEFAULT 1 means existing data auto-links to AKIRA (seeded first, id=1)
        _add_column_if_missing(conn, "watchlist", "user_id", "INTEGER DEFAULT 1")
        _add_column_if_missing(conn, "watchlist_events", "user_id", "INTEGER DEFAULT 1")
        _add_column_if_missing(conn, "backtest_results", "user_id", "INTEGER DEFAULT 1")

        # Seed initial users from SURGE_USERS env var (no-op if already exist)
        _seed_initial_users(conn)

    conn.close()


def _add_column_if_missing(conn, table, column, col_type):
    """Add a column to a table only if it doesn't already exist."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def _seed_initial_users(conn):
    """Seed initial users from SURGE_USERS env var on every init.

    Additive and idempotent:
    - Existing users' password/display_name/role are NEVER overwritten (INSERT OR IGNORE).
    - Missing users (added to SURGE_USERS after initial deploy) are inserted on next init.
    - If SURGE_USERS is unset AND the table is empty, creates a fallback akira/surge owner
      so the app is not locked out on very first run.
    """
    import os
    import json as _json
    from werkzeug.security import generate_password_hash

    raw = os.environ.get("SURGE_USERS", "").strip()

    # No SURGE_USERS: only create fallback if the table is completely empty
    if not raw:
        existing = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()
        if existing["c"] > 0:
            return
        conn.execute(
            "INSERT INTO users (username, password_hash, display_name, avatar_emoji, role) VALUES (?, ?, ?, ?, ?)",
            ("akira", generate_password_hash("surge"), "AKIRA", "🧑‍💻", "owner"),
        )
        return

    try:
        users = _json.loads(raw)
    except Exception as e:
        print(f"[seed] Failed to parse SURGE_USERS JSON: {e}")
        return

    # Sort so owner comes first (so a fresh DB gets id=1 → owner)
    users_sorted = sorted(users, key=lambda u: 0 if u.get("role") == "owner" else 1)

    for u in users_sorted:
        username = (u.get("username") or "").strip()
        password = u.get("password") or ""
        if not username or not password:
            continue
        display_name = u.get("display_name") or username
        role = u.get("role") or "user"
        avatar_emoji = u.get("avatar_emoji") or "👤"
        try:
            conn.execute(
                "INSERT OR IGNORE INTO users (username, password_hash, display_name, avatar_emoji, role) VALUES (?, ?, ?, ?, ?)",
                (username, generate_password_hash(password), display_name, avatar_emoji, role),
            )
        except Exception as e:
            print(f"[seed] Failed to insert user {username}: {e}")


# ── Sessions ──

def save_session(meta):
    conn = _connect()
    with conn:
        cur = conn.execute(
            """INSERT INTO screening_sessions
               (index_name, top_n, total_screened, generated_at, regime_json)
               VALUES (?, ?, ?, ?, ?)""",
            (meta["index_name"], meta["top_n"], meta["total_screened"],
             meta["generated_at"], meta.get("regime_json")),
        )
        session_id = cur.lastrowid
    conn.close()
    return session_id


def save_results(session_id, ranking):
    conn = _connect()
    with conn:
        for r in ranking:
            t = r.get("technicals", {})
            f = r.get("fundamentals", {})
            si = r.get("short_interest", {})
            cur = conn.execute(
                """INSERT INTO screening_results (
                    session_id, rank, ticker, name, sector, price, momentum_score,
                    ret_1d, ret_1w, ret_1m, ret_3m, vol_ratio, ma50_dev, ma200_dev,
                    macd_hist_pct, rsi, golden_cross, overheat,
                    market_cap_b, pe_trailing, pe_forward, pb, dividend_yield,
                    revenue_growth, earnings_growth, eps, target_price, recommendation,
                    sector_etf, rs_1m, rs_3m,
                    short_pct_of_float, short_ratio, shares_short, shares_short_prior_month,
                    float_shares, short_change_pct, squeeze_score,
                    high_52w, low_52w, dist_from_high, bb_width,
                    earnings_date, days_to_earnings,
                    quality_score, entry_difficulty,
                    seed_score, capital_score, capital_grade,
                    obv_slope, obv_divergence, max_drawdown_3m, current_drawdown,
                    adx
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id, r.get("rank"), r.get("ticker"), r.get("name"),
                    r.get("sector"), r.get("price"), r.get("momentum_score"),
                    t.get("ret_1d"), t.get("ret_1w"), t.get("ret_1m"), t.get("ret_3m"),
                    t.get("vol_ratio"), t.get("ma50_dev"), t.get("ma200_dev"),
                    t.get("macd_hist_pct"), t.get("rsi"),
                    1 if t.get("golden_cross") else 0,
                    1 if t.get("overheat") else 0,
                    f.get("market_cap_b"), f.get("pe_trailing"), f.get("pe_forward"),
                    f.get("pb"), f.get("dividend_yield"), f.get("revenue_growth"),
                    f.get("earnings_growth"), f.get("eps"), f.get("target_price"),
                    f.get("recommendation"),
                    t.get("sector_etf"), t.get("rs_1m"), t.get("rs_3m"),
                    si.get("short_pct_of_float"), si.get("short_ratio"),
                    si.get("shares_short"), si.get("shares_short_prior_month"),
                    si.get("float_shares"), si.get("short_change_pct"),
                    r.get("squeeze_score"),
                    t.get("high_52w"), t.get("low_52w"), t.get("dist_from_high"), t.get("bb_width"),
                    f.get("earnings_date"), f.get("days_to_earnings"),
                    r.get("quality_score"), r.get("entry_difficulty"),
                    r.get("seed_score"), r.get("capital_score"), r.get("capital_grade"),
                    t.get("obv_slope"), t.get("obv_divergence"),
                    t.get("max_drawdown_3m"), t.get("current_drawdown"),
                    t.get("adx"),
                ),
            )
            result_id = cur.lastrowid
            ticker = r.get("ticker", "")

            # ── score_components ──────────────────────────────────────────────
            for comp in r.get("score_components", []):
                conn.execute(
                    """INSERT INTO score_components
                       (result_id, component_name, label, raw_value, percentile_value, weighted_score)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (result_id, comp.get("component_name"), comp.get("label"),
                     comp.get("raw_value"), comp.get("percentile_value"), comp.get("weighted_score")),
                )

            # ── stock_tags ────────────────────────────────────────────────────
            for tag in r.get("tags", []):
                conn.execute(
                    """INSERT INTO stock_tags
                       (result_id, ticker, tag_name, confidence, reason_text)
                       VALUES (?, ?, ?, ?, ?)""",
                    (result_id, ticker, tag.get("tag_name"),
                     tag.get("confidence"), tag.get("reason_text")),
                )

            # ── stock_questions ───────────────────────────────────────────────
            for i, q in enumerate(r.get("questions", [])):
                conn.execute(
                    """INSERT INTO stock_questions
                       (result_id, ticker, question_text, sort_order)
                       VALUES (?, ?, ?, ?)""",
                    (result_id, ticker, q, i),
                )
    conn.close()


def get_stock_explain(ticker):
    """Return score_components, tags, questions for the latest DB result of a ticker."""
    conn = _connect()
    row = conn.execute(
        "SELECT id FROM screening_results WHERE ticker = ? ORDER BY id DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    if not row:
        conn.close()
        return None
    result_id = row["id"]

    components = conn.execute(
        "SELECT component_name, label, raw_value, percentile_value, weighted_score "
        "FROM score_components WHERE result_id = ? ORDER BY id",
        (result_id,),
    ).fetchall()

    tags = conn.execute(
        "SELECT tag_name, confidence, reason_text "
        "FROM stock_tags WHERE result_id = ? AND ticker = ?",
        (result_id, ticker),
    ).fetchall()

    questions = conn.execute(
        "SELECT question_text FROM stock_questions "
        "WHERE result_id = ? AND ticker = ? ORDER BY sort_order",
        (result_id, ticker),
    ).fetchall()

    conn.close()
    return {
        "score_components": [dict(c) for c in components],
        "tags": [dict(t) for t in tags],
        "questions": [q["question_text"] for q in questions],
    }


def get_ticker_timeline(ticker, limit=30):
    """Return historical momentum_score, rsi, ret_1m, rank for a ticker across sessions."""
    conn = _connect()
    rows = conn.execute(
        """SELECT r.momentum_score, r.rsi, r.ret_1m, r.rank,
                  s.generated_at, s.index_name
           FROM screening_results r
           JOIN screening_sessions s ON r.session_id = s.id
           WHERE r.ticker = ?
           ORDER BY s.id DESC
           LIMIT ?""",
        (ticker.upper(), limit),
    ).fetchall()
    conn.close()
    # Reverse to chronological order (oldest first)
    return [dict(r) for r in reversed(rows)]


def get_sessions(limit=20):
    """Return recent sessions with summary: top 3 tickers, avg score, regime label."""
    import json
    conn = _connect()
    sessions = conn.execute(
        "SELECT * FROM screening_sessions ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    result = []
    for s in sessions:
        row = dict(s)
        # Top 3 tickers by rank
        top = conn.execute(
            "SELECT ticker, momentum_score FROM screening_results "
            "WHERE session_id = ? ORDER BY rank LIMIT 3", (s["id"],)
        ).fetchall()
        row["top_tickers"] = [r["ticker"] for r in top]
        row["avg_score"]   = round(sum(r["momentum_score"] for r in top if r["momentum_score"]) / max(len(top), 1), 1) if top else None
        # Parse regime label from JSON
        try:
            regime = json.loads(s["regime_json"]) if s.get("regime_json") else None
            row["regime_label"] = regime.get("label") if regime else None
        except Exception:
            row["regime_label"] = None
        result.append(row)
    conn.close()
    return result


def save_backtest_result(bt: dict):
    """Persist a backtest run result to DB. Returns inserted id."""
    import json
    conn = _connect()
    with conn:
        stats = bt.get("stats") or {}
        cur = conn.execute(
            """INSERT INTO backtest_results
               (session_id, horizon_days, top_n, benchmark_ticker,
                avg_return, median_return, win_rate, benchmark_return,
                excess_return, sample_size, detail_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                bt["session_id"], bt["horizon_days"], bt["top_n"],
                bt.get("benchmark_ticker"),
                stats.get("avg_return"), stats.get("median_return"),
                stats.get("win_rate"), stats.get("benchmark_return"),
                stats.get("excess_return"), stats.get("sample_size"),
                json.dumps(bt.get("detail", []), ensure_ascii=False),
            ),
        )
        bt_id = cur.lastrowid
    conn.close()
    return bt_id


def get_backtest_results(session_id=None, limit=50):
    """Return backtest results, optionally filtered by session_id."""
    import json
    conn = _connect()
    if session_id is not None:
        rows = conn.execute(
            "SELECT * FROM backtest_results WHERE session_id = ? ORDER BY id DESC LIMIT ?",
            (session_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM backtest_results ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    result = []
    for r in rows:
        row = dict(r)
        try:
            row["detail"] = json.loads(r["detail_json"]) if r["detail_json"] else []
        except Exception:
            row["detail"] = []
        result.append(row)
    conn.close()
    return result


def get_session_results(session_id):
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM screening_results WHERE session_id = ? ORDER BY rank", (session_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_value_gap_results(session_id, ranking):
    conn = _connect()
    with conn:
        for r in ranking:
            conn.execute(
                """INSERT INTO value_gap_results (
                    session_id, rank, ticker, name, sector, price, target_price,
                    target_gap_pct, value_gap_score, ret_1m, ret_3m, rsi,
                    pe_forward, pe_trailing, pb, eps_growth, revenue_growth,
                    recommendation, market_cap_b, dividend_yield, eps, ma50_dev, ma200_dev
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (session_id, r.get("rank"), r.get("ticker"), r.get("name"),
                 r.get("sector"), r.get("price"), r.get("target_price"),
                 r.get("target_gap_pct"), r.get("value_gap_score"),
                 r.get("ret_1m"), r.get("ret_3m"), r.get("rsi"),
                 r.get("pe_forward"), r.get("pe_trailing"), r.get("pb"),
                 r.get("eps_growth"), r.get("revenue_growth"),
                 r.get("recommendation"), r.get("market_cap_b"),
                 r.get("dividend_yield"), r.get("eps"),
                 r.get("ma50_dev"), r.get("ma200_dev")),
            )
    conn.close()


def get_latest_sessions_by_index():
    """Get the most recent session for each index, with full results."""
    conn = _connect()
    results = {}
    for idx in ("sp500", "nasdaq100", "nikkei225", "growth250"):
      try:
        session = conn.execute(
            "SELECT * FROM screening_sessions WHERE index_name = ? ORDER BY id DESC LIMIT 1",
            (idx,),
        ).fetchone()
        if not session:
            continue
        rows = conn.execute(
            "SELECT * FROM screening_results WHERE session_id = ? ORDER BY rank",
            (session["id"],),
        ).fetchall()
        if not rows:
            continue

        ranking = []
        for r in rows:
            ranking.append({
                "rank": r["rank"],
                "ticker": r["ticker"],
                "name": r["name"],
                "sector": r["sector"],
                "price": r["price"],
                "momentum_score": r["momentum_score"],
                "squeeze_score": r["squeeze_score"],
                "technicals": {
                    "ret_1d": r["ret_1d"], "ret_1w": r["ret_1w"],
                    "ret_1m": r["ret_1m"], "ret_3m": r["ret_3m"],
                    "vol_ratio": r["vol_ratio"], "ma50_dev": r["ma50_dev"],
                    "ma200_dev": r["ma200_dev"], "macd_hist_pct": r["macd_hist_pct"],
                    "rsi": r["rsi"], "golden_cross": bool(r["golden_cross"]),
                    "overheat": bool(r["overheat"]),
                    "sector_etf": r["sector_etf"],
                    "rs_1m": r["rs_1m"], "rs_3m": r["rs_3m"],
                    "rs_label": _compute_rs_label(r),
                    "high_52w": r["high_52w"] if "high_52w" in r.keys() else None,
                    "low_52w": r["low_52w"] if "low_52w" in r.keys() else None,
                    "dist_from_high": r["dist_from_high"] if "dist_from_high" in r.keys() else None,
                    "dist_from_low": None,
                    "is_breakout": (r["dist_from_high"] or -999) >= -1 if "dist_from_high" in r.keys() else False,
                    "bb_width": r["bb_width"] if "bb_width" in r.keys() else None,
                    "bb_squeeze": (r["bb_width"] or 999) < 6 if "bb_width" in r.keys() else False,
                    "obv_slope": r["obv_slope"] if "obv_slope" in r.keys() else 0,
                    "obv_divergence": r["obv_divergence"] if "obv_divergence" in r.keys() else "none",
                    "max_drawdown_3m": r["max_drawdown_3m"] if "max_drawdown_3m" in r.keys() else None,
                    "current_drawdown": r["current_drawdown"] if "current_drawdown" in r.keys() else None,
                    "adx": r["adx"] if "adx" in r.keys() else 0,
                    "support_levels": [],
                    "resistance_levels": [],
                },
                "fundamentals": {
                    "market_cap_b": r["market_cap_b"], "pe_trailing": r["pe_trailing"],
                    "pe_forward": r["pe_forward"], "pb": r["pb"],
                    "dividend_yield": r["dividend_yield"],
                    "revenue_growth": r["revenue_growth"],
                    "earnings_growth": r["earnings_growth"],
                    "eps": r["eps"], "target_price": r["target_price"],
                    "recommendation": r["recommendation"],
                    "earnings_date": r["earnings_date"] if "earnings_date" in r.keys() else None,
                    "days_to_earnings": r["days_to_earnings"] if "days_to_earnings" in r.keys() else None,
                },
                "short_interest": {
                    "short_pct_of_float": r["short_pct_of_float"],
                    "short_ratio": r["short_ratio"],
                    "shares_short": r["shares_short"],
                    "shares_short_prior_month": r["shares_short_prior_month"],
                    "float_shares": r["float_shares"],
                    "short_change_pct": r["short_change_pct"],
                },
                # Sprint 3
                "quality_score": r["quality_score"] if "quality_score" in r.keys() else None,
                "entry_difficulty": r["entry_difficulty"] if "entry_difficulty" in r.keys() else None,
                # Sprint 5
                "seed_score": r["seed_score"] if "seed_score" in r.keys() else None,
                "capital_score": r["capital_score"] if "capital_score" in r.keys() else None,
                "capital_grade": r["capital_grade"] if "capital_grade" in r.keys() else None,
            })

        # Build sector distribution
        sector_dist = {}
        for row in ranking:
            s = row["sector"] or "Unknown"
            sector_dist[s] = sector_dist.get(s, 0) + 1

        # Summary
        scores = [row["momentum_score"] for row in ranking if row["momentum_score"]]
        avg_score = round(sum(scores) / len(scores), 1) if scores else 0
        overheat_count = sum(1 for row in ranking if row["technicals"]["overheat"])
        golden_count = sum(1 for row in ranking if row["technicals"]["golden_cross"])

        # Latest breadth
        breadth_rows = conn.execute(
            """SELECT advances, declines, breadth_pct FROM market_breadth
               WHERE index_name = ? ORDER BY date DESC LIMIT 1""",
            (idx,),
        ).fetchone()
        latest_breadth = {
            "advances": breadth_rows["advances"],
            "declines": breadth_rows["declines"],
            "breadth_pct": breadth_rows["breadth_pct"],
        } if breadth_rows else {"advances": 0, "declines": 0, "breadth_pct": 0}

        # Value gap results
        vg_rows = conn.execute(
            "SELECT * FROM value_gap_results WHERE session_id = ? ORDER BY rank",
            (session["id"],),
        ).fetchall()
        vg_ranking = [dict(r) for r in vg_rows] if vg_rows else []

        # Sprint 2: load market regime from session
        regime = None
        try:
            raw_regime = session["regime_json"] if "regime_json" in session.keys() else None
            if raw_regime:
                import json as _json
                regime = _json.loads(raw_regime)
        except Exception:
            pass

        # Rebuild derived rankings from momentum_ranking data (defensive)
        is_japan = idx in ("nikkei225", "growth250")
        breakout_ranking = []
        sector_rotation = []
        seed_ranking = []
        smallcap_ranking = []
        try:
            breakout_ranking = [rk for rk in ranking
                if rk.get("technicals", {}).get("is_breakout") or rk.get("technicals", {}).get("bb_squeeze")]

            # Rebuild sector rotation from ranking
            _sect_data = {}
            _sect_etf = {}
            for rk in ranking:
                s = rk.get("sector") or "Unknown"
                tech = rk.get("technicals", {}) or {}
                _sect_data.setdefault(s, {"ret_1m": [], "ret_3m": [], "rs_1m": [], "count": 0})
                _sect_etf.setdefault(s, tech.get("sector_etf") or ("^N225" if is_japan else "SPY"))
                _sect_data[s]["count"] += 1
                if tech.get("ret_1m") is not None:
                    _sect_data[s]["ret_1m"].append(tech["ret_1m"])
                if tech.get("ret_3m") is not None:
                    _sect_data[s]["ret_3m"].append(tech["ret_3m"])
                if tech.get("rs_1m") is not None:
                    _sect_data[s]["rs_1m"].append(tech["rs_1m"])
            for s, v in _sect_data.items():
                if not v["ret_1m"]:
                    continue
                r1m = round(sum(v["ret_1m"]) / len(v["ret_1m"]), 2)
                r3m = round(sum(v["ret_3m"]) / len(v["ret_3m"]), 2) if v["ret_3m"] else 0
                rs1m = round(sum(v["rs_1m"]) / len(v["rs_1m"]), 2) if v["rs_1m"] else 0
                if r1m > 0 and r3m > 0:
                    trend = "加速" if r1m > r3m / 3 else "安定"
                elif r1m > 0:
                    trend = "回復"
                elif r3m > 0:
                    trend = "減速"
                else:
                    trend = "衰退"
                sector_rotation.append({
                    "sector": s, "etf": _sect_etf.get(s, "SPY"),
                    "ret_1m_avg": r1m, "ret_3m_avg": r3m, "etf_1m": 0, "etf_3m": 0,
                    "rs_1m_avg": rs1m, "stock_count": v["count"], "trend": trend,
                })
            sector_rotation.sort(key=lambda x: x["ret_1m_avg"], reverse=True)

            if is_japan:
                seed_ranking = sorted(
                    [rk for rk in ranking if rk.get("seed_score") and rk["seed_score"] >= 20],
                    key=lambda x: x.get("seed_score", 0), reverse=True
                )[:20]
                smallcap_ranking = [rk for rk in ranking
                    if (rk.get("fundamentals") or {}).get("market_cap_b") is not None
                    and 1 <= (rk["fundamentals"]["market_cap_b"] or 0) <= 30]
        except Exception:
            # Defensive: if rebuild fails, return empty arrays rather than failing the whole restore
            pass

        index_label = {"sp500": "S&P 500", "nasdaq100": "NASDAQ 100", "nikkei225": "日経225", "growth250": "グロース250"}.get(idx, idx)
        results[idx] = {
            "index": index_label,
            "total_screened": session["total_screened"],
            "generated_at": session["generated_at"],
            "momentum_ranking": ranking,
            "value_gap_ranking": vg_ranking,
            "sector_rotation": sector_rotation,
            "breakout_ranking": breakout_ranking,
            "time_arb_ranking": [],
            "smallcap_ranking": smallcap_ranking,
            "seed_ranking": seed_ranking,
            "sector_distribution": sector_dist,
            "summary": {
                "avg_score": avg_score,
                "overheat_count": overheat_count,
                "golden_cross_count": golden_count,
            },
            "latest_breadth": latest_breadth,
            "regime": regime,
        }
      except Exception as e:
        # Defensive: skip this index if restore fails, continue with others
        import traceback
        print(f"[DB restore] Failed for {idx}: {e}")
        traceback.print_exc()
        continue
    conn.close()
    return results


def _compute_rs_label(r):
    """Compute RS label from DB row."""
    THEME_TICKERS = {"MSTR", "COIN", "MARA", "RIOT", "CLSK", "HUT", "BITF", "CIFR"}
    if r["ticker"] in THEME_TICKERS:
        return "theme"
    rs1 = r["rs_1m"]
    rs3 = r["rs_3m"]
    if rs1 is None:
        return None
    if rs1 > 2:
        return "prime" if (rs3 is not None and rs3 > 0) else "short_term"
    return "sector_driven"


# ── Market Breadth ──

def save_breadth(index_name, records):
    """Save daily breadth data. Uses INSERT OR REPLACE to update existing dates."""
    conn = _connect()
    with conn:
        for r in records:
            conn.execute(
                """INSERT OR REPLACE INTO market_breadth
                   (index_name, date, advances, declines, unchanged, ad_diff, adl, breadth_pct)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (index_name, r["date"], r["advances"], r["declines"],
                 r["unchanged"], r["ad_diff"], r["adl"], r["breadth_pct"]),
            )
    conn.close()


def get_breadth(index_name, days=60):
    """Get recent breadth history for an index."""
    conn = _connect()
    rows = conn.execute(
        """SELECT date, advances, declines, unchanged, ad_diff, adl, breadth_pct
           FROM market_breadth
           WHERE index_name = ?
           ORDER BY date DESC LIMIT ?""",
        (index_name, days),
    ).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


# ── Watchlist ──

def add_to_watchlist(ticker, user_id=1):
    conn = _connect()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (ticker, user_id) VALUES (?, ?)",
            (ticker.upper(), user_id),
        )
    conn.close()


def remove_from_watchlist(ticker, user_id=1):
    conn = _connect()
    with conn:
        conn.execute(
            "DELETE FROM watchlist WHERE ticker = ? AND user_id = ?",
            (ticker.upper(), user_id),
        )
    conn.close()


def get_watchlist(user_id=None):
    """Return a user's watchlist tickers.

    If user_id is None, returns the UNION of all users' tickers. Used by the
    shared screening job to detect changes across any user's watchlist.
    """
    conn = _connect()
    if user_id is None:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM watchlist ORDER BY ticker"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT ticker FROM watchlist WHERE user_id = ? ORDER BY added_at DESC",
            (user_id,),
        ).fetchall()
    conn.close()
    return [r["ticker"] for r in rows]


# ── Smart Watchlist: Custom Alert Rules ──

def get_alert_rules(ticker, user_id):
    """Return alert rules for a specific watchlist entry."""
    import json as _json
    conn = _connect()
    row = conn.execute(
        "SELECT alert_rules_json FROM watchlist WHERE ticker = ? AND user_id = ?",
        (ticker.upper(), user_id),
    ).fetchone()
    conn.close()
    if not row or not row["alert_rules_json"]:
        return []
    try:
        return _json.loads(row["alert_rules_json"])
    except Exception:
        return []


def update_alert_rules(ticker, user_id, rules):
    """Update alert rules for a watchlist entry. Returns True if updated."""
    import json as _json
    conn = _connect()
    with conn:
        cur = conn.execute(
            "UPDATE watchlist SET alert_rules_json = ? WHERE ticker = ? AND user_id = ?",
            (_json.dumps(rules, ensure_ascii=False), ticker.upper(), user_id),
        )
    conn.close()
    return cur.rowcount > 0


def get_all_alert_rules():
    """Return all watchlist entries that have alert rules set (for screening check)."""
    import json as _json
    conn = _connect()
    rows = conn.execute(
        "SELECT ticker, user_id, alert_rules_json FROM watchlist WHERE alert_rules_json IS NOT NULL AND alert_rules_json != '[]' AND alert_rules_json != ''"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        try:
            rules = _json.loads(r["alert_rules_json"])
            if rules:
                result.append({"ticker": r["ticker"], "user_id": r["user_id"], "rules": rules})
        except Exception:
            continue
    return result


# ── Watchlist Events (Sprint 6) ──

def save_watchlist_events(events: list[dict]):
    """Persist change-detection events. events = [{ticker, index_name, event_type, payload_json}]"""
    if not events:
        return
    conn = _connect()
    with conn:
        conn.executemany(
            """INSERT INTO watchlist_events (ticker, index_name, event_type, payload_json)
               VALUES (:ticker, :index_name, :event_type, :payload_json)""",
            events,
        )
    conn.close()


def get_unread_events(index_name=None, limit=50):
    """Return unread events, optionally filtered by index."""
    import json as _json
    conn = _connect()
    if index_name:
        rows = conn.execute(
            """SELECT * FROM watchlist_events WHERE is_read = 0 AND index_name = ?
               ORDER BY id DESC LIMIT ?""",
            (index_name, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM watchlist_events WHERE is_read = 0 ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    result = []
    for r in rows:
        row = dict(r)
        try:
            row["payload"] = _json.loads(r["payload_json"]) if r["payload_json"] else {}
        except Exception:
            row["payload"] = {}
        result.append(row)
    conn.close()
    return result


def get_all_events(index_name=None, limit=100):
    """Return all recent events (read + unread)."""
    import json as _json
    conn = _connect()
    if index_name:
        rows = conn.execute(
            """SELECT * FROM watchlist_events WHERE index_name = ?
               ORDER BY id DESC LIMIT ?""",
            (index_name, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM watchlist_events ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    result = []
    for r in rows:
        row = dict(r)
        try:
            row["payload"] = _json.loads(r["payload_json"]) if r["payload_json"] else {}
        except Exception:
            row["payload"] = {}
        result.append(row)
    conn.close()
    return result


def mark_events_read(event_ids: list[int]):
    """Mark specific events as read."""
    if not event_ids:
        return
    conn = _connect()
    with conn:
        conn.execute(
            f"UPDATE watchlist_events SET is_read = 1 WHERE id IN ({','.join('?' * len(event_ids))})",
            event_ids,
        )
    conn.close()


def mark_all_events_read(index_name=None):
    """Mark all events as read, optionally for a specific index."""
    conn = _connect()
    with conn:
        if index_name:
            conn.execute(
                "UPDATE watchlist_events SET is_read = 1 WHERE index_name = ?", (index_name,)
            )
        else:
            conn.execute("UPDATE watchlist_events SET is_read = 1")
    conn.close()


def get_prev_ranking(index_name: str, limit=50) -> list[dict]:
    """Return the second-to-latest session's ranking for diff comparison."""
    conn = _connect()
    sessions = conn.execute(
        "SELECT id FROM screening_sessions WHERE index_name = ? ORDER BY id DESC LIMIT 2",
        (index_name,),
    ).fetchall()
    if len(sessions) < 2:
        conn.close()
        return []
    prev_session_id = sessions[1]["id"]
    rows = conn.execute(
        """SELECT ticker, rank, momentum_score, quality_score, entry_difficulty,
                  rs_1m, rs_3m, dist_from_high, days_to_earnings
           FROM screening_results WHERE session_id = ? ORDER BY rank LIMIT ?""",
        (prev_session_id, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── CF Cache ──

def save_cf_cache(ticker, data_json):
    conn = _connect()
    with conn:
        conn.execute(
            """INSERT OR REPLACE INTO cf_cache (ticker, data, fetched_at)
               VALUES (?, ?, datetime('now'))""",
            (ticker.upper(), data_json),
        )
    conn.close()


def get_cf_cache(ticker):
    conn = _connect()
    row = conn.execute(
        "SELECT data, fetched_at FROM cf_cache WHERE ticker = ?",
        (ticker.upper(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def clear_cf_cache(ticker):
    conn = _connect()
    with conn:
        conn.execute("DELETE FROM cf_cache WHERE ticker = ?", (ticker.upper(),))
    conn.close()


# ── EDINET company cache (sector / edinet_code / financials) ──

def _ensure_edinet_columns(conn):
    """Add new columns if they don't exist yet (migration safety)."""
    existing = {r[1] for r in conn.execute("PRAGMA table_info(edinet_company_cache)").fetchall()}
    if "latest_financials_json" not in existing:
        conn.execute("ALTER TABLE edinet_company_cache ADD COLUMN latest_financials_json TEXT")
    if "fin_fetched_at" not in existing:
        conn.execute("ALTER TABLE edinet_company_cache ADD COLUMN fin_fetched_at TEXT")


def get_edinet_cached_companies(sec_codes):
    """Return {sec_code: {edinet_code, industry}} for entries cached within 90 days."""
    if not sec_codes:
        return {}
    conn = _connect()
    _ensure_edinet_columns(conn)
    placeholders = ",".join("?" * len(sec_codes))
    rows = conn.execute(
        f"""SELECT sec_code, edinet_code, industry FROM edinet_company_cache
            WHERE sec_code IN ({placeholders})
            AND fetched_at >= datetime('now', '-90 days')""",
        list(sec_codes),
    ).fetchall()
    conn.close()
    return {r["sec_code"]: {"edinet_code": r["edinet_code"], "industry": r["industry"]} for r in rows}


def get_edinet_cached_financials(sec_codes):
    """Return {sec_code: financials_dict} for entries where fin_fetched_at within 30 days."""
    if not sec_codes:
        return {}
    import json as _json
    conn = _connect()
    _ensure_edinet_columns(conn)
    placeholders = ",".join("?" * len(sec_codes))
    rows = conn.execute(
        f"""SELECT sec_code, latest_financials_json FROM edinet_company_cache
            WHERE sec_code IN ({placeholders})
            AND fin_fetched_at >= datetime('now', '-30 days')
            AND latest_financials_json IS NOT NULL""",
        list(sec_codes),
    ).fetchall()
    conn.close()
    result = {}
    for r in rows:
        try:
            result[r["sec_code"]] = _json.loads(r["latest_financials_json"])
        except Exception:
            pass
    return result


def save_edinet_companies(entries):
    """Upsert list of {sec_code, edinet_code, industry} into cache."""
    if not entries:
        return
    conn = _connect()
    _ensure_edinet_columns(conn)
    with conn:
        conn.executemany(
            """INSERT INTO edinet_company_cache (sec_code, edinet_code, industry, fetched_at)
               VALUES (:sec_code, :edinet_code, :industry, datetime('now'))
               ON CONFLICT(sec_code) DO UPDATE SET
                   edinet_code=excluded.edinet_code,
                   industry=excluded.industry,
                   fetched_at=excluded.fetched_at""",
            entries,
        )
    conn.close()


def save_edinet_financials(sec_code, financials_dict):
    """Cache the latest annual financials dict for a sec_code (30-day TTL)."""
    import json as _json
    conn = _connect()
    _ensure_edinet_columns(conn)
    with conn:
        conn.execute(
            """INSERT INTO edinet_company_cache (sec_code, latest_financials_json, fin_fetched_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(sec_code) DO UPDATE SET
                   latest_financials_json=excluded.latest_financials_json,
                   fin_fetched_at=excluded.fin_fetched_at""",
            (sec_code, _json.dumps(financials_dict, ensure_ascii=False)),
        )
    conn.close()


# ── LLM Phase 1: Users ──

def create_user(username, password_hash, display_name, role="user", avatar_emoji="👤"):
    """Create a new user. Returns new user id, or None if username exists."""
    conn = _connect()
    try:
        with conn:
            cur = conn.execute(
                """INSERT INTO users (username, password_hash, display_name, role, avatar_emoji)
                   VALUES (?, ?, ?, ?, ?)""",
                (username, password_hash, display_name, role, avatar_emoji),
            )
            return cur.lastrowid
    except Exception:
        return None
    finally:
        conn.close()


def get_user_by_id(user_id):
    conn = _connect()
    row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_by_username(username):
    conn = _connect()
    row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_user_last_login(user_id):
    conn = _connect()
    with conn:
        conn.execute(
            "UPDATE users SET last_login_at = datetime('now') WHERE id = ?",
            (user_id,),
        )
    conn.close()


def update_user_password(user_id, password_hash):
    conn = _connect()
    with conn:
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (password_hash, user_id),
        )
    conn.close()


def update_user_consent(user_id):
    conn = _connect()
    with conn:
        conn.execute(
            "UPDATE users SET consent_given_at = datetime('now') WHERE id = ? AND consent_given_at IS NULL",
            (user_id,),
        )
    conn.close()


def list_users():
    """Return all users (no password hashes)."""
    conn = _connect()
    rows = conn.execute(
        "SELECT id, username, display_name, avatar_emoji, role, created_at, last_login_at, consent_given_at FROM users ORDER BY id"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_user(user_id):
    conn = _connect()
    with conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.close()


# ── LLM Phase 1: Research Notes ──

def insert_note(user_id, title, question, answer, tickers, tags, index_name, llm_model, tool_calls):
    """Insert a research note. Returns new note id."""
    import json as _json
    conn = _connect()
    with conn:
        cur = conn.execute(
            """INSERT INTO research_notes
               (user_id, title, question, answer, tickers_json, tags_json,
                index_name, llm_model, tool_calls_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                title,
                question,
                answer,
                _json.dumps(tickers or [], ensure_ascii=False),
                _json.dumps(tags or [], ensure_ascii=False),
                index_name,
                llm_model,
                _json.dumps(tool_calls or [], ensure_ascii=False),
            ),
        )
        new_id = cur.lastrowid
    conn.close()
    return new_id


def _row_to_note(row):
    import json as _json
    d = dict(row)
    for k in ("tickers_json", "tags_json", "tool_calls_json"):
        raw = d.get(k)
        try:
            d[k.replace("_json", "")] = _json.loads(raw) if raw else []
        except Exception:
            d[k.replace("_json", "")] = []
        d.pop(k, None)
    return d


def get_notes_by_user(user_id, ticker=None, pinned_only=False, limit=50):
    """List notes for a user, optionally filtered by ticker or pinned status."""
    conn = _connect()
    sql = "SELECT * FROM research_notes WHERE user_id = ?"
    params = [user_id]
    if pinned_only:
        sql += " AND is_pinned = 1"
    if ticker:
        sql += " AND tickers_json LIKE ?"
        params.append(f'%"{ticker.upper()}"%')
    sql += " ORDER BY is_pinned DESC, created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_row_to_note(r) for r in rows]


def get_note_by_id(note_id, user_id=None):
    """Return a single note. If user_id given, enforces ownership."""
    conn = _connect()
    if user_id is not None:
        row = conn.execute(
            "SELECT * FROM research_notes WHERE id = ? AND user_id = ?",
            (note_id, user_id),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM research_notes WHERE id = ?", (note_id,)
        ).fetchone()
    conn.close()
    return _row_to_note(row) if row else None


def update_note_fields(note_id, user_id, **fields):
    """Update allowed fields on a note. Enforces user_id ownership."""
    import json as _json
    allowed = {"title", "question", "answer", "tickers_json", "tags_json", "is_pinned"}
    updates = {}
    for k, v in fields.items():
        if k == "tickers":
            updates["tickers_json"] = _json.dumps(v or [], ensure_ascii=False)
        elif k == "tags":
            updates["tags_json"] = _json.dumps(v or [], ensure_ascii=False)
        elif k in allowed:
            updates[k] = v
    if not updates:
        return False
    updates["updated_at"] = None  # sentinel — overwritten below
    set_clause = ", ".join(f"{k} = ?" for k in updates if k != "updated_at")
    values = [v for k, v in updates.items() if k != "updated_at"]
    set_clause += ", updated_at = datetime('now')"
    conn = _connect()
    with conn:
        cur = conn.execute(
            f"UPDATE research_notes SET {set_clause} WHERE id = ? AND user_id = ?",
            (*values, note_id, user_id),
        )
        changed = cur.rowcount > 0
    conn.close()
    return changed


def delete_note_by_id(note_id, user_id):
    conn = _connect()
    with conn:
        cur = conn.execute(
            "DELETE FROM research_notes WHERE id = ? AND user_id = ?",
            (note_id, user_id),
        )
        changed = cur.rowcount > 0
    conn.close()
    return changed


def toggle_note_pin(note_id, user_id):
    conn = _connect()
    with conn:
        cur = conn.execute(
            """UPDATE research_notes
               SET is_pinned = CASE WHEN is_pinned = 1 THEN 0 ELSE 1 END,
                   updated_at = datetime('now')
               WHERE id = ? AND user_id = ?""",
            (note_id, user_id),
        )
        changed = cur.rowcount > 0
    conn.close()
    return changed


def get_all_notes(ticker=None, limit=30):
    """Return notes from ALL users (owner-only use case). Includes author display_name."""
    conn = _connect()
    sql = """
        SELECT rn.*, u.display_name AS author_name, u.username AS author_username
        FROM research_notes rn
        JOIN users u ON u.id = rn.user_id
    """
    params = []
    if ticker:
        sql += " WHERE rn.tickers_json LIKE ?"
        params.append(f'%"{ticker.upper()}"%')
    sql += " ORDER BY rn.created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_row_to_note(r) for r in rows]


# ── LLM Phase 1: Usage tracking ──

def get_usage(user_id, date):
    conn = _connect()
    row = conn.execute(
        "SELECT * FROM user_usage WHERE user_id = ? AND date = ?",
        (user_id, date),
    ).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {
        "user_id": user_id,
        "date": date,
        "request_count": 0,
        "tokens_in": 0,
        "tokens_out": 0,
        "cost_usd": 0.0,
    }


def increment_usage(user_id, date, tokens_in, tokens_out, cost_usd):
    """UPSERT usage counters for a user+date."""
    conn = _connect()
    with conn:
        conn.execute(
            """INSERT INTO user_usage (user_id, date, request_count, tokens_in, tokens_out, cost_usd)
               VALUES (?, ?, 1, ?, ?, ?)
               ON CONFLICT(user_id, date) DO UPDATE SET
                   request_count = request_count + 1,
                   tokens_in = tokens_in + excluded.tokens_in,
                   tokens_out = tokens_out + excluded.tokens_out,
                   cost_usd = cost_usd + excluded.cost_usd""",
            (user_id, date, tokens_in, tokens_out, cost_usd),
        )
    conn.close()


def get_global_cost_today(date):
    conn = _connect()
    row = conn.execute(
        "SELECT COALESCE(SUM(cost_usd), 0) AS total FROM user_usage WHERE date = ?",
        (date,),
    ).fetchone()
    conn.close()
    return float(row["total"]) if row else 0.0


def get_all_users_usage(date):
    """Owner admin view of all users' usage for a given date."""
    conn = _connect()
    rows = conn.execute(
        """SELECT u.id, u.username, u.display_name, u.role,
                  COALESCE(uu.request_count, 0) AS request_count,
                  COALESCE(uu.tokens_in, 0) AS tokens_in,
                  COALESCE(uu.tokens_out, 0) AS tokens_out,
                  COALESCE(uu.cost_usd, 0) AS cost_usd
           FROM users u
           LEFT JOIN user_usage uu ON uu.user_id = u.id AND uu.date = ?
           ORDER BY u.id""",
        (date,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
