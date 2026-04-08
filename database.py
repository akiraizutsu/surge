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

    conn.close()


def _add_column_if_missing(conn, table, column, col_type):
    """Add a column to a table only if it doesn't already exist."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


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
                    obv_slope, obv_divergence, max_drawdown_3m, current_drawdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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

        index_label = {"sp500": "S&P 500", "nasdaq100": "NASDAQ 100", "nikkei225": "日経225", "growth250": "グロース250"}.get(idx, idx)
        results[idx] = {
            "index": index_label,
            "total_screened": session["total_screened"],
            "generated_at": session["generated_at"],
            "momentum_ranking": ranking,
            "value_gap_ranking": vg_ranking,
            "sector_rotation": [],
            "breakout_ranking": [],
            "time_arb_ranking": [],
            "smallcap_ranking": [],
            "sector_distribution": sector_dist,
            "summary": {
                "avg_score": avg_score,
                "overheat_count": overheat_count,
                "golden_cross_count": golden_count,
            },
            "latest_breadth": latest_breadth,
            "regime": regime,
        }
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

def add_to_watchlist(ticker):
    conn = _connect()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (ticker) VALUES (?)", (ticker.upper(),)
        )
    conn.close()


def remove_from_watchlist(ticker):
    conn = _connect()
    with conn:
        conn.execute("DELETE FROM watchlist WHERE ticker = ?", (ticker.upper(),))
    conn.close()


def get_watchlist():
    conn = _connect()
    rows = conn.execute("SELECT ticker FROM watchlist ORDER BY added_at DESC").fetchall()
    conn.close()
    return [r["ticker"] for r in rows]


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
