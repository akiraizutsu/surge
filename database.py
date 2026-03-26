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
                squeeze_score REAL
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
        """)
    conn.close()


# ── Sessions ──

def save_session(meta):
    conn = _connect()
    with conn:
        cur = conn.execute(
            "INSERT INTO screening_sessions (index_name, top_n, total_screened, generated_at) VALUES (?, ?, ?, ?)",
            (meta["index_name"], meta["top_n"], meta["total_screened"], meta["generated_at"]),
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
            conn.execute(
                """INSERT INTO screening_results (
                    session_id, rank, ticker, name, sector, price, momentum_score,
                    ret_1d, ret_1w, ret_1m, ret_3m, vol_ratio, ma50_dev, ma200_dev,
                    macd_hist_pct, rsi, golden_cross, overheat,
                    market_cap_b, pe_trailing, pe_forward, pb, dividend_yield,
                    revenue_growth, earnings_growth, eps, target_price, recommendation,
                    sector_etf, rs_1m, rs_3m,
                    short_pct_of_float, short_ratio, shares_short, shares_short_prior_month,
                    float_shares, short_change_pct, squeeze_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                ),
            )
    conn.close()


def get_sessions(limit=10):
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM screening_sessions ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_session_results(session_id):
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM screening_results WHERE session_id = ? ORDER BY rank", (session_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_latest_sessions_by_index():
    """Get the most recent session for each index, with full results."""
    conn = _connect()
    results = {}
    for idx in ("sp500", "nasdaq100", "nikkei225"):
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
                },
                "fundamentals": {
                    "market_cap_b": r["market_cap_b"], "pe_trailing": r["pe_trailing"],
                    "pe_forward": r["pe_forward"], "pb": r["pb"],
                    "dividend_yield": r["dividend_yield"],
                    "revenue_growth": r["revenue_growth"],
                    "earnings_growth": r["earnings_growth"],
                    "eps": r["eps"], "target_price": r["target_price"],
                    "recommendation": r["recommendation"],
                },
                "short_interest": {
                    "short_pct_of_float": r["short_pct_of_float"],
                    "short_ratio": r["short_ratio"],
                    "shares_short": r["shares_short"],
                    "shares_short_prior_month": r["shares_short_prior_month"],
                    "float_shares": r["float_shares"],
                    "short_change_pct": r["short_change_pct"],
                },
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

        index_label = {"sp500": "S&P 500", "nasdaq100": "NASDAQ 100", "nikkei225": "日経225"}[idx]
        results[idx] = {
            "index": index_label,
            "total_screened": session["total_screened"],
            "generated_at": session["generated_at"],
            "momentum_ranking": ranking,
            "sector_distribution": sector_dist,
            "summary": {
                "avg_score": avg_score,
                "overheat_count": overheat_count,
                "golden_cross_count": golden_count,
            },
            "latest_breadth": latest_breadth,
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
