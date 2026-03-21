"""Surge - SQLite Database Layer"""

import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "surge.db")


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
