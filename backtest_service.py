"""Sprint 4: Backtest Service.

Given a past screening session, fetches forward returns via yfinance and
computes performance statistics vs a benchmark.

Horizons: 5 / 20 / 60 business days (≈ 1 week / 1 month / 3 months).

Returns (per run):
    {
        "session_id": int,
        "index_name": str,
        "session_date": str,
        "horizon_days": int,
        "top_n": int,
        "benchmark_ticker": str,
        "stats": {
            "avg_return": float,
            "median_return": float,
            "win_rate": float,          # fraction with return > 0
            "benchmark_return": float,
            "excess_return": float,     # avg_return - benchmark_return
            "sample_size": int,
        },
        "detail": [
            {
                "rank": int,
                "ticker": str,
                "name": str,
                "entry_price": float,
                "exit_price": float | None,
                "return_pct": float | None,
                "vs_benchmark": float | None,  # return_pct - benchmark_return
            },
            ...
        ],
    }
"""

import math
from datetime import datetime, timedelta


# Benchmark ticker by index
_BENCHMARK = {
    "sp500":    "SPY",
    "nasdaq100":"QQQ",
    "nikkei225":"^N225",
    "growth250":"^TOPIX",
}

VALID_HORIZONS = {5, 20, 60}


def run_backtest(session_id: int, horizon_days: int, top_n: int,
                 index_name: str, session_date_str: str, results: list) -> dict:
    """Compute forward returns for top_n stocks from a past screening session.

    Args:
        session_id: DB session id.
        horizon_days: business days forward (5, 20, or 60).
        top_n: number of top-ranked stocks to include.
        index_name: 'sp500'|'nasdaq100'|'nikkei225'|'growth250'.
        session_date_str: generated_at string from screening_sessions (YYYY-MM-DD HH:MM).
        results: list of dicts from get_session_results() — already in rank order.

    Returns:
        dict described in module docstring.
    """
    import yfinance as yf
    import pandas as pd

    if horizon_days not in VALID_HORIZONS:
        horizon_days = 20

    benchmark_ticker = _BENCHMARK.get(index_name, "SPY")

    # Parse session date
    try:
        session_date = _parse_date(session_date_str)
    except Exception:
        return _error_result(session_id, index_name, session_date_str, horizon_days, top_n,
                             "Invalid session date")

    # Check if we have enough future data (need horizon_days trading days after session_date)
    today = datetime.utcnow().date()
    # Rough calendar day estimate: add 50% buffer for weekends/holidays
    min_calendar_days = int(horizon_days * 1.5) + 5
    if (today - session_date).days < min_calendar_days:
        return _error_result(session_id, index_name, session_date_str, horizon_days, top_n,
                             f"データ不足: {horizon_days}営業日後のデータがまだありません")

    # Take top N results
    top_results = results[:top_n]
    if not top_results:
        return _error_result(session_id, index_name, session_date_str, horizon_days, top_n,
                             "スクリーニング結果なし")

    tickers = [r["ticker"] for r in top_results]
    all_tickers = tickers + [benchmark_ticker]

    # Download price data from session_date to session_date + generous buffer
    fetch_start = session_date
    fetch_end   = session_date + timedelta(days=horizon_days * 2 + 40)
    if fetch_end > today:
        fetch_end = today

    try:
        raw = yf.download(
            all_tickers,
            start=fetch_start.strftime("%Y-%m-%d"),
            end=fetch_end.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
        if raw.empty:
            return _error_result(session_id, index_name, session_date_str, horizon_days, top_n,
                                 "価格データ取得失敗")

        # Extract Close prices
        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"]
        else:
            close = raw[["Close"]] if "Close" in raw.columns else raw

    except Exception as e:
        return _error_result(session_id, index_name, session_date_str, horizon_days, top_n,
                             f"yfinance error: {e}")

    # Get entry and exit indices
    if len(close) < 2:
        return _error_result(session_id, index_name, session_date_str, horizon_days, top_n,
                             "取得データ行数不足")

    entry_idx = 0  # first available trading day >= session_date
    exit_idx  = min(horizon_days, len(close) - 1)

    # Benchmark return
    bench_col = benchmark_ticker if benchmark_ticker in close.columns else None
    bench_entry = _safe_price(close, bench_col, entry_idx)
    bench_exit  = _safe_price(close, bench_col, exit_idx)
    bench_return = _pct(bench_entry, bench_exit)

    # Per-ticker returns
    detail = []
    returns = []
    for r in top_results:
        ticker = r["ticker"]
        col = ticker if ticker in close.columns else None
        entry_p = _safe_price(close, col, entry_idx)
        exit_p  = _safe_price(close, col, exit_idx)
        ret_pct = _pct(entry_p, exit_p)
        vs_bench = (ret_pct - bench_return) if (ret_pct is not None and bench_return is not None) else None
        if ret_pct is not None:
            returns.append(ret_pct)
        detail.append({
            "rank":        r.get("rank"),
            "ticker":      ticker,
            "name":        r.get("name", ""),
            "entry_price": _round(entry_p),
            "exit_price":  _round(exit_p),
            "return_pct":  _round(ret_pct),
            "vs_benchmark": _round(vs_bench),
        })

    # Aggregate stats
    stats = _compute_stats(returns, bench_return)

    return {
        "session_id":       session_id,
        "index_name":       index_name,
        "session_date":     session_date_str,
        "horizon_days":     horizon_days,
        "top_n":            top_n,
        "benchmark_ticker": benchmark_ticker,
        "stats":            stats,
        "detail":           detail,
        "error":            None,
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _parse_date(date_str: str):
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unparseable date: {date_str}")


def _safe_price(close, col, idx):
    if col is None:
        return None
    try:
        val = float(close[col].iloc[idx])
        return None if math.isnan(val) else val
    except Exception:
        return None


def _pct(entry, exit_p):
    if entry is None or exit_p is None or entry <= 0:
        return None
    return round((exit_p / entry - 1) * 100, 2)


def _round(v, decimals=2):
    if v is None:
        return None
    try:
        return round(float(v), decimals)
    except Exception:
        return None


def _compute_stats(returns: list, bench_return) -> dict:
    n = len(returns)
    if n == 0:
        return {
            "avg_return": None, "median_return": None,
            "win_rate": None, "benchmark_return": _round(bench_return),
            "excess_return": None, "sample_size": 0,
        }
    avg = sum(returns) / n
    sorted_r = sorted(returns)
    mid = n // 2
    median = sorted_r[mid] if n % 2 == 1 else (sorted_r[mid - 1] + sorted_r[mid]) / 2
    win_rate = sum(1 for r in returns if r > 0) / n
    excess = (avg - bench_return) if bench_return is not None else None
    return {
        "avg_return":       round(avg, 2),
        "median_return":    round(median, 2),
        "win_rate":         round(win_rate * 100, 1),
        "benchmark_return": _round(bench_return),
        "excess_return":    _round(excess),
        "sample_size":      n,
    }


def _error_result(session_id, index_name, session_date_str, horizon_days, top_n, msg):
    return {
        "session_id":       session_id,
        "index_name":       index_name,
        "session_date":     session_date_str,
        "horizon_days":     horizon_days,
        "top_n":            top_n,
        "benchmark_ticker": _BENCHMARK.get(index_name, "SPY"),
        "stats":            None,
        "detail":           [],
        "error":            msg,
    }
