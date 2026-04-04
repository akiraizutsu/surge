"""Momentum screening engine for S&P 500, NASDAQ 100, and Nikkei 225."""

import io
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import yfinance as yf

import scoring_service
import tagging_service
import questions_service
import regime_service
import quality_service

warnings.filterwarnings("ignore")

# ── EDINET DB helpers (for Nikkei 225 data enrichment) ───────────────────────
_EDINET_BASE = "https://edinetdb.jp/v1"
# Max new EDINET search API calls per screening run (free plan: 100/day)
# Fundamentals calls (top_n) are added on top, so keep this ≤ 60.
_EDINET_MAX_NEW_SEARCHES = 60


def _edinet_search_api(sec_code, api_key):
    """Single EDINET search API call. Returns {edinet_code, industry} or {}."""
    try:
        r = requests.get(
            f"{_EDINET_BASE}/search",
            params={"q": sec_code},
            headers={"X-API-Key": api_key},
            timeout=10,
        )
        if r.status_code != 200:
            return {}
        for c in (r.json().get("data") or []):
            sc = str(c.get("sec_code") or "")
            if sc[:-1] == sec_code or sc == sec_code:
                return {
                    "edinet_code": c.get("edinet_code") or "",
                    "industry": c.get("industry") or "",
                }
    except Exception:
        pass
    return {}


def _edinet_latest_annual(edinet_code, api_key):
    """Return the most recent annual financial record dict, or {}."""
    try:
        r = requests.get(
            f"{_EDINET_BASE}/companies/{edinet_code}/financials",
            headers={"X-API-Key": api_key},
            timeout=15,
        )
        if r.status_code != 200:
            return {}
        rows = r.json().get("data") or []
        if not rows:
            return {}
        return sorted(rows, key=lambda x: x.get("fiscal_year") or 0)[-1]
    except Exception:
        return {}


def get_edinet_sectors(tickers, api_key, progress_cb=None):
    """Return {ticker: industry} using SQLite cache + limited EDINET API calls.

    Cache TTL: 90 days. New API calls per run capped at _EDINET_MAX_NEW_SEARCHES.
    """
    from database import get_edinet_cached_companies, save_edinet_companies

    sec_codes = [t.replace(".T", "") for t in tickers]
    ticker_map = {t.replace(".T", ""): t for t in tickers}

    # Load cached entries
    cached = get_edinet_cached_companies(sec_codes)

    # Determine which need fetching (not in cache), up to the per-run limit
    uncached = [sc for sc in sec_codes if sc not in cached]
    to_fetch = uncached[:_EDINET_MAX_NEW_SEARCHES]

    # Fetch uncached ones in parallel
    new_entries = []

    def _fetch(sec_code):
        return sec_code, _edinet_search_api(sec_code, api_key)

    if to_fetch:
        done = 0
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(_fetch, sc): sc for sc in to_fetch}
            for future in as_completed(futures):
                sc, info = future.result()
                new_entries.append({
                    "sec_code": sc,
                    "edinet_code": info.get("edinet_code", ""),
                    "industry": info.get("industry", ""),
                })
                cached[sc] = info  # merge into local cache
                done += 1
                if progress_cb and done % 20 == 0:
                    pct = 3 + int(done / len(to_fetch) * 5)
                    progress_cb(f"EDINETセクター取得 {done}/{len(to_fetch)}件...", pct)

        save_edinet_companies(new_entries)

    # Build result dict
    result = {}
    for sc, info in cached.items():
        ind = info.get("industry", "")
        if ind:
            result[ticker_map[sc]] = ind
    return result


def get_edinet_fundamentals(tickers, api_key, price_map, progress_cb=None):
    """Fetch latest annual fundamentals from EDINET for top_n JP tickers.

    Uses SQLite cache for both edinet_code lookup and financial data (30-day TTL).
    price_map: {ticker: current_price} for P/B and dividend yield calc.
    Returns {ticker: {per, pb, eps, roe, dividend_yield, revenue_b, net_income_b}}.
    """
    from database import (
        get_edinet_cached_companies, save_edinet_companies,
        get_edinet_cached_financials, save_edinet_financials,
    )

    sec_codes = [t.replace(".T", "") for t in tickers]
    ticker_map = {t.replace(".T", ""): t for t in tickers}

    # Load edinet_code from company cache; fetch missing ones via API
    company_cache = get_edinet_cached_companies(sec_codes)
    missing_codes = [sc for sc in sec_codes if sc not in company_cache or not company_cache[sc].get("edinet_code")]
    if missing_codes:
        new_entries = []
        for sc in missing_codes:
            info = _edinet_search_api(sc, api_key)
            new_entries.append({"sec_code": sc, "edinet_code": info.get("edinet_code", ""), "industry": info.get("industry", "")})
            company_cache[sc] = info
        save_edinet_companies(new_entries)

    # Load financial data from cache; fetch missing ones via API
    fin_cache = get_edinet_cached_financials(sec_codes)
    need_fin = [sc for sc in sec_codes if sc not in fin_cache and (company_cache.get(sc) or {}).get("edinet_code")]

    def _fetch_fin(sec_code):
        edinet_code = (company_cache.get(sec_code) or {}).get("edinet_code", "")
        return sec_code, _edinet_latest_annual(edinet_code, api_key)

    if need_fin:
        done = 0
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_fetch_fin, sc): sc for sc in need_fin}
            for future in as_completed(futures):
                sc, ann = future.result()
                done += 1
                if progress_cb:
                    pct = 70 + int(done / len(need_fin) * 20)
                    progress_cb(f"EDINETファンダメンタルズ {done}/{len(need_fin)}...", pct)
                if ann:
                    fin_cache[sc] = ann
                    save_edinet_financials(sc, ann)

    # Build result from merged cache
    result = {}
    for sc in sec_codes:
        ann = fin_cache.get(sc)
        if not ann:
            continue
        ticker = ticker_map[sc]
        price_raw = price_map.get(ticker)

        bps = ann.get("bps") or ann.get("adjusted_bps")
        pb = None
        if bps and price_raw and float(bps) > 0:
            pb = round(float(price_raw) / float(bps), 2)

        div_ps = ann.get("dividend_per_share") or ann.get("adjusted_dividend_per_share")
        div_yield = None
        if div_ps and price_raw and float(price_raw) > 0:
            div_yield = round(float(div_ps) / float(price_raw) * 100, 2)  # % scale, matches yfinance

        revenue = ann.get("revenue")
        net_income = ann.get("net_income")

        result[ticker] = {
            "per":          ann.get("per"),
            "pb":           pb,
            "eps":          ann.get("eps") or ann.get("adjusted_eps"),
            "roe":          ann.get("roe_official"),
            "dividend_yield": div_yield,
            "revenue_b":    round(float(revenue) / 1e8, 1) if revenue else None,
            "net_income_b": round(float(net_income) / 1e8, 1) if net_income else None,
        }

    return result


def get_sp500_tickers():
    """Fetch S&P 500 constituent tickers from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    df = tables[0]
    tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
    sectors = dict(zip(
        df["Symbol"].str.replace(".", "-", regex=False),
        df["GICS Sector"]
    ))
    return tickers, sectors


def get_nasdaq100_tickers():
    """Fetch NASDAQ 100 constituent tickers from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    for table in tables:
        cols = [str(c).lower() for c in table.columns]
        if "ticker" in cols or "symbol" in cols:
            col_name = "Ticker" if "ticker" in cols else "Symbol"
            tickers = table[col_name].str.replace(".", "-", regex=False).tolist()
            sector_col = None
            for c in table.columns:
                cl = str(c).lower()
                if "sector" in cl or "industry" in cl or "subsector" in cl:
                    sector_col = c
                    break
            if sector_col:
                sectors = dict(zip(
                    table[col_name].str.replace(".", "-", regex=False),
                    table[sector_col]
                ))
            else:
                sectors = {t: "N/A" for t in tickers}
            return tickers, sectors
    raise ValueError("Could not find NASDAQ 100 ticker table")


def get_nikkei225_tickers():
    """Fetch Nikkei 225 constituent tickers from Wikipedia (Japanese).

    Returns (tickers, sectors, names) where names maps ticker -> Japanese name.
    """
    url = "https://ja.wikipedia.org/wiki/日経平均株価"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    tickers = []
    names = {}
    for table in tables:
        cols = [str(c) for c in table.columns]
        if "証券コード" in cols and "銘柄" in cols:
            for _, row in table.iterrows():
                code = str(row["証券コード"]).strip()
                if code.isdigit():
                    t = code + ".T"
                    tickers.append(t)
                    names[t] = str(row["銘柄"]).strip()
    if not tickers:
        raise ValueError("Could not find Nikkei 225 ticker table")
    # All Nikkei 225 stocks use ^N225 as benchmark (no sector ETF mapping)
    sectors = {t: "N/A" for t in tickers}
    return tickers, sectors, names


def get_growth250_tickers():
    """Fetch TSE Growth Market 250 constituent tickers from Wikipedia.

    Returns (tickers, sectors, names).
    """
    url = "https://ja.wikipedia.org/wiki/東証グロース市場250指数"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    tickers = []
    names = {}
    for table in tables:
        cols = [str(c) for c in table.columns]
        if "コード" in cols and "銘柄名" in cols:
            for _, row in table.iterrows():
                code = str(row["コード"]).strip()
                if code.isdigit() and len(code) == 4:
                    t = code + ".T"
                    tickers.append(t)
                    names[t] = str(row["銘柄名"]).strip()
            break
    if not tickers:
        raise ValueError("Could not find Growth 250 ticker table on Wikipedia")
    sectors = {t: "N/A" for t in tickers}
    return tickers, sectors, names


def compute_rsi(series, period=14):
    """Compute RSI."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(series, fast=12, slow=26, signal=9):
    """Compute MACD line, signal line, and histogram."""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


RS_ALPHA = 2.0  # Threshold for relative strength classification (%)

THEME_TICKERS = {"MSTR", "COIN", "MARA", "RIOT", "CLSK", "BITF", "HUT"}

SECTOR_ETF_MAP = {
    "Information Technology": "XLK",
    "Technology": "XLK",
    "Health Care": "XLV",
    "Healthcare": "XLV",
    "Financials": "XLF",
    "Energy": "XLE",
    "Consumer Discretionary": "XLY",
    "Industrials": "XLI",
    "Utilities": "XLU",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
    "Consumer Staples": "XLP",
}


def _get_benchmark_returns(sectors_used, start_date, end_date, is_japan=False):
    """Fetch 1M and 3M returns for benchmark indices/ETFs.

    For US stocks: sector ETFs + SPY fallback.
    For Japanese stocks: ^N225 only (no sector ETF mapping).
    Returns dict: symbol -> {ret_1m, ret_3m}.
    """
    if is_japan:
        etf_list = ["^N225"]
    else:
        etfs_needed = set()
        for sector in sectors_used:
            etf = SECTOR_ETF_MAP.get(sector)
            if etf:
                etfs_needed.add(etf)
        etfs_needed.add("SPY")  # fallback
        etf_list = list(etfs_needed)

    if not etf_list:
        return {}

    etf_data = yf.download(
        etf_list,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        group_by="ticker",
        threads=True,
        progress=False,
    )

    etf_returns = {}
    for etf in etf_list:
        try:
            if len(etf_list) > 1:
                close = etf_data[etf]["Close"].dropna()
            else:
                close = etf_data["Close"].dropna()

            ret_1m = round(float((close.iloc[-1] / close.iloc[-22] - 1) * 100), 2) if len(close) >= 22 else 0
            ret_3m = round(float((close.iloc[-1] / close.iloc[-66] - 1) * 100), 2) if len(close) >= 66 else 0
            etf_returns[etf] = {"ret_1m": ret_1m, "ret_3m": ret_3m}
        except Exception:
            etf_returns[etf] = {"ret_1m": 0, "ret_3m": 0}

    return etf_returns


def screen_momentum(tickers, sectors, progress_cb=None, is_japan=False):
    """Screen all tickers for momentum indicators."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=400)

    if progress_cb:
        progress_cb("Downloading price data...", 5)

    data = yf.download(
        tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        group_by="ticker",
        threads=True,
        progress=False,
    )

    # Fetch benchmark returns for relative strength
    sectors_used = set(sectors.values())
    etf_returns = _get_benchmark_returns(sectors_used, start_date, end_date, is_japan=is_japan)
    if is_japan:
        fallback_returns = etf_returns.get("^N225", {"ret_1m": 0, "ret_3m": 0})
    else:
        fallback_returns = etf_returns.get("SPY", {"ret_1m": 0, "ret_3m": 0})

    results = []
    total = len(tickers)

    for i, ticker in enumerate(tickers):
        if progress_cb and (i + 1) % 50 == 0:
            pct = 5 + int((i / total) * 60)
            progress_cb(f"Analyzing {i+1}/{total}...", pct)
        try:
            if len(tickers) > 1:
                df = data[ticker].dropna()
            else:
                df = data.dropna()

            if len(df) < 60:
                continue

            close = df["Close"]
            volume = df["Volume"]
            current_price = float(close.iloc[-1])

            ret_1d = (close.iloc[-1] / close.iloc[-2] - 1) * 100 if len(close) >= 2 else 0
            ret_1w = (close.iloc[-1] / close.iloc[-6] - 1) * 100 if len(close) >= 6 else 0
            ret_1m = (close.iloc[-1] / close.iloc[-22] - 1) * 100 if len(close) >= 22 else 0
            ret_3m = (close.iloc[-1] / close.iloc[-66] - 1) * 100 if len(close) >= 66 else 0

            vol_5d_avg = volume.iloc[-5:].mean()
            vol_20d_avg = volume.iloc[-20:].mean()
            vol_ratio = vol_5d_avg / vol_20d_avg if vol_20d_avg > 0 else 1.0

            ma_25 = close.iloc[-25:].mean() if len(close) >= 25 else current_price
            ma_50 = close.iloc[-50:].mean() if len(close) >= 50 else current_price
            ma_200 = close.iloc[-200:].mean() if len(close) >= 200 else current_price
            ma25_dev = (current_price / ma_25 - 1) * 100 if ma_25 > 0 else 0
            ma50_dev = (current_price / ma_50 - 1) * 100
            ma200_dev = (current_price / ma_200 - 1) * 100

            macd_line, signal_line, histogram = compute_macd(close)
            macd_hist = histogram.iloc[-1]
            macd_hist_pct = (macd_hist / current_price) * 100

            rsi = compute_rsi(close)
            rsi_val = rsi.iloc[-1]

            above_50ma = current_price > ma_50
            above_200ma = current_price > ma_200
            golden_cross = bool(above_50ma and above_200ma)

            # 52-week high/low and breakout detection
            trading_days_52w = min(252, len(close))
            high_52w = float(df["High"].iloc[-trading_days_52w:].max()) if "High" in df.columns else current_price
            low_52w = float(df["Low"].iloc[-trading_days_52w:].min()) if "Low" in df.columns else current_price
            dist_from_high = round((current_price / high_52w - 1) * 100, 2) if high_52w > 0 else 0
            dist_from_low = round((current_price / low_52w - 1) * 100, 2) if low_52w > 0 else 0
            is_breakout = current_price >= high_52w * 0.99  # within 1% of 52W high

            # Bollinger Band width (squeeze detection)
            sma_20 = close.iloc[-20:].mean() if len(close) >= 20 else current_price
            std_20 = close.iloc[-20:].std() if len(close) >= 20 else 0
            bb_upper = sma_20 + 2 * std_20
            bb_lower = sma_20 - 2 * std_20
            bb_width = round(float((bb_upper - bb_lower) / sma_20 * 100), 2) if sma_20 > 0 else 0
            bb_squeeze = bb_width < 6  # narrow bands = compression

            # Relative strength vs benchmark
            stock_sector = sectors.get(ticker, "N/A")
            if is_japan:
                sector_etf = "^N225"
                etf_ret = fallback_returns
            else:
                sector_etf = SECTOR_ETF_MAP.get(stock_sector)
                etf_ret = etf_returns.get(sector_etf, fallback_returns)
            rs_1m = round(float(ret_1m) - etf_ret["ret_1m"], 2)
            rs_3m = round(float(ret_3m) - etf_ret["ret_3m"], 2)

            # RS classification
            if ticker in THEME_TICKERS:
                rs_label = "theme"
            elif rs_1m > RS_ALPHA and rs_3m > 0:
                rs_label = "prime"
            elif rs_1m > RS_ALPHA:
                rs_label = "short_term"
            else:
                rs_label = "sector_driven"

            # Sprint 3: quality score (computed from raw OHLCV here while df is in scope)
            _quality = quality_service.compute_quality(
                df=df,
                momentum_score=0,   # placeholder; real score assigned later after ranking
                ma50_dev=float(ma50_dev),
                ma25_dev=float(ma25_dev),
                bb_width=float(bb_width),
                days_to_earnings=None,  # fetched later in get_fundamentals
                rsi=float(rsi_val),
            )

            results.append({
                "ticker": ticker,
                "sector": stock_sector,
                "price": round(float(current_price), 2),
                "ret_1d": round(float(ret_1d), 2),
                "ret_1w": round(float(ret_1w), 2),
                "ret_1m": round(float(ret_1m), 2),
                "ret_3m": round(float(ret_3m), 2),
                "vol_ratio": round(float(vol_ratio), 2),
                "ma25_dev": round(float(ma25_dev), 2),
                "ma50_dev": round(float(ma50_dev), 2),
                "ma200_dev": round(float(ma200_dev), 2),
                "macd_hist": round(float(macd_hist_pct), 4),
                "rsi": round(float(rsi_val), 1),
                "golden_cross": golden_cross,
                "sector_etf": sector_etf or "SPY",
                "rs_1m": rs_1m,
                "rs_3m": rs_3m,
                "rs_label": rs_label,
                "high_52w": round(high_52w, 2),
                "low_52w": round(low_52w, 2),
                "dist_from_high": dist_from_high,
                "dist_from_low": dist_from_low,
                "is_breakout": is_breakout,
                "bb_width": bb_width,
                "bb_squeeze": bb_squeeze,
                # Sprint 3 quality (earnings_days not yet available here)
                "_quality_components": _quality["quality_components"],
                "_quality_base_score": _quality["quality_score"],
            })
        except Exception:
            continue

    return results, data


def compute_breadth(data, tickers):
    """Compute daily advance/decline counts and cumulative ADL from price data.

    Args:
        data: yfinance DataFrame (multi-ticker or single).
        tickers: list of ticker symbols.

    Returns:
        list of dicts with keys: date, advances, declines, unchanged, ad_diff, adl, breadth_pct
    """
    # Build a DataFrame of daily Close prices for all tickers
    closes = {}
    for ticker in tickers:
        try:
            if len(tickers) > 1:
                s = data[ticker]["Close"].dropna()
            else:
                s = data["Close"].dropna()
            closes[ticker] = s
        except Exception:
            continue

    if not closes:
        return []

    close_df = pd.DataFrame(closes)
    # Daily returns: positive = advance, negative = decline
    daily_ret = close_df.pct_change()
    # Drop first row (NaN from pct_change)
    daily_ret = daily_ret.iloc[1:]

    breadth_data = []
    adl = 0.0

    for date_idx, row in daily_ret.iterrows():
        valid = row.dropna()
        advances = int((valid > 0).sum())
        declines = int((valid < 0).sum())
        unchanged = int((valid == 0).sum())
        ad_diff = advances - declines
        adl += ad_diff
        total = advances + declines + unchanged
        breadth_pct = round((advances - declines) / total * 100, 1) if total > 0 else 0

        date_str = date_idx.strftime("%Y-%m-%d") if hasattr(date_idx, "strftime") else str(date_idx)
        breadth_data.append({
            "date": date_str,
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "ad_diff": ad_diff,
            "adl": round(adl, 1),
            "breadth_pct": breadth_pct,
        })

    return breadth_data


def compute_sector_rotation(results, benchmark_returns, is_japan=False):
    """Compute sector rotation data — average returns & RS per sector.

    Returns: list of {sector, etf, ret_1m_avg, ret_3m_avg, etf_1m, etf_3m,
                       rs_1m_avg, stock_count, trend}
    """
    # Group results by sector
    sector_data = {}
    for r in results:
        s = r.get("sector", "Unknown")
        if not s:
            continue
        if s not in sector_data:
            sector_data[s] = {"ret_1m": [], "ret_3m": [], "rs_1m": []}
        if r.get("ret_1m") is not None:
            sector_data[s]["ret_1m"].append(r["ret_1m"])
        if r.get("ret_3m") is not None:
            sector_data[s]["ret_3m"].append(r["ret_3m"])
        if r.get("rs_1m") is not None:
            sector_data[s]["rs_1m"].append(r["rs_1m"])

    rotation = []
    for sector, vals in sector_data.items():
        if not vals["ret_1m"]:
            continue

        ret_1m_avg = round(sum(vals["ret_1m"]) / len(vals["ret_1m"]), 2)
        ret_3m_avg = round(sum(vals["ret_3m"]) / len(vals["ret_3m"]), 2) if vals["ret_3m"] else 0
        rs_1m_avg = round(sum(vals["rs_1m"]) / len(vals["rs_1m"]), 2) if vals["rs_1m"] else 0

        etf = SECTOR_ETF_MAP.get(sector, "^N225" if is_japan else "SPY")
        br = benchmark_returns.get(etf, {})

        # Determine trend: 1M stronger than 3M = accelerating
        if ret_1m_avg > 0 and ret_3m_avg > 0:
            trend = "加速" if ret_1m_avg > ret_3m_avg / 3 else "安定"
        elif ret_1m_avg > 0 and ret_3m_avg <= 0:
            trend = "回復"
        elif ret_1m_avg <= 0 and ret_3m_avg > 0:
            trend = "減速"
        else:
            trend = "衰退"

        rotation.append({
            "sector": sector,
            "etf": etf,
            "ret_1m_avg": ret_1m_avg,
            "ret_3m_avg": ret_3m_avg,
            "etf_1m": br.get("ret_1m", 0),
            "etf_3m": br.get("ret_3m", 0),
            "rs_1m_avg": rs_1m_avg,
            "stock_count": len(vals["ret_1m"]),
            "trend": trend,
        })

    rotation.sort(key=lambda x: x["ret_1m_avg"], reverse=True)
    return rotation


def compute_momentum_score(results):
    """Compute composite momentum score using percentile ranking."""
    df = pd.DataFrame(results)

    df["score_ret_1m"] = df["ret_1m"].rank(pct=True)
    df["score_ret_3m"] = df["ret_3m"].rank(pct=True)
    df["score_vol"] = df["vol_ratio"].rank(pct=True)
    df["score_ma50"] = df["ma50_dev"].rank(pct=True)
    df["score_macd"] = df["macd_hist"].rank(pct=True)
    df["score_rsi"] = df["rsi"].rank(pct=True)

    df["momentum_score"] = (
        df["score_ret_1m"] * 0.20
        + df["score_ret_3m"] * 0.20
        + df["score_vol"] * 0.15
        + df["score_ma50"] * 0.15
        + df["score_macd"] * 0.15
        + df["score_rsi"] * 0.15
    )

    df["overheat"] = df["rsi"] > 70
    df = df.sort_values("momentum_score", ascending=False)
    return df


def get_fundamentals(tickers, progress_cb=None, is_japan=False):
    """Fetch fundamental data for given tickers."""
    fundamentals = []
    total = len(tickers)

    for i, ticker in enumerate(tickers):
        if progress_cb:
            pct = 70 + int((i / total) * 25)
            progress_cb(f"Fetching fundamentals {i+1}/{total}...", pct)
        try:
            info = yf.Ticker(ticker).info
            # Short interest data
            shares_short = info.get("sharesShort")
            shares_short_prior = info.get("sharesShortPriorMonth")
            short_change_pct = None
            if shares_short and shares_short_prior and shares_short_prior > 0:
                short_change_pct = round((shares_short - shares_short_prior) / shares_short_prior * 100, 1)

            # Earnings date
            earnings_date = None
            days_to_earnings = None
            try:
                cal = yf.Ticker(ticker).calendar
                if isinstance(cal, dict):
                    ed_list = cal.get("Earnings Date", [])
                    if ed_list:
                        ed = ed_list[0]  # datetime.date object
                        from datetime import date as _date
                        if isinstance(ed, _date):
                            earnings_date = ed.strftime("%Y-%m-%d")
                            days_to_earnings = (ed - datetime.now().date()).days
                        elif hasattr(ed, "strftime"):
                            earnings_date = ed.strftime("%Y-%m-%d")
                elif isinstance(cal, pd.DataFrame) and "Earnings Date" in cal.index:
                    ed = cal.loc["Earnings Date"].iloc[0]
                    if hasattr(ed, "strftime"):
                        earnings_date = ed.strftime("%Y-%m-%d")
                        dte = ed if not hasattr(ed, "date") else ed.date()
                        days_to_earnings = (dte - datetime.now().date()).days
            except Exception:
                pass

            fundamentals.append({
                "ticker": ticker,
                "market_cap_b": round(info.get("marketCap", 0) / 1e9, 1),
                "pe_trailing": round(info.get("trailingPE", 0) or 0, 1),
                "pe_forward": round(info.get("forwardPE", 0) or 0, 1),
                "pb": round(info.get("priceToBook", 0) or 0, 2),
                "dividend_yield": round((info.get("dividendYield", 0) or 0), 2),
                "revenue_growth": round((info.get("revenueGrowth", 0) or 0) * 100, 1),
                "earnings_growth": round((info.get("earningsGrowth", 0) or 0) * 100, 1),
                "eps": round(info.get("trailingEps", 0) or 0, 2),
                "target_price": round(info.get("targetMeanPrice", 0) or 0, 2),
                "recommendation": info.get("recommendationKey", "N/A"),
                "short_name": info.get("shortName", ticker),
                "short_pct_of_float": round(info.get("shortPercentOfFloat", 0) or 0, 4),
                "short_ratio": round(info.get("shortRatio", 0) or 0, 2),
                "shares_short": shares_short,
                "shares_short_prior_month": shares_short_prior,
                "float_shares": info.get("floatShares"),
                "short_change_pct": short_change_pct,
                "earnings_date": earnings_date,
                "days_to_earnings": days_to_earnings,
            })
        except Exception:
            fundamentals.append({"ticker": ticker, "short_name": ticker, "error": True})

    return fundamentals


def compute_squeeze_score(ranking):
    """Compute short squeeze expectation score using percentile ranking."""
    df = pd.DataFrame(ranking)

    has_data = df["short_pct_of_float"].notna() & (df["short_pct_of_float"] > 0)

    if has_data.sum() < 3:
        df["squeeze_score"] = None
        return df

    df.loc[has_data, "sq_pct_rank"] = df.loc[has_data, "short_pct_of_float"].rank(pct=True)
    df.loc[has_data, "sq_dtc_rank"] = df.loc[has_data, "short_ratio"].rank(pct=True)

    chg_valid = has_data & df["short_change_pct"].notna()
    df.loc[chg_valid, "sq_chg_rank"] = df.loc[chg_valid, "short_change_pct"].rank(pct=True)
    df.loc[has_data & ~chg_valid, "sq_chg_rank"] = 0.5

    df["momentum_norm"] = df["momentum_score"] / 100.0

    df.loc[has_data, "squeeze_score"] = (
        df.loc[has_data, "sq_pct_rank"] * 0.40
        + df.loc[has_data, "sq_dtc_rank"] * 0.30
        + df.loc[has_data, "sq_chg_rank"] * 0.15
        + df.loc[has_data, "momentum_norm"] * 0.15
    ) * 100

    df.loc[~has_data, "squeeze_score"] = None

    return df


def compute_value_gap(all_results, fundamentals_list, is_japan=False):
    """Identify contrarian candidates: stocks sold off despite strong fundamentals.

    Criteria for candidates:
    - Target price > current price by 15%+ (analyst upside)
    - 1M return is negative (being sold off)
    - EPS growth or revenue growth is positive (fundamentals intact)

    Score = weighted percentile rank of:
    - Target gap %    (30%)
    - Forward PE low  (20%)
    - EPS growth      (20%)
    - Revenue growth  (15%)
    - Analyst rating  (15%)
    """
    fund_map = {f["ticker"]: f for f in fundamentals_list if not f.get("error")}

    candidates = []
    for r in all_results:
        ticker = r["ticker"]
        fund = fund_map.get(ticker, {})
        target = fund.get("target_price", 0) or 0
        price = r["price"]
        if not price or price <= 0 or not target or target <= 0:
            continue

        target_gap_pct = round((target - price) / price * 100, 1)
        ret_1m = r.get("ret_1m", 0) or 0
        eps_growth = fund.get("earnings_growth", 0) or 0
        rev_growth = fund.get("revenue_growth", 0) or 0
        pe_forward = fund.get("pe_forward", 0) or 0
        recommendation = fund.get("recommendation", "N/A")

        # Filter: must have upside, be declining, and have some growth
        if target_gap_pct < 15:
            continue
        if ret_1m >= 0:
            continue
        if eps_growth <= 0 and rev_growth <= 0:
            continue

        # Recommendation score
        rec_score_map = {"strong_buy": 5, "buy": 4, "hold": 3, "sell": 2, "strong_sell": 1}
        rec_num = rec_score_map.get(recommendation, 3)

        candidates.append({
            "ticker": ticker,
            "name": fund.get("short_name", ticker),
            "sector": r.get("sector", ""),
            "price": price,
            "target_price": target,
            "target_gap_pct": target_gap_pct,
            "ret_1m": ret_1m,
            "ret_3m": r.get("ret_3m", 0),
            "rsi": r.get("rsi", 50),
            "pe_forward": pe_forward,
            "eps_growth": eps_growth,
            "revenue_growth": rev_growth,
            "recommendation": recommendation,
            "rec_num": rec_num,
            "market_cap_b": fund.get("market_cap_b", 0),
            "pe_trailing": fund.get("pe_trailing", 0),
            "pb": fund.get("pb", 0),
            "dividend_yield": fund.get("dividend_yield", 0),
            "eps": fund.get("eps", 0),
            "ma50_dev": r.get("ma50_dev", 0),
            "ma200_dev": r.get("ma200_dev", 0),
        })

    if len(candidates) < 2:
        # Add score=0 and return what we have
        for c in candidates:
            c["value_gap_score"] = 50.0
        return candidates

    # Percentile ranking for score
    cdf = pd.DataFrame(candidates)
    cdf["s_gap"] = cdf["target_gap_pct"].rank(pct=True)
    # Lower forward PE = more undervalued (invert rank)
    cdf["s_pe"] = 1 - cdf["pe_forward"].rank(pct=True) if (cdf["pe_forward"] > 0).any() else 0.5
    cdf["s_eps"] = cdf["eps_growth"].rank(pct=True)
    cdf["s_rev"] = cdf["revenue_growth"].rank(pct=True)
    cdf["s_rec"] = cdf["rec_num"].rank(pct=True)

    cdf["value_gap_score"] = (
        cdf["s_gap"] * 0.30
        + cdf["s_pe"] * 0.20
        + cdf["s_eps"] * 0.20
        + cdf["s_rev"] * 0.15
        + cdf["s_rec"] * 0.15
    ) * 100

    cdf = cdf.sort_values("value_gap_score", ascending=False)

    result = []
    for rank_idx, (_, row) in enumerate(cdf.iterrows(), 1):
        result.append({
            "rank": rank_idx,
            "ticker": row["ticker"],
            "name": row["name"],
            "sector": row["sector"],
            "price": row["price"],
            "target_price": row["target_price"],
            "target_gap_pct": row["target_gap_pct"],
            "value_gap_score": round(row["value_gap_score"], 1),
            "ret_1m": row["ret_1m"],
            "ret_3m": row["ret_3m"],
            "rsi": row["rsi"],
            "pe_forward": row["pe_forward"],
            "pe_trailing": row["pe_trailing"],
            "pb": row["pb"],
            "eps_growth": row["eps_growth"],
            "revenue_growth": row["revenue_growth"],
            "recommendation": row["recommendation"],
            "market_cap_b": row["market_cap_b"],
            "dividend_yield": row["dividend_yield"],
            "eps": row["eps"],
            "ma50_dev": row["ma50_dev"],
            "ma200_dev": row["ma200_dev"],
        })

    return result


# ── Time Arbitrage ─────────────────────────────────────────────────────────────

def compute_time_arbitrage(results, name_map=None, is_japan=False, progress_cb=None):
    """Time arbitrage: one-time bad earnings + CapEx surge + price recovery potential.

    Pre-filter: ret_1m < -5% (sold off), ret_3m > -60% (not collapsed), RSI 25-65.
    Fetch cashflow + financials for up to 40 candidates in parallel.
    Score: CapEx growth (45%) + NI drop magnitude (35%) + RSI recovery (20%).
    """
    if name_map is None:
        name_map = {}

    candidates = [
        r for r in results
        if (r.get("ret_1m", 0) or 0) < -5
        and (r.get("ret_3m", 0) or 0) > -60
        and 25 < (r.get("rsi", 50) or 50) < 65
    ]
    candidates.sort(key=lambda r: r.get("ret_1m", 0) or 0)
    candidates = candidates[:40]

    if not candidates:
        return []

    divisor = 1e8 if is_japan else 1e9  # 億円 or $B

    def find_series(df, *keywords):
        for key in keywords:
            for idx in df.index:
                if key.lower() in str(idx).lower():
                    s = df.loc[idx].dropna()
                    if len(s) >= 2:
                        return s
        return pd.Series(dtype=float)

    def fetch_one(r):
        ticker = r["ticker"]
        try:
            t = yf.Ticker(ticker)
            cf = t.cashflow
            fin = t.financials

            if cf is None or cf.empty or fin is None or fin.empty:
                return None

            capex_s = find_series(cf, "capital expenditure")
            opcf_s = find_series(cf, "operating cash flow")
            ni_s = find_series(fin, "net income")
            rev_s = find_series(fin, "total revenue", "revenue")

            if len(capex_s) < 2 or len(ni_s) < 2:
                return None

            capex_latest = abs(float(capex_s.iloc[0]))
            capex_prior = abs(float(capex_s.iloc[1]))
            if capex_prior <= 0:
                return None

            capex_growth = (capex_latest / capex_prior - 1) * 100
            opcf_raw = float(opcf_s.iloc[0]) if not opcf_s.empty else 0
            ni_latest = float(ni_s.iloc[0])
            ni_prior = float(ni_s.iloc[1])
            if ni_prior == 0:
                return None

            ni_change = (ni_latest / abs(ni_prior) - 1) * 100

            rev_stable = True
            if len(rev_s) >= 2:
                rv, rp = float(rev_s.iloc[0]), float(rev_s.iloc[1])
                if rp != 0:
                    rev_stable = (rv / abs(rp) - 1) * 100 > -20

            # Criteria: CapEx surged ≥20%, NI dropped ≥15%, operating CF > 0, revenue stable
            if capex_growth < 20 or ni_change > -15 or opcf_raw <= 0 or not rev_stable:
                return None

            return {
                "ticker": ticker,
                "sector": r["sector"],
                "price": r["price"],
                "ret_1m": r["ret_1m"],
                "ret_3m": r["ret_3m"],
                "rsi": round(float(r["rsi"]), 1),
                "capex_growth": round(capex_growth, 1),
                "ni_change": round(ni_change, 1),
                "opcf_val": round(opcf_raw / divisor, 1),
            }
        except Exception:
            return None

    raw = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_one, r): r for r in candidates}
        for fut in as_completed(futs):
            res = fut.result()
            if res:
                raw.append(res)

    if not raw:
        return []

    if len(raw) < 2:
        raw[0]["rank"] = 1
        raw[0]["name"] = name_map.get(raw[0]["ticker"], raw[0]["ticker"])
        raw[0]["arb_score"] = 50.0
        return raw

    sdf = pd.DataFrame(raw)
    sdf["s_capex"] = sdf["capex_growth"].rank(pct=True)
    sdf["s_ni_drop"] = (-sdf["ni_change"]).rank(pct=True)
    sdf["s_rsi"] = 1 - (sdf["rsi"] - 47).abs().rank(pct=True)
    sdf["arb_score"] = (
        sdf["s_capex"] * 0.45 + sdf["s_ni_drop"] * 0.35 + sdf["s_rsi"] * 0.20
    ) * 100
    sdf = sdf.sort_values("arb_score", ascending=False)

    result = []
    for rank_idx, (_, row) in enumerate(sdf.iterrows(), 1):
        result.append({
            "rank": rank_idx,
            "ticker": row["ticker"],
            "name": name_map.get(row["ticker"], row["ticker"]),
            "sector": row["sector"],
            "price": row["price"],
            "ret_1m": row["ret_1m"],
            "ret_3m": row["ret_3m"],
            "rsi": row["rsi"],
            "capex_growth": row["capex_growth"],
            "ni_change": row["ni_change"],
            "opcf_val": row["opcf_val"],
            "arb_score": round(float(row["arb_score"]), 1),
        })
    return result[:15]


# ── Small-cap Momentum ─────────────────────────────────────────────────────────

def compute_smallcap_momentum(results, all_fundamentals, score_df, is_japan=False):
    """Small/mid-cap stocks with rising momentum.

    Uses market_cap_b from all_fundamentals (top_n + declining pool).
    Filter: Japan 100億-3000億円, US $0.5B-$15B; momentum_score ≥ 45.
    """
    fund_map = {f["ticker"]: f for f in all_fundamentals if f.get("market_cap_b")}

    score_map = {
        row["ticker"]: round(float(row["momentum_score"]) * 100, 1)
        for _, row in score_df.iterrows()
    }

    # cap_min / cap_max in units of market_cap_b (B in local currency)
    # Japan: 1B JPY = 10億円  →  100億円 = 10 B JPY, 3000億円 = 300 B JPY
    # US:    1B USD            →  $0.5B = 0.5, $15B = 15
    cap_min, cap_max = (10, 300) if is_japan else (0.5, 15)

    candidates = []
    for r in results:
        fund = fund_map.get(r["ticker"])
        if not fund:
            continue
        cap = fund.get("market_cap_b", 0) or 0
        if not (cap_min <= cap <= cap_max):
            continue
        score = score_map.get(r["ticker"], 0)
        if score < 45:
            continue
        candidates.append({
            "ticker": r["ticker"],
            "name": fund.get("short_name", r["ticker"]),
            "sector": r["sector"],
            "price": r["price"],
            "market_cap_b": cap,          # raw; frontend formats per market
            "ret_1m": r["ret_1m"],
            "ret_3m": r["ret_3m"],
            "rsi": r["rsi"],
            "momentum_score": score,
            "dividend_yield": fund.get("dividend_yield", 0),
        })

    candidates.sort(key=lambda x: x["momentum_score"], reverse=True)
    for i, c in enumerate(candidates[:15], 1):
        c["rank"] = i
    return candidates[:15]


def run_screening(index="sp500", top_n=20, progress_cb=None):
    """Run full screening pipeline. Returns dict with results."""
    if progress_cb:
        progress_cb("Fetching ticker list...", 2)

    is_japan = index in ("nikkei225", "growth250")

    jp_names = {}
    _edinet_key = os.environ.get("EDINETDB_API_KEY", "")
    if index == "nasdaq100":
        tickers, sectors = get_nasdaq100_tickers()
    elif index == "nikkei225":
        tickers, sectors, jp_names = get_nikkei225_tickers()
        # Enrich sectors from EDINET (parallel, 225 API calls)
        if _edinet_key:
            if progress_cb:
                progress_cb("EDINETセクター取得中...", 3)
            edinet_sectors = get_edinet_sectors(tickers, _edinet_key, progress_cb)
            sectors.update(edinet_sectors)  # overwrite "N/A" with real industry
    elif index == "growth250":
        tickers, sectors, jp_names = get_growth250_tickers()
    else:
        tickers, sectors = get_sp500_tickers()

    if progress_cb:
        progress_cb(f"Found {len(tickers)} tickers", 8)

    results, price_data = screen_momentum(tickers, sectors, progress_cb, is_japan=is_japan)

    # Compute market breadth (ADL) from raw price data
    breadth_data = compute_breadth(price_data, tickers)

    if progress_cb:
        progress_cb("Computing scores...", 68)

    df = compute_momentum_score(results)
    top = df.head(top_n)

    top_tickers = top["ticker"].tolist()
    fundamentals = get_fundamentals(top_tickers, progress_cb, is_japan=is_japan)

    # For Nikkei 225: enrich fundamentals with EDINET annual data
    if is_japan and _edinet_key:
        if progress_cb:
            progress_cb("EDINETファンダメンタルズ取得中...", 70)
        # Build price_map from screening results for P/B and dividend yield calc
        price_map = {r["ticker"]: r["price"] for r in results if "ticker" in r}
        edinet_funds = get_edinet_fundamentals(top_tickers, _edinet_key, price_map, progress_cb)
        for f in fundamentals:
            ef = edinet_funds.get(f["ticker"], {})
            # Fill only missing / zero values from yfinance
            if not f.get("pe_trailing") and ef.get("per"):
                f["pe_trailing"] = round(float(ef["per"]), 1)
            if not f.get("pb") and ef.get("pb"):
                f["pb"] = ef["pb"]
            if not f.get("eps") and ef.get("eps"):
                f["eps"] = round(float(ef["eps"]), 2)
            if not f.get("dividend_yield") and ef.get("dividend_yield"):
                f["dividend_yield"] = ef["dividend_yield"]
            # EDINET-only fields
            f["roe"] = ef.get("roe")
            f["revenue_b"] = ef.get("revenue_b")
            f["net_income_b"] = ef.get("net_income_b")

    if progress_cb:
        progress_cb("Building report...", 97)

    fund_map = {f["ticker"]: f for f in fundamentals}
    ranking = []

    for rank_idx, (_, row) in enumerate(top.iterrows(), 1):
        ticker = row["ticker"]
        fund = fund_map.get(ticker, {})
        # Use Japanese name from Wikipedia for Nikkei 225, fallback to yfinance shortName
        display_name = jp_names.get(ticker) or fund.get("short_name", ticker)
        ranking.append({
            "rank": rank_idx,
            "ticker": ticker,
            "name": display_name,
            "sector": row["sector"],
            "price": row["price"],
            "momentum_score": round(row["momentum_score"] * 100, 1),
            "score_components": scoring_service.extract_score_components(row),
            "technicals": {
                "ret_1d": row["ret_1d"],
                "ret_1w": row["ret_1w"],
                "ret_1m": row["ret_1m"],
                "ret_3m": row["ret_3m"],
                "vol_ratio": row["vol_ratio"],
                "ma50_dev": row["ma50_dev"],
                "ma200_dev": row["ma200_dev"],
                "macd_hist_pct": row["macd_hist"],
                "rsi": row["rsi"],
                "sector_etf": row.get("sector_etf", "SPY"),
                "rs_1m": row.get("rs_1m", 0),
                "rs_3m": row.get("rs_3m", 0),
                "rs_label": row.get("rs_label", ""),
                "high_52w": row.get("high_52w"),
                "low_52w": row.get("low_52w"),
                "dist_from_high": row.get("dist_from_high"),
                "dist_from_low": row.get("dist_from_low"),
                "is_breakout": bool(row.get("is_breakout", False)),
                "bb_width": row.get("bb_width"),
                "bb_squeeze": bool(row.get("bb_squeeze", False)),
                "golden_cross": row["golden_cross"],
                "overheat": bool(row["overheat"]),
            },
            "fundamentals": {
                "market_cap_b": fund.get("market_cap_b"),
                "pe_trailing": fund.get("pe_trailing"),
                "pe_forward": fund.get("pe_forward"),
                "pb": fund.get("pb"),
                "dividend_yield": fund.get("dividend_yield"),
                "revenue_growth": fund.get("revenue_growth"),
                "earnings_growth": fund.get("earnings_growth"),
                "eps": fund.get("eps"),
                "target_price": fund.get("target_price"),
                "recommendation": fund.get("recommendation"),
                "earnings_date": fund.get("earnings_date"),
                "days_to_earnings": fund.get("days_to_earnings"),
                # EDINET-only (Nikkei 225)
                "roe": fund.get("roe"),
                "revenue_b": fund.get("revenue_b"),
                "net_income_b": fund.get("net_income_b"),
            },
            "short_interest": {
                "short_pct_of_float": fund.get("short_pct_of_float"),
                "short_ratio": fund.get("short_ratio"),
                "shares_short": fund.get("shares_short"),
                "shares_short_prior_month": fund.get("shares_short_prior_month"),
                "float_shares": fund.get("float_shares"),
                "short_change_pct": fund.get("short_change_pct"),
            },
            "squeeze_score": None,
        })

    # Compute value gap (contrarian) candidates
    # Get fundamentals for declining stocks (1M return < 0) that aren't already in top_tickers
    declining_tickers = [r["ticker"] for r in results if (r.get("ret_1m", 0) or 0) < 0 and r["ticker"] not in top_tickers]
    # Limit to avoid excessive API calls — take worst performers first
    declining_tickers.sort(key=lambda t: next((r["ret_1m"] for r in results if r["ticker"] == t), 0))
    declining_tickers = declining_tickers[:50]

    if progress_cb:
        progress_cb("Fetching contrarian fundamentals...", 92)

    declining_fund = get_fundamentals(declining_tickers, is_japan=is_japan)
    all_fund_for_gap = fundamentals + declining_fund
    value_gap_ranking = compute_value_gap(results, all_fund_for_gap, is_japan=is_japan)

    # Time arbitrage — build name map first
    name_map = {}
    if is_japan:
        name_map = {r["ticker"]: jp_names.get(r["ticker"], r["ticker"]) for r in results}
    for f in fundamentals + declining_fund:
        t = f.get("ticker")
        if t and not name_map.get(t):
            name_map[t] = f.get("short_name", t)

    if progress_cb:
        progress_cb("タイム裁定候補を計算中...", 93)
    time_arb_ranking = compute_time_arbitrage(results, name_map=name_map, is_japan=is_japan)

    # Small-cap momentum
    smallcap_ranking = compute_smallcap_momentum(results, fundamentals + declining_fund, df, is_japan=is_japan)

    # Compute squeeze scores
    sq_df = compute_squeeze_score(
        [{"short_pct_of_float": r["short_interest"]["short_pct_of_float"],
          "short_ratio": r["short_interest"]["short_ratio"],
          "short_change_pct": r["short_interest"]["short_change_pct"],
          "momentum_score": r["momentum_score"]} for r in ranking]
    )
    for i, row in sq_df.iterrows():
        val = row.get("squeeze_score")
        ranking[i]["squeeze_score"] = round(float(val), 1) if pd.notna(val) else None

    # ── Sprint 3: Quality scores (finalize with real momentum_score + earnings) ─
    _result_map = {r["ticker"]: r for r in results}
    for item in ranking:
        ticker = item["ticker"]
        r = _result_map.get(ticker, {})
        fund = fund_map.get(ticker, {})
        base_comps = r.get("_quality_components", {})
        q = quality_service.finalize_quality(
            base_components=base_comps,
            momentum_score=item["momentum_score"],
            ma50_dev=r.get("ma50_dev", 0),
            ma25_dev=r.get("ma25_dev", 0),
            bb_width=r.get("bb_width", 8),
            days_to_earnings=fund.get("days_to_earnings"),
            rsi=r.get("rsi", 50),
        )
        item["quality_score"] = q["quality_score"]
        item["quality_components"] = q["quality_components"]
        item["entry_difficulty"] = q["entry_difficulty"]
        item["technicals"]["ma25_dev"] = r.get("ma25_dev")

    # ── Sprint 1: Tags & Questions ────────────────────────────────────────────
    for item in ranking:
        t = item.get("technicals", {})
        f = item.get("fundamentals", {})
        si = item.get("short_interest", {})
        flat = {
            "ticker": item["ticker"],
            "momentum_score": item["momentum_score"],
            "price": item["price"],
            "ret_1m": t.get("ret_1m"),
            "ret_3m": t.get("ret_3m"),
            "vol_ratio": t.get("vol_ratio"),
            "rsi": t.get("rsi"),
            "dist_from_high": t.get("dist_from_high"),
            "bb_squeeze": t.get("bb_squeeze"),
            "is_breakout": t.get("is_breakout"),
            "rs_label": t.get("rs_label"),
            "high_52w": t.get("high_52w"),
            "days_to_earnings": f.get("days_to_earnings"),
            "short_ratio": si.get("short_ratio"),
            "short_pct_of_float": si.get("short_pct_of_float"),
        }
        tags = tagging_service.assign_tags(flat)
        item["tags"] = tags
        item["questions"] = questions_service.generate_questions(flat, tags)

    # Breakout candidates (within 5% of 52W high or BB squeeze)
    breakout_candidates = [r for r in results if r.get("dist_from_high") is not None and (r["dist_from_high"] >= -5 or r.get("bb_squeeze"))]
    breakout_candidates.sort(key=lambda r: r.get("dist_from_high", -999), reverse=True)
    breakout_ranking = []
    for rank_idx, r in enumerate(breakout_candidates[:30], 1):
        display_name = jp_names.get(r["ticker"], r["ticker"]) if is_japan else r["ticker"]
        status = []
        if r.get("is_breakout"):
            status.append("新高値")
        if r.get("bb_squeeze"):
            status.append("BB圧縮")
        if r.get("golden_cross"):
            status.append("GC")
        breakout_ranking.append({
            "rank": rank_idx,
            "ticker": r["ticker"],
            "name": display_name,
            "sector": r["sector"],
            "price": r["price"],
            "high_52w": r["high_52w"],
            "low_52w": r["low_52w"],
            "dist_from_high": r["dist_from_high"],
            "dist_from_low": r["dist_from_low"],
            "is_breakout": r["is_breakout"],
            "bb_width": r["bb_width"],
            "bb_squeeze": r["bb_squeeze"],
            "momentum_score": round(float(df.loc[df["ticker"] == r["ticker"], "momentum_score"].iloc[0]) * 100, 1) if r["ticker"] in df["ticker"].values else 0,
            "rsi": r["rsi"],
            "status": " / ".join(status) if status else "-",
        })

    # Sector rotation analysis
    # Need benchmark returns — re-use the same data already fetched during screen_momentum
    sectors_used = set(r["sector"] for r in results if r.get("sector"))
    benchmark_rets = _get_benchmark_returns(sectors_used, start_date=datetime.now() - timedelta(days=400), end_date=datetime.now(), is_japan=is_japan)
    sector_rotation = compute_sector_rotation(results, benchmark_rets, is_japan=is_japan)

    # Sector summary from all screened results
    sector_counts = {}
    for r in results:
        s = r["sector"]
        sector_counts[s] = sector_counts.get(s, 0) + 1

    # Top sector distribution
    top_sectors = {}
    for item in ranking:
        s = item["sector"]
        top_sectors[s] = top_sectors.get(s, 0) + 1

    if progress_cb:
        progress_cb("Complete!", 100)

    # Latest breadth snapshot for summary card
    latest_breadth = breadth_data[-1] if breadth_data else {"advances": 0, "declines": 0, "breadth_pct": 0}

    # ── Sprint 2: Market Regime Classification ────────────────────────────────
    momentum_scores_list = [r["momentum_score"] for r in ranking if r.get("momentum_score") is not None]
    regime = regime_service.classify(
        breadth_pct=latest_breadth.get("breadth_pct", 0),
        adl_data=breadth_data,
        momentum_scores=momentum_scores_list,
        sector_rotation=sector_rotation,
    )

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "index": {"sp500": "S&P 500", "nasdaq100": "NASDAQ 100", "nikkei225": "日経225", "growth250": "グロース250"}.get(index, index.upper()),
        "total_screened": len(results),
        "total_tickers": len(tickers),
        "top_n": top_n,
        "summary": {
            "avg_score": round(sum(r["momentum_score"] for r in ranking) / len(ranking), 1) if ranking else 0,
            "overheat_count": sum(1 for r in ranking if r["technicals"]["overheat"]),
            "golden_cross_count": sum(1 for r in ranking if r["technicals"]["golden_cross"]),
        },
        "sector_distribution": top_sectors,
        "all_sectors": sector_counts,
        "momentum_ranking": ranking,
        "value_gap_ranking": value_gap_ranking[:20],
        "sector_rotation": sector_rotation,
        "breakout_ranking": breakout_ranking,
        "time_arb_ranking": time_arb_ranking,
        "smallcap_ranking": smallcap_ranking,
        "breadth": breadth_data,
        "latest_breadth": latest_breadth,
        "regime": regime,
    }
