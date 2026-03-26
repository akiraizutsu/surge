"""Momentum screening engine for S&P 500, NASDAQ 100, and Nikkei 225."""

import io
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings("ignore")


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

            ma_50 = close.iloc[-50:].mean() if len(close) >= 50 else current_price
            ma_200 = close.iloc[-200:].mean() if len(close) >= 200 else current_price
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

            results.append({
                "ticker": ticker,
                "sector": stock_sector,
                "price": round(float(current_price), 2),
                "ret_1d": round(float(ret_1d), 2),
                "ret_1w": round(float(ret_1w), 2),
                "ret_1m": round(float(ret_1m), 2),
                "ret_3m": round(float(ret_3m), 2),
                "vol_ratio": round(float(vol_ratio), 2),
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


def run_screening(index="sp500", top_n=20, progress_cb=None):
    """Run full screening pipeline. Returns dict with results."""
    if progress_cb:
        progress_cb("Fetching ticker list...", 2)

    is_japan = index == "nikkei225"

    jp_names = {}
    if index == "nasdaq100":
        tickers, sectors = get_nasdaq100_tickers()
    elif index == "nikkei225":
        tickers, sectors, jp_names = get_nikkei225_tickers()
    else:
        tickers, sectors = get_sp500_tickers()

    if progress_cb:
        progress_cb(f"Found {len(tickers)} tickers", 5)

    results, price_data = screen_momentum(tickers, sectors, progress_cb, is_japan=is_japan)

    # Compute market breadth (ADL) from raw price data
    breadth_data = compute_breadth(price_data, tickers)

    if progress_cb:
        progress_cb("Computing scores...", 68)

    df = compute_momentum_score(results)
    top = df.head(top_n)

    top_tickers = top["ticker"].tolist()
    fundamentals = get_fundamentals(top_tickers, progress_cb, is_japan=is_japan)

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

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "index": {"sp500": "S&P 500", "nasdaq100": "NASDAQ 100", "nikkei225": "日経225"}.get(index, index.upper()),
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
        "breadth": breadth_data,
        "latest_breadth": latest_breadth,
    }
