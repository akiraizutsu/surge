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
    """Fetch Nikkei 225 constituent tickers from Wikipedia (Japanese)."""
    url = "https://ja.wikipedia.org/wiki/日経平均株価"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    tickers = []
    for table in tables:
        cols = [str(c) for c in table.columns]
        if "証券コード" in cols and "銘柄" in cols:
            for _, row in table.iterrows():
                code = str(row["証券コード"]).strip()
                if code.isdigit():
                    tickers.append(code + ".T")
    if not tickers:
        raise ValueError("Could not find Nikkei 225 ticker table")
    # All Nikkei 225 stocks use ^N225 as benchmark (no sector ETF mapping)
    sectors = {t: "N/A" for t in tickers}
    return tickers, sectors


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
            })
        except Exception:
            continue

    return results


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


def run_screening(index="sp500", top_n=20, progress_cb=None):
    """Run full screening pipeline. Returns dict with results."""
    if progress_cb:
        progress_cb("Fetching ticker list...", 2)

    is_japan = index == "nikkei225"

    if index == "nasdaq100":
        tickers, sectors = get_nasdaq100_tickers()
    elif index == "nikkei225":
        tickers, sectors = get_nikkei225_tickers()
    else:
        tickers, sectors = get_sp500_tickers()

    if progress_cb:
        progress_cb(f"Found {len(tickers)} tickers", 5)

    results = screen_momentum(tickers, sectors, progress_cb, is_japan=is_japan)

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
        ranking.append({
            "rank": rank_idx,
            "ticker": ticker,
            "name": fund.get("short_name", ticker),
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
    }
