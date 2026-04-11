"""Index constituent fetchers for S&P 500 / NASDAQ 100 / Nikkei 225 / Growth 250.

Each function scrapes the corresponding Wikipedia page and returns the
tickers plus a sector mapping. Japanese indices additionally return a
name map (ticker -> Japanese name).

Extracted from screener.py so that the main screening engine can focus
on orchestration and so that ticker sources can be mocked in tests.
"""

import io
import re

import pandas as pd
import requests


# Valid TSE security code: 4 characters, digits or uppercase letters, must
# contain at least one digit. Accepts both legacy numeric codes like "7203"
# and the new alphanumeric format like "285A" (e.g. キオクシアホールディングス).
_TSE_CODE_RE = re.compile(r"^(?=.*\d)[0-9A-Z]{4}$")


def _is_valid_tse_code(code: str) -> bool:
    return bool(_TSE_CODE_RE.match(code))


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

    Accepts both legacy numeric codes (e.g. ``7203``) and the new TSE
    alphanumeric format (e.g. ``285A`` for キオクシアホールディングス).
    Deduplicates across historical sub-tables on the page.

    Returns (tickers, sectors, names) where names maps ticker -> Japanese name.
    """
    url = "https://ja.wikipedia.org/wiki/日経平均株価"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    tickers = []
    seen = set()
    names = {}
    for table in tables:
        cols = [str(c) for c in table.columns]
        if "証券コード" in cols and "銘柄" in cols:
            for _, row in table.iterrows():
                code = str(row["証券コード"]).strip().upper()
                if _is_valid_tse_code(code):
                    t = code + ".T"
                    if t in seen:
                        continue
                    seen.add(t)
                    tickers.append(t)
                    names[t] = str(row["銘柄"]).strip()
    if not tickers:
        raise ValueError("Could not find Nikkei 225 ticker table")
    # All Nikkei 225 stocks use ^N225 as benchmark (no sector ETF mapping)
    sectors = {t: "N/A" for t in tickers}
    return tickers, sectors, names


def get_growth250_tickers():
    """Fetch TSE Growth Market 250 constituent tickers from Wikipedia.

    Accepts both legacy numeric codes and alphanumeric TSE codes.

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
                code = str(row["コード"]).strip().upper()
                if _is_valid_tse_code(code):
                    t = code + ".T"
                    tickers.append(t)
                    names[t] = str(row["銘柄名"]).strip()
            break
    if not tickers:
        raise ValueError("Could not find Growth 250 ticker table on Wikipedia")
    sectors = {t: "N/A" for t in tickers}
    return tickers, sectors, names
