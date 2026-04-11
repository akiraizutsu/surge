"""EDINET / J-Quants Cash Flow analysis for Japanese stocks.

Wraps EDINET DB search + financials endpoints and J-Quants quarterly
summary, and builds a flat payload that the `/api/cf_analysis/<ticker>`
Flask route can return directly.

Extracted from app.py to keep route handlers thin and to make the
CF pipeline testable in isolation.
"""

import os

import requests


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


def fetch_quarterly_jquants(sec_code):
    """Fetch quarterly CF from J-Quants /v2/fins/summary.

    Returns list of {period, operating_cf, investing_cf, financing_cf, fcf} in 億円,
    incremental (not cumulative). Empty list if API key missing or error.
    """
    api_key = os.environ.get("JQUANTS_API_KEY", "")
    if not api_key:
        return []
    try:
        code5 = sec_code + "0" if len(sec_code) == 4 else sec_code
        r = requests.get(
            "https://api.jquants.com/v2/fins/summary",
            headers={"x-api-key": api_key},
            params={"code": code5},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        rows = r.json().get("data") or []

        # Group by fiscal year, sort by period type order
        _order = {"1Q": 1, "2Q": 2, "3Q": 3, "FY": 4}
        rows = [row for row in rows if row.get("CFO") not in (None, "")]
        rows.sort(key=lambda x: (x.get("CurFYSt", ""), _order.get(x.get("CurPerType", ""), 9)))

        result = []
        prev_fy, prev_cfo, prev_cfi, prev_cff = None, 0, 0, 0
        for row in rows:
            fy_st = row.get("CurFYSt", "")[:7]  # "2024-04"
            ptype = row.get("CurPerType", "")
            if ptype not in ("1Q", "2Q", "3Q", "FY"):
                continue

            cfo_cum = float(row.get("CFO") or 0)
            cfi_cum = float(row.get("CFI") or 0)
            cff_cum = float(row.get("CFF") or 0)

            if fy_st != prev_fy:
                prev_fy, prev_cfo, prev_cfi, prev_cff = fy_st, 0, 0, 0

            cfo = cfo_cum - prev_cfo
            cfi = cfi_cum - prev_cfi
            cff = cff_cum - prev_cff
            prev_cfo, prev_cfi, prev_cff = cfo_cum, cfi_cum, cff_cum

            fy_label = fy_st[:4] + "/" + fy_st[5:7]
            period_label = f"{fy_label} {ptype}"
            result.append({
                "period":       period_label,
                "operating_cf": round(cfo / _UNIT_DIV, 1),
                "investing_cf": round(cfi / _UNIT_DIV, 1),
                "financing_cf": round(cff / _UNIT_DIV, 1),
                "fcf":          round(cfo / _UNIT_DIV, 1),
            })

        return result[-12:]  # 直近12四半期（3年分）
    except Exception:
        return []


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


def build_cf_payload(ticker):
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

    # 3. Quarterly CF from J-Quants (replaces empty EDINET quarterly)
    quarterly = fetch_quarterly_jquants(sec_code)

    # 4. Build timeline
    timeline = build_timeline(annual_rows)
    if not timeline:
        return None

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
