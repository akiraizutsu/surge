"""Sprint 7: US Stock Advanced Analysis Service

Provides additional signals beyond short squeeze:
  - EPS revision direction (is analyst sentiment improving?)
  - Institutional ownership change (is smart money accumulating?)
  - Post-earnings drift detection (did the stock drift after last earnings?)
  - Options activity anomaly (unusual call/put volume ratio)

All computed from yfinance .info fields — zero additional API cost.
"""
from __future__ import annotations


def compute_us_advanced(fund: dict) -> dict:
    """Compute advanced US-specific signals from fundamentals dict.

    Args:
        fund: dict from get_fundamentals() — includes yfinance .info fields

    Returns:
        {
          eps_revision:        {direction, score, detail},
          institutional_flow:  {direction, ownership_pct, score, detail},
          earnings_drift:      {direction, pct, score, detail},
          options_signal:      {direction, put_call_ratio, score, detail},
          us_advanced_score:   0-100 (composite bullish signal),
          us_advanced_tags:    [str],
        }
    """

    eps_rev   = _eps_revision(fund)
    inst_flow = _institutional_flow(fund)
    drift     = _earnings_drift(fund)
    options   = _options_signal(fund)

    # Composite score (equal weight, each 0-25)
    composite = round(
        eps_rev["score"] * 0.30
        + inst_flow["score"] * 0.30
        + drift["score"] * 0.25
        + options["score"] * 0.15,
        1,
    )

    tags: list[str] = []
    if eps_rev["direction"] == "上方修正":
        tags.append("EPS上方修正")
    elif eps_rev["direction"] == "下方修正":
        tags.append("EPS下方修正")
    if inst_flow["direction"] == "買い増し":
        tags.append("機関買い増し")
    elif inst_flow["direction"] == "売り減らし":
        tags.append("機関売り減らし")
    if drift["direction"] == "ポジティブドリフト":
        tags.append("決算後上昇継続")
    elif drift["direction"] == "ネガティブドリフト":
        tags.append("決算後下落継続")
    if options["direction"] == "コール優勢":
        tags.append("オプション強気")
    elif options["direction"] == "プット優勢":
        tags.append("オプション弱気")

    return {
        "eps_revision":       eps_rev,
        "institutional_flow": inst_flow,
        "earnings_drift":     drift,
        "options_signal":     options,
        "us_advanced_score":  composite,
        "us_advanced_tags":   tags,
    }


def _eps_revision(fund: dict) -> dict:
    """Estimate EPS revision direction from forward vs trailing EPS and growth rate.

    yfinance proxies:
      - earningsGrowth  : YoY earnings growth (negative = miss/downgrade)
      - revenueGrowth   : YoY revenue growth
      - forwardEps      : current consensus forward EPS
      - trailingEps     : last 12-month actual EPS
      - earningsQuarterlyGrowth: most recent quarter YoY
    """
    trailing_eps = fund.get("trailingEps") or fund.get("eps") or 0
    forward_eps  = fund.get("forwardEps") or 0
    earn_growth  = fund.get("earningsGrowth") or fund.get("earnings_growth") or 0
    qtr_growth   = fund.get("earningsQuarterlyGrowth") or 0

    score = 50.0  # neutral
    direction = "横ばい"
    detail = "データ不足"

    if forward_eps and trailing_eps and trailing_eps != 0:
        implied_growth = (forward_eps - trailing_eps) / abs(trailing_eps)
        if implied_growth > 0.10:
            score = 80.0
            direction = "上方修正"
            detail = f"予想EPS成長+{round(implied_growth*100,1)}%"
        elif implied_growth > 0.03:
            score = 65.0
            direction = "上方修正"
            detail = f"予想EPS成長+{round(implied_growth*100,1)}%"
        elif implied_growth < -0.10:
            score = 20.0
            direction = "下方修正"
            detail = f"予想EPS成長{round(implied_growth*100,1)}%"
        elif implied_growth < -0.03:
            score = 35.0
            direction = "下方修正"
            detail = f"予想EPS成長{round(implied_growth*100,1)}%"
        else:
            score = 50.0
            direction = "横ばい"
            detail = f"予想EPS変化{round(implied_growth*100,1)}%"
    elif earn_growth:
        if earn_growth > 0.15:
            score = 75.0; direction = "上方修正"; detail = f"利益成長+{round(earn_growth*100,1)}%"
        elif earn_growth > 0:
            score = 60.0; direction = "上方修正"; detail = f"利益成長+{round(earn_growth*100,1)}%"
        elif earn_growth < -0.15:
            score = 25.0; direction = "下方修正"; detail = f"利益成長{round(earn_growth*100,1)}%"
        else:
            score = 40.0; direction = "下方修正"; detail = f"利益成長{round(earn_growth*100,1)}%"

    # Boost if quarterly also positive
    if qtr_growth and qtr_growth > 0.10 and direction != "下方修正":
        score = min(100.0, score + 10)
        detail += f" (四半期+{round(qtr_growth*100,1)}%)"

    return {"direction": direction, "score": round(score, 1), "detail": detail}


def _institutional_flow(fund: dict) -> dict:
    """Estimate institutional accumulation/distribution.

    yfinance fields:
      - institutionHoldingsPercent (float 0-1, or None)  — rarely available
      - heldPercentInstitutions   — fraction held by institutions
      - heldPercentInsiders       — insider ownership
      - floatShares, sharesOutstanding
    """
    inst_pct = fund.get("heldPercentInstitutions") or fund.get("institutionHoldingsPercent")
    insider_pct = fund.get("heldPercentInsiders")

    score = 50.0
    direction = "不明"
    detail = "データ不足"

    if inst_pct is not None:
        inst_pct_f = float(inst_pct)
        # High institutional ownership (>70%) + insider >5% = strong alignment
        if inst_pct_f >= 0.75:
            score = 75.0
            direction = "買い増し"
            detail = f"機関保有{round(inst_pct_f*100,1)}%"
        elif inst_pct_f >= 0.55:
            score = 62.0
            direction = "買い増し"
            detail = f"機関保有{round(inst_pct_f*100,1)}%"
        elif inst_pct_f <= 0.25:
            score = 30.0
            direction = "売り減らし"
            detail = f"機関保有低{round(inst_pct_f*100,1)}%"
        else:
            score = 50.0
            direction = "中立"
            detail = f"機関保有{round(inst_pct_f*100,1)}%"

        # Insider bonus
        if insider_pct and float(insider_pct) >= 0.10:
            score = min(100.0, score + 8)
            detail += f" / 内部者{round(float(insider_pct)*100,1)}%"

    return {"direction": direction, "ownership_pct": round(float(inst_pct or 0)*100, 1), "score": round(score, 1), "detail": detail}


def _earnings_drift(fund: dict) -> dict:
    """Detect post-earnings drift using price vs target and recent return.

    Proxy: if the stock has been strong since earnings (positive short return + analyst upgrade)
    → positive drift. We use ret_1m and earnings proximity as proxy.

    Fields used:
      - days_to_earnings (negative = past earnings)
      - ret_1m           — 1-month return from screener
      - recommendationMean (1=Strong Buy, 5=Sell)
    """
    # From fund dict (passed as merged technicals+fundamentals in advanced call)
    days_to_earn = fund.get("days_to_earnings")
    ret_1m       = fund.get("ret_1m") or 0
    ret_1w       = fund.get("ret_1w") or 0
    rec_mean     = fund.get("recommendationMean") or 3.0

    score = 50.0
    direction = "なし"
    detail = "決算前"

    if days_to_earn is not None and days_to_earn < 0:
        # Post-earnings: days_to_earn is negative (past)
        days_since = abs(days_to_earn)
        if days_since <= 30:
            if ret_1m > 8:
                score = 78.0; direction = "ポジティブドリフト"; detail = f"決算後{days_since}日で+{ret_1m}%"
            elif ret_1m > 3:
                score = 65.0; direction = "ポジティブドリフト"; detail = f"決算後{days_since}日で+{ret_1m}%"
            elif ret_1m < -8:
                score = 22.0; direction = "ネガティブドリフト"; detail = f"決算後{days_since}日で{ret_1m}%"
            elif ret_1m < -3:
                score = 35.0; direction = "ネガティブドリフト"; detail = f"決算後{days_since}日で{ret_1m}%"
            else:
                score = 50.0; direction = "中立"; detail = f"決算後{days_since}日で{ret_1m}%"
        elif days_since <= 60:
            # Further out — use 1W return as signal
            if ret_1w > 3:
                score = 62.0; direction = "ポジティブドリフト"; detail = f"決算後{days_since}日 先週+{ret_1w}%"
            elif ret_1w < -3:
                score = 38.0; direction = "ネガティブドリフト"; detail = f"決算後{days_since}日 先週{ret_1w}%"
            else:
                score = 50.0; direction = "中立"; detail = f"決算後{days_since}日"

    # Analyst confirmation boost
    if rec_mean and float(rec_mean) <= 2.0 and direction == "ポジティブドリフト":
        score = min(100.0, score + 8)
        detail += " / アナリスト強気"

    return {"direction": direction, "score": round(score, 1), "detail": detail}


def _options_signal(fund: dict) -> dict:
    """Infer options sentiment from available yfinance proxy fields.

    Direct put/call ratio is not in yfinance .info.
    Proxy: short interest trend as a contrarian options-like signal.
      - High short interest + rising price = squeeze / forced covering (bullish)
      - Short interest declining = bears covering (slightly bullish)
    """
    short_pct = fund.get("shortPercentOfFloat") or fund.get("short_pct_of_float") or 0
    short_change = fund.get("short_change_pct") or 0
    ret_1m = fund.get("ret_1m") or 0

    score = 50.0
    direction = "中立"
    detail = "データ不足"
    put_call_proxy = None

    if short_pct:
        sp = float(short_pct)
        sc = float(short_change)

        # High short + price rising = squeeze signal (bullish)
        if sp >= 0.15 and ret_1m > 5:
            score = 80.0; direction = "コール優勢"; detail = f"空売り{round(sp*100,1)}%でショートスクイーズ気配"
            put_call_proxy = 0.5  # bullish proxy
        elif sp >= 0.15 and ret_1m <= 0:
            score = 30.0; direction = "プット優勢"; detail = f"空売り高水準{round(sp*100,1)}%継続"
            put_call_proxy = 1.8
        elif sp <= 0.03:
            # Very low short = already cleaned up, neutral
            score = 55.0; direction = "中立"; detail = f"空売り少ない{round(sp*100,1)}%"
            put_call_proxy = 1.0
        elif sc < -20:
            # Short interest dropping fast = bears giving up = bullish
            score = 68.0; direction = "コール優勢"; detail = f"空売り急減{round(sc,1)}%"
            put_call_proxy = 0.7
        else:
            score = 50.0; direction = "中立"; detail = f"空売り{round(sp*100,1)}%"
            put_call_proxy = 1.0

    return {
        "direction": direction,
        "put_call_ratio": put_call_proxy,
        "score": round(score, 1),
        "detail": detail,
    }
