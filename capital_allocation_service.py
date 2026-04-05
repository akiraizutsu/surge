"""Sprint 5: 資本配分スコア (Capital Allocation Score).

Evaluates the *quality of how a company deploys its capital*.
Each of 8 sub-dimensions is scored 0-5 (integer), then normalised to 0-100.

Sub-dimensions:
  1. ocf_stability        — Operating CF consistency / strength
  2. capex_consistency    — CapEx discipline (not erratic)
  3. fcf_quality          — FCF generation ability
  4. net_cash_strength    — Balance sheet cushion
  5. dilution_risk        — Share issuance risk (inverted)
  6. debt_tolerance       — Debt sustainability
  7. shareholder_return   — Dividend / buyback track record
  8. mna_capacity         — M&A headroom (surplus cash, low debt)

All inputs come from yfinance Ticker.info fields.
Missing data is gracefully handled (defaults to neutral score).
"""

from __future__ import annotations

import math
from typing import Optional


def _safe(val, default=0.0) -> float:
    if val is None:
        return default
    try:
        v = float(val)
        return default if math.isnan(v) or math.isinf(v) else v
    except (TypeError, ValueError):
        return default


def _clamp(val: float, lo=0.0, hi=5.0) -> float:
    return max(lo, min(hi, val))


def _score5(val: float, thresholds: list[float]) -> int:
    """Convert a value to a 0-5 score given ascending threshold breakpoints.

    thresholds: [t1, t2, t3, t4] → scores 1,2,3,4,5 if val ≥ t (highest wins)
    val < t1 → 0
    """
    score = 0
    for t in thresholds:
        if val >= t:
            score += 1
    return min(score, 5)


def compute_capital_allocation(fund: dict) -> dict:
    """Compute capital allocation score from yfinance fundamentals.

    Args:
        fund: dict from get_fundamentals() or yf.Ticker().info
              Keys: operatingCashflow, freeCashflow, capitalExpenditures,
                    totalRevenue, totalCash, totalDebt, sharesOutstanding,
                    floatShares, dividendYield, payoutRatio,
                    revenueGrowth, earningsGrowth, marketCap

    Returns:
        {
          capital_score: float (0-100),
          capital_components: {
            ocf_stability, capex_consistency, fcf_quality,
            net_cash_strength, dilution_risk, debt_tolerance,
            shareholder_return, mna_capacity
          },   # each 0-5
          capital_grade: str  ('A' | 'B' | 'C' | 'D' | 'F')
        }
    """
    ocf          = _safe(fund.get("operatingCashflow") or fund.get("operating_cashflow"))
    fcf          = _safe(fund.get("freeCashflow") or fund.get("free_cashflow"))
    capex_raw    = _safe(fund.get("capitalExpenditures") or fund.get("capital_expenditures"))
    total_rev    = _safe(fund.get("totalRevenue") or fund.get("total_revenue"), 1)
    total_cash   = _safe(fund.get("totalCash") or fund.get("total_cash"))
    total_debt   = _safe(fund.get("totalDebt") or fund.get("total_debt"))
    shares_out   = _safe(fund.get("sharesOutstanding") or fund.get("shares_outstanding"), 1)
    float_shares = _safe(fund.get("floatShares") or fund.get("float_shares"), shares_out)
    div_yield    = _safe(fund.get("dividendYield") or fund.get("dividend_yield"))
    payout_ratio = _safe(fund.get("payoutRatio") or fund.get("payout_ratio"))
    mkt_cap      = _safe(fund.get("marketCap") or fund.get("market_cap_b", 0) * 1e9, 1)
    rev_growth   = _safe(fund.get("revenueGrowth") or fund.get("revenue_growth"))
    earn_growth  = _safe(fund.get("earningsGrowth") or fund.get("earnings_growth"))

    capex_abs = abs(capex_raw)

    # ── 1. OCF Stability (0-5) ────────────────────────────────────────────────
    # OCF margin = OCF / Revenue
    ocf_margin = ocf / total_rev if total_rev > 0 else 0
    ocf_score = _score5(ocf_margin, [-0.05, 0.0, 0.05, 0.10, 0.15])

    # ── 2. CapEx Consistency (0-5) ────────────────────────────────────────────
    # Proxy: CapEx to revenue ratio — moderate is good (investing but not reckless)
    # Very high or very low are both less ideal
    capex_ratio = capex_abs / total_rev if total_rev > 0 else 0
    if capex_ratio <= 0.01:
        capex_score = 1   # No investment → low growth potential
    elif capex_ratio <= 0.05:
        capex_score = 3
    elif capex_ratio <= 0.15:
        capex_score = 5
    elif capex_ratio <= 0.25:
        capex_score = 4
    else:
        capex_score = 2   # Too heavy capex burden

    # ── 3. FCF Quality (0-5) ─────────────────────────────────────────────────
    # FCF margin
    fcf_margin = fcf / total_rev if total_rev > 0 else 0
    fcf_score = _score5(fcf_margin, [-0.05, 0.0, 0.03, 0.07, 0.12])

    # ── 4. Net Cash Strength (0-5) ────────────────────────────────────────────
    # Net cash / Market Cap ratio
    net_cash = total_cash - total_debt
    net_cash_ratio = net_cash / mkt_cap if mkt_cap > 0 else 0
    net_cash_score = _score5(net_cash_ratio, [-0.20, -0.05, 0.0, 0.05, 0.15])

    # ── 5. Dilution Risk (0-5, inverted) ─────────────────────────────────────
    # Float vs shares outstanding — high float / issued shares = low dilution risk
    dilution_ratio = float_shares / shares_out if shares_out > 0 else 1.0
    # If float ~ shares → no dilution. We penalise float < 70% of outstanding
    if dilution_ratio >= 0.95:
        dilution_score = 5
    elif dilution_ratio >= 0.85:
        dilution_score = 4
    elif dilution_ratio >= 0.70:
        dilution_score = 3
    elif dilution_ratio >= 0.50:
        dilution_score = 2
    else:
        dilution_score = 1

    # ── 6. Debt Tolerance (0-5) ───────────────────────────────────────────────
    # Debt / OCF ratio (years to repay). Lower is better.
    if ocf > 0:
        debt_to_ocf = total_debt / ocf
        debt_score = _score5(-debt_to_ocf, [-20, -10, -5, -2, -0.5])
    elif total_debt <= 0:
        debt_score = 5   # No debt
    else:
        debt_score = 0   # Debt with no OCF

    # ── 7. Shareholder Return (0-5) ────────────────────────────────────────────
    # Based on dividend yield + payout ratio
    if div_yield > 0:
        if div_yield >= 0.04 and 0.1 <= payout_ratio <= 0.6:
            shareholder_score = 5
        elif div_yield >= 0.02:
            shareholder_score = 4
        elif div_yield >= 0.01:
            shareholder_score = 3
        else:
            shareholder_score = 2
    else:
        shareholder_score = 1   # No dividend (could be buyback but we can't tell)

    # ── 8. M&A Capacity (0-5) ─────────────────────────────────────────────────
    # Net cash ratio + FCF margin combined
    mna_composite = (net_cash_ratio * 0.6) + (fcf_margin * 0.4)
    mna_score = _score5(mna_composite, [-0.1, 0.0, 0.03, 0.08, 0.15])

    # ── Aggregate ─────────────────────────────────────────────────────────────
    components = {
        "ocf_stability":     int(_clamp(ocf_score)),
        "capex_consistency": int(_clamp(capex_score)),
        "fcf_quality":       int(_clamp(fcf_score)),
        "net_cash_strength": int(_clamp(net_cash_score)),
        "dilution_risk":     int(_clamp(dilution_score)),
        "debt_tolerance":    int(_clamp(debt_score)),
        "shareholder_return": int(_clamp(shareholder_score)),
        "mna_capacity":      int(_clamp(mna_score)),
    }

    total_raw = sum(components.values())   # 0-40
    capital_score = round(total_raw / 40 * 100, 1)

    # Grade
    if capital_score >= 75:
        grade = "A"
    elif capital_score >= 60:
        grade = "B"
    elif capital_score >= 45:
        grade = "C"
    elif capital_score >= 30:
        grade = "D"
    else:
        grade = "F"

    return {
        "capital_score": capital_score,
        "capital_components": components,
        "capital_grade": grade,
    }


# Human-readable labels for each component (for UI display)
COMPONENT_LABELS = {
    "ocf_stability":      "営業CF安定性",
    "capex_consistency":  "設備投資一貫性",
    "fcf_quality":        "FCF創出力",
    "net_cash_strength":  "ネットキャッシュ",
    "dilution_risk":      "希薄化リスク",
    "debt_tolerance":     "有利子負債耐性",
    "shareholder_return": "株主還元方針",
    "mna_capacity":       "M&A余力",
}
