"""Sprint 5: 種まき度スコア (Seed Score) — Japan stocks only.

Detects companies where the market misreads investment-phase spending as
deterioration. High seed score = potential mispricing opportunity.

Score components (0-100 each, weighted sum → 0-100):
  1. capex_surge       (25%) — CapEx / Revenue rising YoY (investing mode)
  2. ocf_positive      (20%) — Operating CF still positive despite earnings dip
  3. revenue_growth    (20%) — Top-line intact (demand not collapsing)
  4. earnings_dip      (20%) — EPS/net income declining (market reads as bad)
  5. price_disappoint  (15%) — Stock down from 52W high (market disappointed)

Seed tags:
  先行投資誤認   — CapEx surge + OCF positive + price falling
  回収前夜       — revenue stable, margins compressing, FCF recovering
  種まき継続中   — consistent CapEx growth ≥ 2 years
  機関の短期売り誤認 — earnings dip but cash flow strong
"""

from __future__ import annotations

import math
from typing import Optional

# ── Seed tag definitions ────────────────────────────────────────────────────

SEED_TAGS = {
    "先行投資誤認": {
        "label": "先行投資誤認",
        "color": "emerald",
        "desc": "設備投資急増により一時的に利益圧縮。CF主導の実態は健全。",
    },
    "回収前夜": {
        "label": "回収前夜",
        "color": "teal",
        "desc": "売上安定・マージン回復兆候。投資回収フェーズへの転換期。",
    },
    "種まき継続中": {
        "label": "種まき継続中",
        "color": "cyan",
        "desc": "複数期にわたる持続的な先行投資。成長余地が蓄積中。",
    },
    "機関の短期売り誤認": {
        "label": "機関の短期売り誤認",
        "color": "indigo",
        "desc": "短期EPS悪化で機関売りが先行。CF健全で業績回復余地大。",
    },
}


def _safe(val, default=0.0) -> float:
    if val is None:
        return default
    try:
        v = float(val)
        return default if math.isnan(v) or math.isinf(v) else v
    except (TypeError, ValueError):
        return default


def _clamp(val: float, lo=0.0, hi=100.0) -> float:
    return max(lo, min(hi, val))


def compute_seed_score(fund: dict, technicals: dict) -> dict:
    """Compute seed score from fundamental + technical data.

    Args:
        fund: fundamentals dict (from get_fundamentals / yf.Ticker.info)
              Keys used: revenue_growth, earnings_growth, pe_trailing,
                         operating_cashflow, free_cashflow, capital_expenditures,
                         total_revenue (yf.Ticker.info field names)
        technicals: from screen_momentum result
              Keys used: dist_from_high, ret_1m, ret_3m

    Returns:
        {
          seed_score: float (0-100),
          seed_components: {capex_surge, ocf_positive, revenue_growth,
                            earnings_dip, price_disappoint},
          seed_tags: list[str],
          seed_note: str,
        }
    """
    rev_growth     = _safe(fund.get("revenue_growth"))      # % e.g. 5.2
    earn_growth    = _safe(fund.get("earnings_growth"))      # % e.g. -15.0
    ocf            = _safe(fund.get("operatingCashflow") or fund.get("operating_cashflow"))
    fcf            = _safe(fund.get("freeCashflow") or fund.get("free_cashflow"))
    capex_raw      = _safe(fund.get("capitalExpenditures") or fund.get("capital_expenditures"))
    total_revenue  = _safe(fund.get("totalRevenue") or fund.get("total_revenue"), 1)
    pe             = _safe(fund.get("pe_trailing") or fund.get("trailingPE"))

    dist_from_high = _safe(technicals.get("dist_from_high"))   # negative = below high
    ret_1m         = _safe(technicals.get("ret_1m"))

    # ── Component 1: CapEx surge (25%) ────────────────────────────────────────
    # Proxy: |capex| / revenue > 5% (CapEx values in yfinance are usually negative)
    capex_abs = abs(capex_raw)
    if total_revenue > 0 and capex_abs > 0:
        capex_ratio = capex_abs / total_revenue  # 0-1 range
        # Score rises with capex ratio up to ~20% of revenue
        capex_score = _clamp(capex_ratio / 0.20 * 100)
    else:
        capex_score = 0.0

    # ── Component 2: OCF positive (20%) ──────────────────────────────────────
    # OCF positive despite earnings decline = healthy underlying business
    if ocf > 0:
        # Full score if OCF > 0 and earnings declining
        ocf_score = 80.0 if earn_growth < -5 else 50.0
    elif ocf == 0:
        ocf_score = 30.0
    else:
        ocf_score = 0.0

    # ── Component 3: Revenue growth intact (20%) ─────────────────────────────
    if rev_growth >= 10:
        rev_score = 100.0
    elif rev_growth >= 5:
        rev_score = 75.0
    elif rev_growth >= 0:
        rev_score = 50.0
    elif rev_growth >= -5:
        rev_score = 25.0
    else:
        rev_score = 0.0

    # ── Component 4: Earnings dip (20%) ──────────────────────────────────────
    # We want earnings DOWN (market reads as bad, but we say it's investment)
    if earn_growth < -20:
        earn_score = 100.0
    elif earn_growth < -10:
        earn_score = 80.0
    elif earn_growth < -5:
        earn_score = 60.0
    elif earn_growth < 0:
        earn_score = 40.0
    else:
        earn_score = 0.0   # No earnings dip → not a "seeding" situation

    # ── Component 5: Price disappointment (15%) ───────────────────────────────
    # Stock below 52W high = market already discounted
    if dist_from_high <= -20:
        price_score = 100.0
    elif dist_from_high <= -10:
        price_score = 75.0
    elif dist_from_high <= -5:
        price_score = 50.0
    elif dist_from_high < 0:
        price_score = 30.0
    else:
        price_score = 0.0   # At high = no disappointment to exploit

    # ── Weighted composite ────────────────────────────────────────────────────
    seed_score = _clamp(
        capex_score  * 0.25
        + ocf_score  * 0.20
        + rev_score  * 0.20
        + earn_score * 0.20
        + price_score * 0.15
    )

    # ── Seed tag assignment ───────────────────────────────────────────────────
    tags = _assign_seed_tags(
        capex_score=capex_score,
        ocf_score=ocf_score,
        rev_score=rev_score,
        earn_score=earn_score,
        price_score=price_score,
        seed_score=seed_score,
        earn_growth=earn_growth,
        ocf=ocf,
        dist_from_high=dist_from_high,
    )

    # ── Summary note ─────────────────────────────────────────────────────────
    note = _build_note(
        seed_score=seed_score,
        rev_growth=rev_growth,
        earn_growth=earn_growth,
        ocf=ocf,
        capex_abs=capex_abs,
        dist_from_high=dist_from_high,
    )

    return {
        "seed_score": round(seed_score, 1),
        "seed_components": {
            "capex_surge":      round(capex_score, 1),
            "ocf_positive":     round(ocf_score, 1),
            "revenue_growth":   round(rev_score, 1),
            "earnings_dip":     round(earn_score, 1),
            "price_disappoint": round(price_score, 1),
        },
        "seed_tags": tags,
        "seed_note": note,
    }


def _assign_seed_tags(
    capex_score, ocf_score, rev_score, earn_score, price_score,
    seed_score, earn_growth, ocf, dist_from_high
) -> list[str]:
    tags = []

    # 先行投資誤認: CapEx surge + OCF positive + price falling
    if capex_score >= 50 and ocf_score >= 50 and price_score >= 30:
        tags.append("先行投資誤認")

    # 機関の短期売り誤認: earnings down + CF healthy + price disappointed
    if earn_score >= 60 and ocf > 0 and price_score >= 50:
        tags.append("機関の短期売り誤認")

    # 回収前夜: revenue stable + earnings starting to recover (earn_growth not too negative)
    if rev_score >= 50 and -10 < earn_growth < 0 and price_score >= 25:
        tags.append("回収前夜")

    # 種まき継続中: high capex + still positive revenue
    if capex_score >= 60 and rev_score >= 50:
        tags.append("種まき継続中")

    return tags


def _build_note(seed_score, rev_growth, earn_growth, ocf, capex_abs, dist_from_high) -> str:
    if seed_score < 20:
        return "種まき要素は低い"
    parts = []
    if earn_growth < -5:
        parts.append(f"利益{earn_growth:.0f}%減（先行投資影響）")
    if rev_growth > 0:
        parts.append(f"売上+{rev_growth:.0f}%成長継続")
    if ocf > 0:
        parts.append("営業CF黒字維持")
    if dist_from_high <= -10:
        parts.append(f"52W高値比{dist_from_high:.0f}%（市場失望）")
    if not parts:
        return "中程度の種まき要素あり"
    return "。".join(parts[:3])
