"""Phase 1: Options Analysis Service

Computes options-derived metrics from yfinance option_chain() data:
  - Put/Call Ratio (volume & OI)
  - Gamma Exposure (GEX) via Black-Scholes gamma
  - IV Rank (approximate vs historical vol)
  - IV Skew (OTM put vs OTM call IV)
  - Max Pain (OI-weighted pain strike)
  - Unusual options activity detection

All computed from yfinance — zero additional API cost.
"""
from __future__ import annotations

import numpy as np
import yfinance as yf


# ── Constants ────────────────────────────────────────────────────────────────

_RISK_FREE_RATE = 0.045
_MIN_CONTRACTS = 5  # minimum calls+puts to compute meaningful metrics
_SQRT_2PI = np.sqrt(2.0 * np.pi)


# ── Black-Scholes Gamma ──────────────────────────────────────────────────────

def _bs_gamma(S, K, T, r, sigma):
    """Black-Scholes gamma for a European option (call = put gamma).

    Uses numpy only (no scipy dependency).
    Returns 0 for invalid inputs (T<=0, sigma<=0, S<=0).
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    sqrt_T = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    # Standard normal PDF: (1/sqrt(2π)) * exp(-d1²/2)
    pdf_d1 = np.exp(-0.5 * d1 ** 2) / _SQRT_2PI
    return pdf_d1 / (S * sigma * sqrt_T)


# ── Sub-computations ─────────────────────────────────────────────────────────

def _compute_gex(calls, puts, S, T):
    """Compute net dealer Gamma Exposure.

    Dealers are typically short calls (negative gamma) and short puts (positive gamma).
    Net GEX > 0 → price-suppressing (dealers hedge by selling rallies/buying dips)
    Net GEX < 0 → price-amplifying (dealers hedge in same direction = gamma squeeze risk)

    Returns (raw_gex, gex_score -100..+100).
    """
    r = _RISK_FREE_RATE
    total_gex = 0.0

    for _, row in calls.iterrows():
        sigma = row.get("impliedVolatility", 0)
        oi = row.get("openInterest", 0) or 0
        strike = row.get("strike", 0)
        if sigma > 0 and oi > 0 and strike > 0:
            gamma = _bs_gamma(S, strike, T, r, sigma)
            # Dealers short calls → negative gamma
            total_gex -= gamma * oi * 100 * S

    for _, row in puts.iterrows():
        sigma = row.get("impliedVolatility", 0)
        oi = row.get("openInterest", 0) or 0
        strike = row.get("strike", 0)
        if sigma > 0 and oi > 0 and strike > 0:
            gamma = _bs_gamma(S, strike, T, r, sigma)
            # Dealers short puts → positive gamma
            total_gex += gamma * oi * 100 * S

    # Normalize to -100..+100 scale
    # Use S² as normalization factor (GEX scales with price²)
    if S > 0:
        normalized = total_gex / (S * S) * 10
    else:
        normalized = 0
    gex_score = max(-100, min(100, round(normalized, 1)))
    return total_gex, gex_score


def _compute_pcr(calls, puts):
    """Compute Put/Call ratios (volume-based and OI-based)."""
    call_vol = int(calls["volume"].sum()) if "volume" in calls.columns else 0
    put_vol = int(puts["volume"].sum()) if "volume" in puts.columns else 0
    call_oi = int(calls["openInterest"].sum()) if "openInterest" in calls.columns else 0
    put_oi = int(puts["openInterest"].sum()) if "openInterest" in puts.columns else 0

    pcr_volume = round(put_vol / call_vol, 3) if call_vol > 0 else None
    pcr_oi = round(put_oi / call_oi, 3) if call_oi > 0 else None

    return {
        "pcr_volume": pcr_volume,
        "pcr_oi": pcr_oi,
        "call_volume": call_vol,
        "put_volume": put_vol,
        "call_oi": call_oi,
        "put_oi": put_oi,
    }


def _compute_max_pain(calls, puts):
    """Find the max pain strike — price at which total option holder losses are maximized.

    This is the strike that minimizes total intrinsic value for all option holders.
    """
    all_strikes = sorted(set(calls["strike"].tolist() + puts["strike"].tolist()))
    if not all_strikes:
        return None

    min_pain = float("inf")
    max_pain_strike = all_strikes[0]

    for test_price in all_strikes:
        total_pain = 0.0
        # Call holder loss: max(0, strike - test_price) is worthless, they paid premium
        # Actually: call holder value at test_price = max(0, test_price - strike) * OI
        # Pain = what holders LOSE = OI * max(0, strike - test_price) doesn't make sense
        # Correct: total value to holders at test_price
        # Max pain = strike where total holder value is MINIMIZED

        for _, row in calls.iterrows():
            oi = row.get("openInterest", 0) or 0
            strike = row.get("strike", 0)
            # Call holder gains if test_price > strike
            total_pain += max(0, test_price - strike) * oi

        for _, row in puts.iterrows():
            oi = row.get("openInterest", 0) or 0
            strike = row.get("strike", 0)
            # Put holder gains if test_price < strike
            total_pain += max(0, strike - test_price) * oi

        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = test_price

    return max_pain_strike


def _compute_iv_metrics(calls, puts, S, hist_vol):
    """Compute ATM IV, IV Rank (approximate), and IV Skew.

    IV Rank approximation: compare ATM IV to historical realized volatility.
    IV Skew: average OTM put IV - average OTM call IV.
    """
    # Find ATM options (closest strike to current price)
    all_options = []
    for _, row in calls.iterrows():
        iv = row.get("impliedVolatility", 0)
        if iv and iv > 0:
            all_options.append({"strike": row["strike"], "iv": iv, "type": "call"})
    for _, row in puts.iterrows():
        iv = row.get("impliedVolatility", 0)
        if iv and iv > 0:
            all_options.append({"strike": row["strike"], "iv": iv, "type": "put"})

    if not all_options:
        return {"iv_atm": None, "iv_rank": None, "skew": None}

    # ATM IV: average IV of options closest to current price
    sorted_by_dist = sorted(all_options, key=lambda x: abs(x["strike"] - S))
    atm_options = sorted_by_dist[:4]  # top 4 closest
    iv_atm = round(np.mean([o["iv"] for o in atm_options]) * 100, 1)  # as percentage

    # IV Rank (approximate): compare ATM IV to historical vol
    iv_rank = None
    if hist_vol and hist_vol > 0:
        atm_iv_decimal = iv_atm / 100
        # Simple ratio-based rank: if IV = hist_vol → 50, higher → above 50
        ratio = atm_iv_decimal / hist_vol
        iv_rank = round(min(100, max(0, ratio * 50)), 1)

    # IV Skew: OTM put IV - OTM call IV
    # OTM puts: strike = 90-97% of S, OTM calls: strike = 103-110% of S
    otm_put_ivs = [o["iv"] for o in all_options
                   if o["type"] == "put" and 0.90 * S <= o["strike"] <= 0.97 * S]
    otm_call_ivs = [o["iv"] for o in all_options
                    if o["type"] == "call" and 1.03 * S <= o["strike"] <= 1.10 * S]

    skew = None
    if otm_put_ivs and otm_call_ivs:
        avg_put_iv = np.mean(otm_put_ivs)
        avg_call_iv = np.mean(otm_call_ivs)
        skew = round((avg_put_iv - avg_call_iv) * 100, 2)  # in percentage points

    return {"iv_atm": iv_atm, "iv_rank": iv_rank, "skew": skew}


def _detect_unusual_activity(calls, puts):
    """Detect unusual options activity.

    Criteria:
    - Call volume / put volume > 3 (extreme call dominance)
    - Call volume / call OI > 0.5 (high turnover vs open interest)
    """
    call_vol = calls["volume"].sum() if "volume" in calls.columns else 0
    put_vol = puts["volume"].sum() if "volume" in puts.columns else 0
    call_oi = calls["openInterest"].sum() if "openInterest" in calls.columns else 0

    if call_vol <= 0:
        return False

    vol_ratio = call_vol / put_vol if put_vol > 0 else 999
    turnover = call_vol / call_oi if call_oi > 0 else 0

    return bool(vol_ratio > 3 or turnover > 0.5)


def _score_options(metrics):
    """Compute composite options score (0-100, higher = more bullish signal).

    Weights:
    - PCR: 25% (low PCR = bullish)
    - GEX: 25% (negative GEX = squeeze potential = bullish for momentum)
    - IV Rank: 15% (moderate IV = neutral; extreme = adjust)
    - Unusual Activity: 15% (unusual calls = bullish)
    - Skew: 10% (high put skew = hedging demand)
    - Max Pain: 10% (price below max pain = potential magnet pull up)
    """
    score = 50.0  # neutral baseline

    # PCR component (25%): PCR < 0.7 = bullish, > 1.3 = bearish
    pcr = metrics.get("pcr_volume")
    if pcr is not None:
        if pcr < 0.5:
            score += 25
        elif pcr < 0.7:
            score += 15
        elif pcr < 1.0:
            score += 5
        elif pcr > 1.5:
            score -= 20
        elif pcr > 1.3:
            score -= 10

    # GEX component (25%): negative GEX = squeeze potential
    gex = metrics.get("gex_score", 0) or 0
    if gex < -50:
        score += 20  # strong negative GEX = momentum amplification
    elif gex < -20:
        score += 10
    elif gex > 50:
        score -= 10  # strong positive GEX = suppression
    elif gex > 20:
        score -= 5

    # IV Rank component (15%): moderate is neutral, extreme high = caution
    iv_rank = metrics.get("iv_rank")
    if iv_rank is not None:
        if iv_rank > 80:
            score -= 10  # very elevated IV = caution
        elif iv_rank < 20:
            score += 5   # low IV = potential for expansion

    # Unusual Activity (15%): bullish signal
    if metrics.get("unusual_activity"):
        score += 12

    # Skew (10%): high put skew = hedging demand (contrarian bullish)
    skew = metrics.get("skew")
    if skew is not None:
        if skew > 10:
            score += 5  # high put demand, could mean floor protection
        elif skew < -5:
            score -= 5  # call skew, already priced in

    # Max Pain proximity (10%)
    max_pain = metrics.get("max_pain")
    current_price = metrics.get("_current_price", 0)
    if max_pain and current_price and current_price > 0:
        dist_pct = (current_price - max_pain) / current_price * 100
        if dist_pct < -3:
            score += 5  # price below max pain = potential pull up
        elif dist_pct > 3:
            score -= 3  # price above max pain = potential pull down

    return round(max(0, min(100, score)), 1)


# ── Public API ───────────────────────────────────────────────────────────────

_FALLBACK = {
    "pcr_volume": None, "pcr_oi": None,
    "gex_score": None, "gex_label": None,
    "iv_rank": None, "iv_atm": None,
    "max_pain": None, "unusual_activity": None,
    "call_volume": 0, "put_volume": 0,
    "call_oi": 0, "put_oi": 0,
    "skew": None,
    "direction": "中立", "score": 50.0,
    "detail": "オプションデータ取得失敗",
    "gamma_squeeze_risk": False,
    "expiry_used": None,
}


def compute_options_metrics(ticker, current_price, hist_vol=None, num_expiries=1):
    """Compute options-derived metrics for a US stock.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL")
        current_price: Current stock price
        hist_vol: Historical annualized volatility (optional, for IV Rank)
        num_expiries: Number of nearest expiries to analyze (1 for screening, 2 for live)

    Returns:
        Dict with all options metrics, or fallback dict on failure.
    """
    if not current_price or current_price <= 0:
        return dict(_FALLBACK)

    try:
        t = yf.Ticker(ticker)
        expiries = t.options
        if not expiries:
            return dict(_FALLBACK)

        # Select expiry: skip if within 1 day, use next
        from datetime import datetime, timedelta
        today = datetime.now().date()
        valid_expiries = []
        for exp_str in expiries:
            try:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                days_to_exp = (exp_date - today).days
                if days_to_exp >= 2:  # skip very near-term
                    valid_expiries.append((exp_str, days_to_exp))
            except ValueError:
                continue

        if not valid_expiries:
            return dict(_FALLBACK)

        # Use nearest valid expiry (or merge first N)
        expiries_to_use = valid_expiries[:num_expiries]

        # Aggregate chains across expiries
        all_calls = []
        all_puts = []
        primary_T = None
        primary_expiry = None

        for exp_str, days in expiries_to_use:
            try:
                chain = t.option_chain(exp_str)
                calls = chain.calls.copy()
                puts = chain.puts.copy()
                # Fill NaN volumes and OI with 0
                calls["volume"] = calls["volume"].fillna(0).astype(int)
                calls["openInterest"] = calls["openInterest"].fillna(0).astype(int)
                puts["volume"] = puts["volume"].fillna(0).astype(int)
                puts["openInterest"] = puts["openInterest"].fillna(0).astype(int)
                all_calls.append(calls)
                all_puts.append(puts)
                if primary_T is None:
                    primary_T = max(days / 365.0, 1 / 365.0)
                    primary_expiry = exp_str
            except Exception:
                continue

        if not all_calls or not all_puts:
            return dict(_FALLBACK)

        import pandas as pd
        merged_calls = pd.concat(all_calls, ignore_index=True)
        merged_puts = pd.concat(all_puts, ignore_index=True)

        # Minimum contract check
        if len(merged_calls) < _MIN_CONTRACTS or len(merged_puts) < _MIN_CONTRACTS:
            fb = dict(_FALLBACK)
            fb["detail"] = "オプション取引が少なすぎます"
            fb["expiry_used"] = primary_expiry
            return fb

        S = float(current_price)

        # ── Compute all metrics ──────────────────────────────────────────────
        pcr = _compute_pcr(merged_calls, merged_puts)
        raw_gex, gex_score = _compute_gex(merged_calls, merged_puts, S, primary_T)
        max_pain = _compute_max_pain(merged_calls, merged_puts)
        iv_data = _compute_iv_metrics(merged_calls, merged_puts, S, hist_vol)
        unusual = _detect_unusual_activity(merged_calls, merged_puts)

        # GEX label
        if gex_score <= -30:
            gex_label = "ネガティブGEX"
        elif gex_score >= 30:
            gex_label = "ポジティブGEX"
        else:
            gex_label = "中立"

        # Direction
        pcr_v = pcr["pcr_volume"]
        if pcr_v is not None and pcr_v < 0.7:
            direction = "コール優勢"
        elif pcr_v is not None and pcr_v > 1.3:
            direction = "プット優勢"
        else:
            direction = "中立"

        # Gamma squeeze risk: negative GEX + high short interest implied
        gamma_squeeze_risk = bool(gex_score <= -30 and (pcr_v is not None and pcr_v < 0.8))

        metrics = {
            **pcr,
            "gex_score": gex_score,
            "gex_label": gex_label,
            "iv_rank": iv_data["iv_rank"],
            "iv_atm": iv_data["iv_atm"],
            "max_pain": max_pain,
            "unusual_activity": unusual,
            "skew": iv_data["skew"],
            "direction": direction,
            "gamma_squeeze_risk": gamma_squeeze_risk,
            "expiry_used": primary_expiry,
            "_current_price": S,  # for score calculation
        }

        # Composite score
        metrics["score"] = _score_options(metrics)
        metrics.pop("_current_price", None)

        # Detail text
        parts = []
        if gamma_squeeze_risk:
            parts.append("ガンマスクイーズ警戒")
        if gex_label != "中立":
            parts.append(gex_label)
        if unusual:
            parts.append("異常コール出来高")
        if iv_data["iv_rank"] is not None and iv_data["iv_rank"] > 75:
            parts.append(f"IV高水準({iv_data['iv_rank']:.0f})")
        if pcr_v is not None:
            parts.append(f"PCR {pcr_v:.2f}")
        if max_pain:
            dist = round((S - max_pain) / S * 100, 1)
            parts.append(f"MaxPain${max_pain:.0f}({dist:+.1f}%)")

        metrics["detail"] = " / ".join(parts) if parts else "中立"

        return metrics

    except Exception:
        return dict(_FALLBACK)
