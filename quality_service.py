"""Sprint 3: Quality of Momentum Service.

Computes a 0-100 quality score measuring how 'clean' and 'sustainable'
the momentum is, and assigns an entry-difficulty label.

High quality = low ATR chop, volume-confirmed moves, clean trend structure.

Components (25 pts each):
    1. ボラ安定性     — ATR% relative to price (lower = cleaner)
    2. 出来高品質     — up-day volume / down-day volume ratio
    3. トレンド純度   — gap dependency ratio + wick ratio
    4. 決算リスク控除 — proximity to earnings date

Entry Difficulty labels (rule-based, post-score):
    良好 / 押し待ち候補 / 初動監視 / 追いかけ注意 /
    ボラ高注意 / 決算通過待ち / 地合い依存強め / 様子見
"""

import math


# ── Entry difficulty color/label metadata ─────────────────────────────────────

ENTRY_DIFFICULTY_META = {
    "良好":         {"color": "#10b981", "bg": "bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400"},
    "押し待ち候補": {"color": "#0ea5e9", "bg": "bg-sky-100 dark:bg-sky-900/30 text-sky-700 dark:text-sky-400"},
    "初動監視":     {"color": "#8b5cf6", "bg": "bg-violet-100 dark:bg-violet-900/30 text-violet-700 dark:text-violet-400"},
    "追いかけ注意": {"color": "#f43f5e", "bg": "bg-rose-100 dark:bg-rose-900/30 text-rose-700 dark:text-rose-400"},
    "ボラ高注意":   {"color": "#f97316", "bg": "bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400"},
    "決算通過待ち": {"color": "#f59e0b", "bg": "bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400"},
    "地合い依存強め":{"color": "#64748b","bg": "bg-slate-100 dark:bg-gray-800 text-slate-600 dark:text-gray-400"},
    "様子見":       {"color": "#9ca3af", "bg": "bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-500"},
}


def compute_quality(
    df,
    momentum_score: float,
    ma50_dev: float,
    ma25_dev: float,
    bb_width: float,
    days_to_earnings,
    rsi: float,
) -> dict:
    """Compute Quality of Momentum score and entry difficulty label.

    Args:
        df: pandas DataFrame with Open/High/Low/Close/Volume columns (≥30 rows).
        momentum_score: 0-100 momentum score.
        ma50_dev: % deviation from 50-day MA (positive = above).
        ma25_dev: % deviation from 25-day MA (positive = above).
        bb_width: Bollinger Band width %.
        days_to_earnings: int or None.
        rsi: latest RSI value.

    Returns:
        dict with quality_score (0-100), quality_components dict,
        entry_difficulty str.
    """
    try:
        close_s = df["Close"]
        high_s  = df["High"]
        low_s   = df["Low"]
        vol_s   = df["Volume"]
        open_s  = df["Open"]
    except Exception:
        return _default_result()

    n = len(close_s)
    if n < 20:
        return _default_result()

    close  = close_s.values
    high   = high_s.values
    low    = low_s.values
    volume = vol_s.values
    opens  = open_s.values

    # ── 1. ボラ安定性 (ATR%) ─────────────────────────────────────────────────
    atr_pct = _compute_atr_pct(close, high, low)
    if atr_pct is None:
        vol_stability_score = 12
    elif atr_pct < 1.5:
        vol_stability_score = 25
    elif atr_pct < 2.5:
        vol_stability_score = 20
    elif atr_pct < 4.0:
        vol_stability_score = 13
    elif atr_pct < 6.0:
        vol_stability_score = 6
    else:
        vol_stability_score = 0

    # ── 2. 出来高品質 (上昇日/下落日の出来高比) ──────────────────────────────
    up_down_ratio = _compute_vol_quality(close, volume)
    if up_down_ratio is None:
        vol_quality_score = 12
    elif up_down_ratio > 2.5:
        vol_quality_score = 25
    elif up_down_ratio > 1.8:
        vol_quality_score = 20
    elif up_down_ratio > 1.2:
        vol_quality_score = 13
    elif up_down_ratio > 0.8:
        vol_quality_score = 6
    else:
        vol_quality_score = 0

    # ── 3. トレンド純度 (ギャップ率 + ヒゲ比率) ──────────────────────────────
    gap_dep, wick_ratio = _compute_trend_purity(close, opens, high, low)

    if gap_dep is None:
        gap_score = 6
    elif gap_dep < 0.25:
        gap_score = 13
    elif gap_dep < 0.45:
        gap_score = 10
    elif gap_dep < 0.65:
        gap_score = 6
    else:
        gap_score = 0

    if wick_ratio is None:
        wick_score = 6
    elif wick_ratio < 0.25:
        wick_score = 12
    elif wick_ratio < 0.45:
        wick_score = 9
    elif wick_ratio < 0.70:
        wick_score = 5
    else:
        wick_score = 0

    purity_score = gap_score + wick_score  # max 25

    # ── 4. 決算リスク控除 ─────────────────────────────────────────────────────
    if days_to_earnings is not None and days_to_earnings >= 0:
        if days_to_earnings <= 3:
            earnings_score = 4
        elif days_to_earnings <= 7:
            earnings_score = 12
        elif days_to_earnings <= 14:
            earnings_score = 19
        else:
            earnings_score = 25
    else:
        earnings_score = 25  # unknown → no penalty

    # ── Total ─────────────────────────────────────────────────────────────────
    raw = vol_stability_score + vol_quality_score + purity_score + earnings_score
    quality_score = round(min(100.0, max(0.0, float(raw))), 1)

    # ── Entry difficulty ──────────────────────────────────────────────────────
    entry_difficulty = _compute_entry_difficulty(
        momentum_score=momentum_score,
        quality_score=quality_score,
        ma50_dev=ma50_dev,
        ma25_dev=ma25_dev,
        bb_width=bb_width,
        days_to_earnings=days_to_earnings,
        rsi=rsi,
        atr_pct=atr_pct,
    )

    return {
        "quality_score": quality_score,
        "quality_components": {
            "vol_stability_score": vol_stability_score,
            "vol_quality_score": vol_quality_score,
            "purity_score": purity_score,
            "earnings_score": earnings_score,
            "atr_pct": round(atr_pct, 2) if atr_pct is not None else None,
            "up_down_vol_ratio": round(up_down_ratio, 2) if up_down_ratio is not None else None,
            "gap_dep": round(gap_dep, 2) if gap_dep is not None else None,
            "wick_ratio": round(wick_ratio, 2) if wick_ratio is not None else None,
        },
        "entry_difficulty": entry_difficulty,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _compute_atr_pct(close, high, low, window=20):
    """Average True Range as % of latest close."""
    try:
        n = min(window, len(close) - 1)
        if n < 5:
            return None
        tr_vals = []
        for i in range(1, n + 1):
            c_prev = close[-i - 1]
            h = high[-i]
            lo = low[-i]
            tr_vals.append(max(h - lo, abs(h - c_prev), abs(lo - c_prev)))
        atr = sum(tr_vals) / len(tr_vals)
        return (atr / close[-1] * 100) if close[-1] > 0 else None
    except Exception:
        return None


def _compute_vol_quality(close, volume, window=20):
    """Ratio of volume on up-days vs down-days over last `window` bars."""
    try:
        n = min(window, len(close) - 1)
        if n < 5:
            return None
        up_vol = dn_vol = 0.0
        for i in range(1, n + 1):
            v = float(volume[-i])
            if close[-i] > close[-i - 1]:
                up_vol += v
            else:
                dn_vol += v
        return up_vol / dn_vol if dn_vol > 0 else 2.0
    except Exception:
        return None


def _compute_trend_purity(close, opens, high, low, window=20):
    """
    Returns (gap_dependency_ratio, wick_ratio) over last `window` bars.
    gap_dep: fraction of total move attributable to overnight gaps.
    wick_ratio: average (wick_total) / body over all candles.
    """
    try:
        n = min(window, len(close) - 1)
        if n < 5:
            return None, None

        total_move = gap_move = 0.0
        total_body = total_wick = 0.0

        for i in range(1, n + 1):
            o = float(opens[-i])
            c = float(close[-i])
            h = float(high[-i])
            lo = float(low[-i])
            c_prev = float(close[-i - 1])

            day_move = abs(c - c_prev)
            gap = abs(o - c_prev)
            total_move += day_move
            gap_move += gap

            body = abs(c - o)
            upper_wick = max(0.0, h - max(c, o))
            lower_wick = max(0.0, min(c, o) - lo)
            total_body += body
            total_wick += (upper_wick + lower_wick)

        gap_dep = gap_move / total_move if total_move > 0 else 0.5
        wick_ratio = total_wick / total_body if total_body > 0 else 1.0
        return gap_dep, wick_ratio
    except Exception:
        return None, None


def _compute_entry_difficulty(
    momentum_score, quality_score, ma50_dev, ma25_dev,
    bb_width, days_to_earnings, rsi, atr_pct,
):
    """Rule-based entry difficulty label (ordered by priority)."""

    # 決算通過待ち: earnings within 5 days
    if days_to_earnings is not None and 0 <= days_to_earnings <= 5:
        return "決算通過待ち"

    # ボラ高注意: ATR too wide → unpredictable intraday moves
    if atr_pct is not None and atr_pct >= 5.5:
        return "ボラ高注意"

    # 追いかけ注意: strong momentum but RSI extreme + far above MAs
    if momentum_score >= 72 and rsi >= 76 and (ma50_dev is not None and ma50_dev >= 15):
        return "追いかけ注意"

    # 初動監視: BB compressed + momentum still building
    if bb_width is not None and bb_width < 5.5 and momentum_score < 60:
        return "初動監視"

    # 地合い依存強め: low quality = fragile trend
    if quality_score < 38:
        return "地合い依存強め"

    # 押し待ち候補: solid quality + slightly hot RSI → wait for dip
    if quality_score >= 62 and momentum_score >= 62 and rsi >= 66:
        return "押し待ち候補"

    # 良好: quality and momentum both decent, RSI not extreme
    if quality_score >= 52 and momentum_score >= 52 and rsi < 75:
        return "良好"

    return "様子見"


def finalize_quality(
    base_components: dict,
    momentum_score: float,
    ma50_dev: float,
    ma25_dev: float,
    bb_width: float,
    days_to_earnings,
    rsi: float,
) -> dict:
    """Recompute quality score and entry difficulty using pre-computed base
    component scores (from raw df) plus the now-known earnings proximity.

    Call this in the ranking loop after fundamentals (earnings_date) are fetched.

    Args:
        base_components: dict returned by compute_quality()["quality_components"].
        All other args: same as compute_quality().

    Returns:
        Updated dict with quality_score, quality_components, entry_difficulty.
    """
    # Use pre-computed OHLCV-based scores
    vol_stability_score = base_components.get("vol_stability_score", 12)
    vol_quality_score   = base_components.get("vol_quality_score",   12)
    purity_score        = base_components.get("purity_score",        12)
    atr_pct             = base_components.get("atr_pct")

    # Fresh earnings score
    if days_to_earnings is not None and days_to_earnings >= 0:
        if days_to_earnings <= 3:
            earnings_score = 4
        elif days_to_earnings <= 7:
            earnings_score = 12
        elif days_to_earnings <= 14:
            earnings_score = 19
        else:
            earnings_score = 25
    else:
        earnings_score = 25

    quality_score = round(min(100.0, max(0.0, float(
        vol_stability_score + vol_quality_score + purity_score + earnings_score
    ))), 1)

    entry_difficulty = _compute_entry_difficulty(
        momentum_score=momentum_score,
        quality_score=quality_score,
        ma50_dev=ma50_dev,
        ma25_dev=ma25_dev,
        bb_width=bb_width,
        days_to_earnings=days_to_earnings,
        rsi=rsi,
        atr_pct=atr_pct,
    )

    updated_components = dict(base_components)
    updated_components["earnings_score"] = earnings_score

    return {
        "quality_score": quality_score,
        "quality_components": updated_components,
        "entry_difficulty": entry_difficulty,
    }


def _default_result():
    return {
        "quality_score": 50.0,
        "quality_components": {},
        "entry_difficulty": "様子見",
    }
