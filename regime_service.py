"""Sprint 2: Market Regime Classification Service.

Rule-based classification of current market conditions using:
- Market breadth (ADL trend, breadth_pct)
- Momentum score distribution (broad vs narrow leadership)
- Sector rotation trends (accelerating/decelerating sectors)

No external API calls — all data comes from existing screening pipeline.
"""

import statistics

# ── Regime definitions ────────────────────────────────────────────────────────

REGIMES = {
    "健全なリスクオン": {
        "color": "#10b981",   # emerald
        "dot": "🟢",
        "implication": "広範な上昇局面。モメンタム戦略が最も有効。高スコア銘柄への積極参加を検討。",
    },
    "一極集中型": {
        "color": "#f59e0b",   # amber
        "dot": "🟡",
        "implication": "特定銘柄・セクターが牽引。インデックスの動きに惑わされず、RS「本命」銘柄を精選。",
    },
    "セクターローテーション中": {
        "color": "#06b6d4",   # cyan
        "dot": "🔵",
        "implication": "資金移動が活発。入れ替わりの早いフェーズ。出来高先行型・押し目継続型タグに注目。",
    },
    "軟調・様子見": {
        "color": "#94a3b8",   # slate
        "dot": "⚪",
        "implication": "方向感が乏しい。ポジションサイズを抑え、明確なシグナルが出るまで待機を優先。",
    },
    "リスクオフ移行": {
        "color": "#f97316",   # orange
        "dot": "🟠",
        "implication": "下落圧力が増している。利益確定・ポジション縮小を優先。新規エントリーは慎重に。",
    },
    "調整・下落局面": {
        "color": "#f43f5e",   # rose
        "dot": "🔴",
        "implication": "広範な下落局面。守りを固める。リバーサル初期型タグの銘柄で反転確認後に検討。",
    },
    "底値反発試み": {
        "color": "#8b5cf6",   # violet
        "dot": "🟣",
        "implication": "売られすぎ後の反発試み。成功確率は5割程度。少量打診買いと損切り設定を徹底。",
    },
}


def classify(
    breadth_pct: float,
    adl_data: list,
    momentum_scores: list,
    sector_rotation: list,
) -> dict:
    """Classify market regime from available screening data.

    Args:
        breadth_pct: Latest advance-decline breadth percentage (-100 to +100).
        adl_data: List of daily breadth records [{'adl': float, 'breadth_pct': float, ...}].
                  Expected chronological order (oldest first).
        momentum_scores: List of momentum scores for top-ranked stocks (0-100).
        sector_rotation: List of sector rotation dicts with 'trend' key.

    Returns:
        dict with keys: regime_label, confidence, description, signals, implication, color, dot
    """
    signals = []
    scores_map = {}  # label → sub-score (0-1)

    # ── 1. Breadth analysis ───────────────────────────────────────────────────
    breadth = float(breadth_pct) if breadth_pct is not None else 0.0

    if breadth > 40:
        scores_map["健全なリスクオン"] = scores_map.get("健全なリスクオン", 0) + 0.35
        signals.append(f"騰落比率 +{breadth:.1f}% (広範な上昇)")
    elif breadth > 15:
        scores_map["健全なリスクオン"] = scores_map.get("健全なリスクオン", 0) + 0.15
        scores_map["一極集中型"] = scores_map.get("一極集中型", 0) + 0.10
        signals.append(f"騰落比率 +{breadth:.1f}% (緩やかな上昇優勢)")
    elif breadth > -15:
        scores_map["セクターローテーション中"] = scores_map.get("セクターローテーション中", 0) + 0.15
        scores_map["軟調・様子見"] = scores_map.get("軟調・様子見", 0) + 0.15
        signals.append(f"騰落比率 {breadth:+.1f}% (拮抗)")
    elif breadth > -35:
        scores_map["リスクオフ移行"] = scores_map.get("リスクオフ移行", 0) + 0.25
        signals.append(f"騰落比率 {breadth:+.1f}% (下落優勢)")
    else:
        scores_map["調整・下落局面"] = scores_map.get("調整・下落局面", 0) + 0.35
        signals.append(f"騰落比率 {breadth:+.1f}% (広範な下落)")

    # ── 2. ADL trend analysis ─────────────────────────────────────────────────
    adl_trend = _compute_adl_trend(adl_data)
    if adl_trend is not None:
        if adl_trend > 200:
            scores_map["健全なリスクオン"] = scores_map.get("健全なリスクオン", 0) + 0.25
            signals.append(f"ADL 強い上昇トレンド (5日平均が30日平均比 +{adl_trend:.0f})")
        elif adl_trend > 50:
            scores_map["健全なリスクオン"] = scores_map.get("健全なリスクオン", 0) + 0.10
            signals.append(f"ADL 上昇トレンド継続")
        elif adl_trend > -50:
            # ADL flat → could be rotation or concentration
            scores_map["セクターローテーション中"] = scores_map.get("セクターローテーション中", 0) + 0.10
            signals.append("ADL 横ばい (資金移動の可能性)")
        elif adl_trend > -200:
            scores_map["リスクオフ移行"] = scores_map.get("リスクオフ移行", 0) + 0.20
            scores_map["底値反発試み"] = scores_map.get("底値反発試み", 0) + 0.05
            signals.append(f"ADL 下落トレンド (5日平均が30日平均比 {adl_trend:.0f})")
        else:
            scores_map["調整・下落局面"] = scores_map.get("調整・下落局面", 0) + 0.25
            signals.append(f"ADL 急落トレンド")

    # ── 3. Breadth recovery check (possible reversal) ─────────────────────────
    reversal = _detect_reversal(adl_data, breadth_pct)
    if reversal:
        scores_map["底値反発試み"] = scores_map.get("底値反発試み", 0) + 0.30
        signals.append("直近5日間の騰落比率が改善傾向 (底値反発の可能性)")

    # ── 4. Momentum score distribution ───────────────────────────────────────
    if momentum_scores:
        avg_score = statistics.mean(momentum_scores)
        try:
            stdev = statistics.stdev(momentum_scores) if len(momentum_scores) >= 2 else 0
        except Exception:
            stdev = 0

        if avg_score >= 65:
            scores_map["健全なリスクオン"] = scores_map.get("健全なリスクオン", 0) + 0.20
            signals.append(f"モメンタムスコア平均 {avg_score:.1f} (強い)")
        elif avg_score >= 50:
            if stdev > 20:
                # High dispersion = few leaders
                scores_map["一極集中型"] = scores_map.get("一極集中型", 0) + 0.20
                signals.append(f"モメンタムスコア平均 {avg_score:.1f}、分散大 (一部銘柄集中)")
            else:
                scores_map["セクターローテーション中"] = scores_map.get("セクターローテーション中", 0) + 0.10
                signals.append(f"モメンタムスコア平均 {avg_score:.1f} (中程度)")
        else:
            scores_map["軟調・様子見"] = scores_map.get("軟調・様子見", 0) + 0.15
            signals.append(f"モメンタムスコア平均 {avg_score:.1f} (低調)")

    # ── 5. Sector rotation analysis ───────────────────────────────────────────
    if sector_rotation:
        accel = [s for s in sector_rotation if s.get("trend") == "加速"]
        decel = [s for s in sector_rotation if s.get("trend") in ("衰退", "減速")]
        recover = [s for s in sector_rotation if s.get("trend") == "回復"]

        if len(accel) >= 3:
            scores_map["健全なリスクオン"] = scores_map.get("健全なリスクオン", 0) + 0.15
            signals.append(f"{len(accel)}セクターが加速中 (広範な資金流入)")
        elif len(accel) >= 1 and len(decel) >= 1:
            scores_map["セクターローテーション中"] = scores_map.get("セクターローテーション中", 0) + 0.20
            signals.append(f"セクター間で加速/減速混在 (ローテーション活発)")
        elif len(recover) >= 2:
            scores_map["底値反発試み"] = scores_map.get("底値反発試み", 0) + 0.10
            signals.append(f"{len(recover)}セクターが回復トレンドへ転換")
        elif len(decel) >= 3:
            scores_map["リスクオフ移行"] = scores_map.get("リスクオフ移行", 0) + 0.15
            signals.append(f"{len(decel)}セクターが減速/衰退 (資金撤退)")

    # ── 6. Pick the winner ────────────────────────────────────────────────────
    if not scores_map:
        regime_label = "軟調・様子見"
        confidence = 0.40
    else:
        regime_label = max(scores_map, key=scores_map.get)
        raw_conf = scores_map[regime_label]
        # Normalize confidence: cap at 0.95, floor at 0.35
        confidence = min(0.95, max(0.35, raw_conf))

    meta = REGIMES.get(regime_label, REGIMES["軟調・様子見"])

    return {
        "regime_label": regime_label,
        "confidence": round(confidence, 2),
        "description": _build_description(regime_label, breadth_pct, adl_trend),
        "signals": signals[:5],   # max 5 key signals
        "implication": meta["implication"],
        "color": meta["color"],
        "dot": meta["dot"],
    }


def _compute_adl_trend(adl_data: list):
    """Compare 5-day average ADL vs 30-day average ADL.

    Returns positive if ADL is trending up, negative if down, None if insufficient data.
    """
    if not adl_data or len(adl_data) < 10:
        return None
    adl_values = [d.get("adl", 0) for d in adl_data if d.get("adl") is not None]
    if len(adl_values) < 10:
        return None

    recent_5 = adl_values[-5:]
    older_30 = adl_values[max(0, len(adl_values) - 30): -5] if len(adl_values) > 5 else adl_values
    if not older_30:
        return None

    avg_recent = sum(recent_5) / len(recent_5)
    avg_older = sum(older_30) / len(older_30)
    return avg_recent - avg_older


def _detect_reversal(adl_data: list, current_breadth: float) -> bool:
    """Detect if breadth is recovering after a bearish period."""
    if not adl_data or len(adl_data) < 15:
        return False
    if current_breadth is None or current_breadth >= 0:
        return False  # Not recovering from a bearish state

    breadth_vals = [d.get("breadth_pct", 0) for d in adl_data if d.get("breadth_pct") is not None]
    if len(breadth_vals) < 15:
        return False

    # Check: was breadth deeply negative before, and has it improved recently?
    prior_avg = sum(breadth_vals[-15:-5]) / 10 if len(breadth_vals) >= 15 else 0
    recent_avg = sum(breadth_vals[-5:]) / min(5, len(breadth_vals[-5:]))
    # Reversal: prior was very negative and recent is notably better
    return prior_avg < -20 and recent_avg > prior_avg + 15


def _build_description(regime_label: str, breadth_pct: float, adl_trend) -> str:
    """Build a 1-2 sentence human-readable description."""
    b_str = f"騰落比率{breadth_pct:+.1f}%" if breadth_pct is not None else "騰落比率不明"
    trend_str = ""
    if adl_trend is not None:
        if adl_trend > 50:
            trend_str = "・ADL上昇トレンド"
        elif adl_trend < -50:
            trend_str = "・ADL下落トレンド"

    descs = {
        "健全なリスクオン":      f"{b_str}{trend_str}。多くのセクターで資金が流入する健全な上昇相場。",
        "一極集中型":           f"{b_str}{trend_str}。上昇は一部銘柄・セクターに集中し、指数が割高に見える局面。",
        "セクターローテーション中": f"{b_str}{trend_str}。資金がセクター間を活発に移動している局面。",
        "軟調・様子見":         f"{b_str}{trend_str}。方向感が定まらず、参加者が様子を見ている局面。",
        "リスクオフ移行":        f"{b_str}{trend_str}。リスク回避の動きが強まり、下落圧力が増している局面。",
        "調整・下落局面":        f"{b_str}{trend_str}。広範な売り圧力が続いており、守りを固める局面。",
        "底値反発試み":         f"{b_str}{trend_str}。下落後に反発を試みているが、継続性はまだ不確実。",
    }
    return descs.get(regime_label, f"{b_str}。相場環境は不明瞭。")
