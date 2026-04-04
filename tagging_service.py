"""Sprint 1: Selection Reason Tagging Service.

Rule-based tag assignment for momentum-ranked stocks.
Each tag has a name, confidence (0-1), and reason_text.

Input: flat stock dict with fields:
    ticker, momentum_score, price,
    ret_1m, ret_3m, vol_ratio, rsi,
    dist_from_high, bb_squeeze, is_breakout, rs_label, high_52w,
    days_to_earnings, short_ratio, short_pct_of_float
"""


def assign_tags(stock: dict) -> list:
    """Return list of tag dicts for the given stock.

    Each tag: {"tag_name": str, "confidence": float, "reason_text": str}
    """
    tags = []

    ret_1m = stock.get("ret_1m") or 0
    ret_3m = stock.get("ret_3m") or 0
    vol_ratio = stock.get("vol_ratio") or 0
    rsi = stock.get("rsi") or 50
    dist_from_high = stock.get("dist_from_high")
    bb_squeeze = stock.get("bb_squeeze", False)
    is_breakout = stock.get("is_breakout", False)
    rs_label = stock.get("rs_label") or ""
    days_to_earnings = stock.get("days_to_earnings")
    short_ratio = stock.get("short_ratio") or 0
    short_pct = stock.get("short_pct_of_float") or 0
    momentum_score = stock.get("momentum_score") or 0

    # ── 出来高先行型 ─────────────────────────────────────────────────────────────
    # 出来高が急増しているが株価変化が小さい → 機関の仕込みの可能性
    if vol_ratio >= 2.0 and abs(ret_1m) < 8:
        conf = min(vol_ratio / 4.0, 1.0)
        tags.append({
            "tag_name": "出来高先行型",
            "confidence": round(conf, 2),
            "reason_text": f"出来高が20日平均の{vol_ratio:.1f}倍に急増、価格変化は軽微 → 機関の仕込み兆候の可能性",
        })

    # ── 高値更新初動型 ────────────────────────────────────────────────────────────
    # 52週高値の2%以内 + 出来高増加 → ブレイクアウト試み
    if dist_from_high is not None and dist_from_high >= -2.0 and vol_ratio >= 1.3:
        conf = 0.85 if is_breakout else 0.70
        gap_str = f"+{dist_from_high:.1f}%" if dist_from_high >= 0 else f"{dist_from_high:.1f}%"
        tags.append({
            "tag_name": "高値更新初動型",
            "confidence": conf,
            "reason_text": f"52週高値まで{gap_str}、出来高増加を伴うブレイクアウト試み",
        })

    # ── BB圧縮ブレイク型 ─────────────────────────────────────────────────────────
    # ボリンジャーバンド圧縮後に上昇 → エネルギー解放
    if bb_squeeze and ret_1m > 2.0:
        tags.append({
            "tag_name": "BB圧縮ブレイク型",
            "confidence": 0.75,
            "reason_text": "ボリンジャーバンド圧縮（低ボラティリティ蓄積）後に上昇ブレイク、エネルギー解放フェーズ",
        })

    # ── 押し目継続型 ─────────────────────────────────────────────────────────────
    # 3M強い上昇トレンド中の健全な短期調整
    if ret_3m > 8 and -6 < ret_1m < 1 and 38 <= rsi <= 62:
        conf = min(ret_3m / 20.0, 0.9)
        tags.append({
            "tag_name": "押し目継続型",
            "confidence": round(conf, 2),
            "reason_text": f"3M: +{ret_3m:.1f}%の上昇トレンド中に健全な押し目 (RSI={rsi:.0f})、エントリー好機の可能性",
        })

    # ── 短期過熱型 ───────────────────────────────────────────────────────────────
    # 1M急騰 + RSI高水準 → 利確圧力に注意
    if ret_1m > 12 and rsi > 70:
        conf = min((rsi - 70) / 20.0 + 0.6, 0.95)
        tags.append({
            "tag_name": "短期過熱型",
            "confidence": round(conf, 2),
            "reason_text": f"1M: +{ret_1m:.1f}%急騰、RSI={rsi:.0f}で過熱圏 → 利確売りや短期調整リスクに注意",
        })

    # ── 決算先回り型 ─────────────────────────────────────────────────────────────
    # 決算7日以内 + モメンタム高 → 機関の先取り買い
    if days_to_earnings is not None and 0 <= days_to_earnings <= 7 and momentum_score >= 60:
        tags.append({
            "tag_name": "決算先回り型",
            "confidence": 0.80,
            "reason_text": f"決算まで{days_to_earnings}日、モメンタムスコア{momentum_score:.0f} → 機関の先取り買いの可能性、決算サプライズに注意",
        })

    # ── 需給主導型 ───────────────────────────────────────────────────────────────
    # 空売り比率高 + 株価上昇 → スクイーズ圧力
    if short_pct > 0.10 and ret_1m > 4:  # short_pct is 0-1 range
        conf = min(short_pct * 2.5, 0.95)
        tags.append({
            "tag_name": "需給主導型",
            "confidence": round(conf, 2),
            "reason_text": f"空売り比率{short_pct*100:.1f}%の高水準、株価上昇でショートスクイーズ圧力増大",
        })
    elif short_ratio > 5 and ret_1m > 4:
        conf = min(short_ratio / 12.0, 0.85)
        tags.append({
            "tag_name": "需給主導型",
            "confidence": round(conf, 2),
            "reason_text": f"Days to Cover {short_ratio:.1f}日の需給逼迫、株価上昇でショートスクイーズ圧力",
        })

    # ── 指数逆行強者 ─────────────────────────────────────────────────────────────
    # RS「本命」判定 → ベンチマーク比で継続的アウトパフォーム
    if rs_label in ("prime", "本命") and ret_3m > 0:
        tags.append({
            "tag_name": "指数逆行強者",
            "confidence": 0.80,
            "reason_text": "相対強度「本命」判定 (1M・3Mともにセクター比でプラス)、市場全体の動きに左右されにくい実力銘柄",
        })

    # ── リバーサル初期型 ─────────────────────────────────────────────────────────
    # 3M大幅下落後に反転兆候 → 底値圏からの反発
    if ret_3m < -12 and ret_1m > 2 and rsi > 42:
        tags.append({
            "tag_name": "リバーサル初期型",
            "confidence": 0.65,
            "reason_text": f"3M: {ret_3m:.1f}%下落後に反転兆候 (RSI回復中)、下げの要因解消を要確認",
        })

    return tags
