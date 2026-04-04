"""Sprint 1: Confirmation Questions Service.

Generates 3-5 actionable "what to verify before entry" questions
based on a stock's technical/fundamental profile and assigned tags.

Input:
    stock: flat dict (same as tagging_service input)
    tags: list of tag dicts from tagging_service.assign_tags()
"""


def generate_questions(stock: dict, tags: list) -> list:
    """Return list of up to 5 question strings (sorted by importance).

    Each question is a concise Japanese string prompting the user
    to verify a specific risk or opportunity factor.
    """
    questions = []
    tag_names = {t["tag_name"] for t in tags}

    ret_1m = stock.get("ret_1m") or 0
    ret_3m = stock.get("ret_3m") or 0
    vol_ratio = stock.get("vol_ratio") or 0
    rsi = stock.get("rsi") or 50
    dist_from_high = stock.get("dist_from_high")
    days_to_earnings = stock.get("days_to_earnings")
    short_pct = stock.get("short_pct_of_float") or 0
    short_ratio = stock.get("short_ratio") or 0
    momentum_score = stock.get("momentum_score") or 0
    rs_label = stock.get("rs_label") or ""

    # 出来高急増 → 買い手の特定
    if "出来高先行型" in tag_names or vol_ratio >= 2.0:
        questions.append(
            f"出来高が{vol_ratio:.1f}倍に急増した具体的な理由（ニュース・TOB・機関新規買い）を特定できているか？"
            " 理由不明の急増は需給の歪みを示す場合がある。"
        )

    # 52週高値圏 → 上値抵抗確認
    if "高値更新初動型" in tag_names or (dist_from_high is not None and dist_from_high >= -3.0):
        high_str = f"{stock.get('high_52w', 'N/A')}" if stock.get('high_52w') else "52週高値"
        questions.append(
            f"{high_str}付近に上値抵抗（過去のブレイク失敗歴、心理的節目）はないか？"
            " ブレイクアウトが続かないケースは多い。"
        )

    # 短期過熱 → 利確圧力評価
    if "短期過熱型" in tag_names or rsi > 70:
        questions.append(
            f"RSI={rsi:.0f}の過熱圏・1M+{ret_1m:.1f}%の急騰後。"
            " 信用買い残の積み上がりや機関の利確売りタイミングと重なっていないか確認したか？"
        )

    # 決算接近 → 決算リスク評価
    if "決算先回り型" in tag_names or (days_to_earnings is not None and 0 <= days_to_earnings <= 14):
        questions.append(
            f"決算まで{days_to_earnings}日。"
            " アナリストコンセンサスに対する上振れ余地（EPS・売上）と、ガイダンス下方修正リスクはそれぞれどのくらいか？"
        )

    # 需給主導 → 踏み上げの持続性確認
    if "需給主導型" in tag_names:
        q_str = f"空売り比率{short_pct*100:.1f}%" if short_pct > 0 else f"DtC {short_ratio:.1f}日"
        questions.append(
            f"{q_str}の踏み上げ期待だが、空売り勢が正しい場合（業績悪化・バリュエーション過大）のシナリオも検討したか？"
            " スクイーズは急反転するリスクも高い。"
        )

    # 押し目 → サポートライン確認
    if "押し目継続型" in tag_names:
        questions.append(
            f"上昇トレンド中の押し目とみるが、25日MA・50日MAのサポートは機能しているか？"
            f" 直近の安値（押し目下限）を明確に設定し、損切りラインを事前に決めているか？"
        )

    # リバーサル → 下げ要因の解消確認
    if "リバーサル初期型" in tag_names:
        questions.append(
            f"3M: {ret_3m:.1f}%下落後の反転。下落要因（業績悪化・セクター売り・需給）は本当に解消されたか？"
            " Vリバより底練りが長引く可能性を排除できるか？"
        )

    # 相対強度が高い → セクター vs 個別要因の確認
    if rs_label in ("prime", "本命") and "指数逆行強者" in tag_names:
        questions.append(
            "セクター相対強度が高いが、この強さは個別の業績・催事によるものか、"
            " セクターETFへの資金流入（パッシブ買い）に乗っているだけか？"
        )

    # 高スコア銘柄 → R/R設定の確認（常時）
    if momentum_score >= 65:
        questions.append(
            f"モメンタムスコア{momentum_score:.0f}の高評価銘柄。"
            " エントリーする場合、損切りライン（例: 直近安値 -3%）と利確目標の比率（R/R ≥ 2）を事前に設定しているか？"
        )

    # 最大5問まで返す（重要度順に並べているので先頭から取る）
    return questions[:5]
