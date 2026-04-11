"""Tests for questions_service.generate_questions."""

import questions_service


# ── Empty inputs ──────────────────────────────────────────────────────────

def test_empty_stock_and_empty_tags_returns_empty_list():
    assert questions_service.generate_questions({}, []) == []


def test_none_tolerant_fields():
    stock = {
        "ret_1m": None, "ret_3m": None, "vol_ratio": None, "rsi": None,
        "dist_from_high": None, "days_to_earnings": None,
        "short_pct_of_float": None, "short_ratio": None,
        "momentum_score": None, "rs_label": None,
    }
    out = questions_service.generate_questions(stock, [])
    assert isinstance(out, list)


# ── Individual triggers (independent of tags, via stock conditions) ──────

def test_volume_trigger_on_high_vol_ratio():
    stock = {"vol_ratio": 3.0}
    out = questions_service.generate_questions(stock, [])
    assert any("出来高" in q for q in out)


def test_52w_high_trigger():
    stock = {"dist_from_high": -1.0}
    out = questions_service.generate_questions(stock, [])
    assert any("上値抵抗" in q for q in out)


def test_overheat_trigger_on_high_rsi():
    stock = {"rsi": 75.0, "ret_1m": 10.0}
    out = questions_service.generate_questions(stock, [])
    assert any("過熱" in q for q in out)


def test_earnings_trigger_within_14_days():
    stock = {"days_to_earnings": 10}
    out = questions_service.generate_questions(stock, [])
    assert any("決算" in q for q in out)


def test_rr_trigger_on_high_momentum():
    stock = {"momentum_score": 70.0}
    out = questions_service.generate_questions(stock, [])
    assert any("損切り" in q or "R/R" in q for q in out)


# ── Tag-based triggers ────────────────────────────────────────────────────

def test_pullback_tag_adds_support_question():
    stock = {}
    tags = [{"tag_name": "押し目継続型", "confidence": 0.8, "reason_text": "x"}]
    out = questions_service.generate_questions(stock, tags)
    assert any("サポート" in q or "MA" in q for q in out)


def test_reversal_tag_adds_cause_question():
    stock = {"ret_3m": -20.0}
    tags = [{"tag_name": "リバーサル初期型", "confidence": 0.7, "reason_text": "x"}]
    out = questions_service.generate_questions(stock, tags)
    assert any("下落要因" in q or "解消" in q for q in out)


def test_rs_label_with_outperformer_tag_adds_sector_vs_individual_question():
    stock = {"rs_label": "本命"}
    tags = [{"tag_name": "指数逆行強者", "confidence": 0.8, "reason_text": "x"}]
    out = questions_service.generate_questions(stock, tags)
    assert any("セクター" in q for q in out)


# ── Cap ────────────────────────────────────────────────────────────────────

def test_max_5_questions():
    stock = {
        "vol_ratio": 3.0,
        "dist_from_high": -1.0,
        "rsi": 78.0, "ret_1m": 15.0,
        "days_to_earnings": 5,
        "short_pct_of_float": 0.15,
        "momentum_score": 80.0,
        "rs_label": "本命",
        "ret_3m": 12.0,
    }
    tags = [
        {"tag_name": "出来高先行型", "confidence": 0.9, "reason_text": "x"},
        {"tag_name": "高値更新初動型", "confidence": 0.8, "reason_text": "x"},
        {"tag_name": "短期過熱型", "confidence": 0.85, "reason_text": "x"},
        {"tag_name": "決算先回り型", "confidence": 0.8, "reason_text": "x"},
        {"tag_name": "需給主導型", "confidence": 0.8, "reason_text": "x"},
        {"tag_name": "押し目継続型", "confidence": 0.8, "reason_text": "x"},
        {"tag_name": "指数逆行強者", "confidence": 0.8, "reason_text": "x"},
    ]
    out = questions_service.generate_questions(stock, tags)
    assert len(out) <= 5


def test_always_returns_list_of_strings():
    out = questions_service.generate_questions(
        {"vol_ratio": 3.0, "momentum_score": 70.0},
        []
    )
    assert isinstance(out, list)
    for q in out:
        assert isinstance(q, str)
        assert q  # not empty
