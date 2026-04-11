"""Tests for tagging_service.assign_tags.

Each of the 9 tag branches is exercised individually with a minimal stock
dict that triggers only that branch, plus a few cross-cutting tests for
empty/None handling and multi-tag scenarios.
"""

import tagging_service


def _tag_names(stock):
    return {t["tag_name"] for t in tagging_service.assign_tags(stock)}


def _tag_by_name(stock, name):
    for t in tagging_service.assign_tags(stock):
        if t["tag_name"] == name:
            return t
    return None


# ── Baseline: empty / None-heavy stock should return empty ────────────────

def test_empty_stock_returns_empty_list():
    assert tagging_service.assign_tags({}) == []


def test_none_fields_do_not_crash():
    stock = {
        "ret_1m": None, "ret_3m": None, "vol_ratio": None, "rsi": None,
        "dist_from_high": None, "bb_squeeze": None, "is_breakout": None,
        "rs_label": None, "days_to_earnings": None,
        "short_ratio": None, "short_pct_of_float": None,
        "momentum_score": None,
    }
    tags = tagging_service.assign_tags(stock)
    assert isinstance(tags, list)


# ── Tag 1: 出来高先行型 ────────────────────────────────────────────────────

def test_volume_leading_tag_triggers_on_high_vol_ratio_with_small_move():
    stock = {"vol_ratio": 2.5, "ret_1m": 3.0}
    assert "出来高先行型" in _tag_names(stock)


def test_volume_leading_tag_does_not_trigger_when_price_moved():
    stock = {"vol_ratio": 2.5, "ret_1m": 15.0}  # abs(ret_1m) >= 8
    assert "出来高先行型" not in _tag_names(stock)


# ── Tag 2: 高値更新初動型 ─────────────────────────────────────────────────

def test_near_52w_high_tag_triggers():
    stock = {"dist_from_high": -1.0, "vol_ratio": 1.5, "is_breakout": True}
    assert "高値更新初動型" in _tag_names(stock)


def test_near_52w_high_tag_higher_confidence_on_actual_breakout():
    without = _tag_by_name({"dist_from_high": -1.0, "vol_ratio": 1.5}, "高値更新初動型")
    withbreak = _tag_by_name({"dist_from_high": -1.0, "vol_ratio": 1.5, "is_breakout": True}, "高値更新初動型")
    assert without["confidence"] == 0.70
    assert withbreak["confidence"] == 0.85


# ── Tag 3: BB圧縮ブレイク型 ───────────────────────────────────────────────

def test_bb_squeeze_break_tag_triggers():
    stock = {"bb_squeeze": True, "ret_1m": 3.0}
    assert "BB圧縮ブレイク型" in _tag_names(stock)


def test_bb_squeeze_without_move_does_not_trigger():
    stock = {"bb_squeeze": True, "ret_1m": 1.0}  # ret_1m must exceed 2.0
    assert "BB圧縮ブレイク型" not in _tag_names(stock)


# ── Tag 4: 押し目継続型 ────────────────────────────────────────────────────

def test_pullback_continuation_tag_triggers():
    stock = {"ret_3m": 15.0, "ret_1m": -3.0, "rsi": 50.0}
    assert "押し目継続型" in _tag_names(stock)


def test_pullback_continuation_needs_strong_3m_uptrend():
    stock = {"ret_3m": 5.0, "ret_1m": -3.0, "rsi": 50.0}  # ret_3m <= 8
    assert "押し目継続型" not in _tag_names(stock)


# ── Tag 5: 短期過熱型 ─────────────────────────────────────────────────────

def test_short_term_overheat_tag_triggers():
    stock = {"ret_1m": 20.0, "rsi": 78.0}
    assert "短期過熱型" in _tag_names(stock)


def test_short_term_overheat_not_triggered_below_threshold():
    stock = {"ret_1m": 20.0, "rsi": 65.0}  # rsi not > 70
    assert "短期過熱型" not in _tag_names(stock)


# ── Tag 6: 決算先回り型 ───────────────────────────────────────────────────

def test_earnings_preemption_tag_triggers():
    stock = {"days_to_earnings": 5, "momentum_score": 75.0}
    assert "決算先回り型" in _tag_names(stock)


def test_earnings_preemption_needs_high_momentum():
    stock = {"days_to_earnings": 5, "momentum_score": 50.0}  # < 60
    assert "決算先回り型" not in _tag_names(stock)


def test_earnings_preemption_not_triggered_if_too_far():
    stock = {"days_to_earnings": 14, "momentum_score": 75.0}  # > 7
    assert "決算先回り型" not in _tag_names(stock)


# ── Tag 7: 需給主導型 ─────────────────────────────────────────────────────

def test_short_squeeze_tag_triggers_on_high_short_pct():
    stock = {"short_pct_of_float": 0.25, "ret_1m": 5.0}
    assert "需給主導型" in _tag_names(stock)


def test_short_squeeze_tag_fallback_on_days_to_cover():
    stock = {"short_ratio": 8.0, "ret_1m": 5.0, "short_pct_of_float": 0}
    assert "需給主導型" in _tag_names(stock)


# ── Tag 8: 指数逆行強者 ───────────────────────────────────────────────────

def test_index_outperformer_tag_triggers_on_prime_label():
    stock = {"rs_label": "本命", "ret_3m": 10.0}
    assert "指数逆行強者" in _tag_names(stock)


def test_index_outperformer_tag_not_triggered_on_neutral_label():
    stock = {"rs_label": "neutral", "ret_3m": 10.0}
    assert "指数逆行強者" not in _tag_names(stock)


# ── Tag 9: リバーサル初期型 ───────────────────────────────────────────────

def test_reversal_early_tag_triggers_after_drop_and_rebound():
    stock = {"ret_3m": -15.0, "ret_1m": 5.0, "rsi": 50.0}
    assert "リバーサル初期型" in _tag_names(stock)


def test_reversal_early_not_triggered_without_rsi_recovery():
    stock = {"ret_3m": -15.0, "ret_1m": 5.0, "rsi": 35.0}  # rsi not > 42
    assert "リバーサル初期型" not in _tag_names(stock)


# ── Multi-tag scenario ────────────────────────────────────────────────────

def test_multiple_tags_can_apply_simultaneously():
    stock = {
        "vol_ratio": 2.5, "ret_1m": 3.0,        # 出来高先行型
        "dist_from_high": -1.0,                  # 高値更新初動型
        "rs_label": "本命", "ret_3m": 10.0,      # 指数逆行強者
    }
    names = _tag_names(stock)
    assert "出来高先行型" in names
    assert "高値更新初動型" in names
    assert "指数逆行強者" in names


# ── Tag shape invariants ──────────────────────────────────────────────────

def test_every_tag_has_required_fields():
    stock = {
        "vol_ratio": 3.0, "ret_1m": 2.0,
        "dist_from_high": -0.5, "is_breakout": True,
        "bb_squeeze": True,
        "ret_3m": 12.0, "rsi": 48.0,
    }
    tags = tagging_service.assign_tags(stock)
    assert len(tags) > 0
    for t in tags:
        assert set(t.keys()) == {"tag_name", "confidence", "reason_text"}
        assert 0.0 <= t["confidence"] <= 1.0
        assert isinstance(t["reason_text"], str) and t["reason_text"]
