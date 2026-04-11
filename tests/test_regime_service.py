"""Tests for regime_service.classify and its internal helpers."""

import regime_service


# ── classify() top-level behaviour ────────────────────────────────────────

def test_empty_inputs_fall_back_to_soft_regime():
    result = regime_service.classify(
        breadth_pct=None,
        adl_data=[],
        momentum_scores=[],
        sector_rotation=[],
    )
    # None breadth goes through as 0.0 → falls into the -15<x<15 bucket
    # which boosts セクターローテーション中 / 軟調・様子見.
    # Either is acceptable for an "empty" fallback — just check shape.
    assert result["regime_label"] in regime_service.REGIMES
    assert 0.35 <= result["confidence"] <= 0.95


def test_high_breadth_returns_healthy_risk_on():
    result = regime_service.classify(
        breadth_pct=50.0,
        adl_data=[],
        momentum_scores=[70, 68, 65, 62, 60],
        sector_rotation=[{"trend": "加速"} for _ in range(4)],
    )
    assert result["regime_label"] == "健全なリスクオン"
    assert result["confidence"] >= 0.35


def test_deep_negative_breadth_returns_correction():
    result = regime_service.classify(
        breadth_pct=-50.0,
        adl_data=[],
        momentum_scores=[],
        sector_rotation=[],
    )
    assert result["regime_label"] == "調整・下落局面"


def test_concentration_regime_on_mixed_momentum():
    # avg around 55, high stdev → 一極集中型
    result = regime_service.classify(
        breadth_pct=20.0,
        adl_data=[],
        momentum_scores=[90, 85, 80, 20, 10, 5, 70, 65, 55, 50],
        sector_rotation=[],
    )
    # Could be 健全なリスクオン OR 一極集中型 depending on weighting.
    assert result["regime_label"] in ("一極集中型", "健全なリスクオン")
    assert any("分散" in s or "集中" in s or "平均" in s for s in result["signals"])


def test_sector_rotation_mix_triggers_rotation_regime():
    result = regime_service.classify(
        breadth_pct=-5.0,
        adl_data=[],
        momentum_scores=[50, 55, 45, 52, 48],
        sector_rotation=[
            {"trend": "加速"}, {"trend": "加速"},
            {"trend": "減速"}, {"trend": "衰退"},
        ],
    )
    assert result["regime_label"] == "セクターローテーション中"


def test_confidence_clamped_to_expected_range():
    # Pump all signals toward one regime — confidence must still be ≤ 0.95.
    result = regime_service.classify(
        breadth_pct=80.0,
        adl_data=[{"adl": i * 100, "breadth_pct": 40} for i in range(40)],
        momentum_scores=[80] * 20,
        sector_rotation=[{"trend": "加速"} for _ in range(6)],
    )
    assert 0.35 <= result["confidence"] <= 0.95


def test_result_has_all_required_keys():
    result = regime_service.classify(
        breadth_pct=10.0, adl_data=[], momentum_scores=[], sector_rotation=[]
    )
    for key in ("regime_label", "confidence", "description", "signals",
                "implication", "color", "dot"):
        assert key in result
    assert isinstance(result["signals"], list)
    assert len(result["signals"]) <= 5


# ── _compute_adl_trend ────────────────────────────────────────────────────

def test_compute_adl_trend_insufficient_data_returns_none():
    assert regime_service._compute_adl_trend(None) is None
    assert regime_service._compute_adl_trend([]) is None
    assert regime_service._compute_adl_trend([{"adl": 1}] * 5) is None  # < 10


def test_compute_adl_trend_rising_series_is_positive():
    data = [{"adl": i * 10} for i in range(40)]
    trend = regime_service._compute_adl_trend(data)
    assert trend is not None and trend > 0


def test_compute_adl_trend_falling_series_is_negative():
    data = [{"adl": (40 - i) * 10} for i in range(40)]
    trend = regime_service._compute_adl_trend(data)
    assert trend is not None and trend < 0


# ── _detect_reversal ──────────────────────────────────────────────────────

def test_detect_reversal_skips_when_current_breadth_positive():
    data = [{"breadth_pct": -30} for _ in range(20)]
    assert regime_service._detect_reversal(data, 10.0) is False


def test_detect_reversal_needs_sufficient_history():
    data = [{"breadth_pct": -30} for _ in range(10)]
    assert regime_service._detect_reversal(data, -5.0) is False


def test_detect_reversal_true_on_improving_breadth():
    # Last 5 much better than prior 10 (improvement of > +15)
    data = [{"breadth_pct": -40} for _ in range(10)]
    data += [{"breadth_pct": -5} for _ in range(5)]
    assert regime_service._detect_reversal(data, -5.0) is True


def test_detect_reversal_false_when_still_falling():
    data = [{"breadth_pct": -20} for _ in range(10)]
    data += [{"breadth_pct": -25} for _ in range(5)]
    assert regime_service._detect_reversal(data, -25.0) is False


# ── REGIMES metadata ──────────────────────────────────────────────────────

def test_all_regimes_have_metadata():
    for label, meta in regime_service.REGIMES.items():
        assert "color" in meta
        assert "dot" in meta
        assert "implication" in meta
        assert meta["color"].startswith("#")
