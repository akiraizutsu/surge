"""Tests for scoring_service: WEIGHT_PRESETS structure and extract_score_components."""

import math

import scoring_service


# ── WEIGHT_PRESETS structure ──────────────────────────────────────────────

def test_weight_presets_has_expected_keys():
    assert set(scoring_service.WEIGHT_PRESETS.keys()) == {
        "balanced", "volume", "trend", "breakout", "reversal"
    }


def test_weight_presets_each_sums_to_one():
    for preset_name, preset in scoring_service.WEIGHT_PRESETS.items():
        total = sum(preset["weights"].values())
        assert math.isclose(total, 1.0, abs_tol=1e-9), (
            f"{preset_name} weights sum to {total}, expected 1.0"
        )


def test_weight_presets_component_keys_match_score_components():
    expected = {c["component_name"] for c in scoring_service.SCORE_COMPONENTS}
    for preset_name, preset in scoring_service.WEIGHT_PRESETS.items():
        assert set(preset["weights"].keys()) == expected, (
            f"{preset_name} weight keys mismatch SCORE_COMPONENTS"
        )


def test_weight_presets_has_label():
    for preset_name, preset in scoring_service.WEIGHT_PRESETS.items():
        assert isinstance(preset.get("label"), str) and preset["label"], (
            f"{preset_name} has empty label"
        )


# ── SCORE_COMPONENTS default weights sum to 1.0 ───────────────────────────

def test_score_components_default_weights_sum_to_one():
    total = sum(c["weight"] for c in scoring_service.SCORE_COMPONENTS)
    assert math.isclose(total, 1.0, abs_tol=1e-9)


# ── extract_score_components ──────────────────────────────────────────────

def _full_row():
    """Return a dict shaped like the df row expected by extract_score_components."""
    return {
        "score_ret_1m":  0.80,
        "score_ret_3m":  0.60,
        "score_vol":     0.90,
        "score_ma50":    0.50,
        "score_macd":    0.70,
        "score_rsi":     0.40,
        "ret_1m":        12.5,
        "ret_3m":        25.0,
        "vol_ratio":     2.3,
        "ma50_dev":      8.1,
        "macd_hist":     0.45,
        "rsi":           62.0,
    }


def test_extract_score_components_returns_six_components():
    components = scoring_service.extract_score_components(_full_row())
    assert len(components) == 6
    names = [c["component_name"] for c in components]
    assert names == ["ret_1m", "ret_3m", "vol_ratio", "ma50_dev", "macd_hist", "rsi"]


def test_extract_score_components_full_row_values():
    components = scoring_service.extract_score_components(_full_row())
    by_name = {c["component_name"]: c for c in components}

    # ret_1m: percentile=0.80, weight=0.20 → weighted=16.0, percentile_value=80.0
    assert by_name["ret_1m"]["percentile_value"] == 80.0
    assert by_name["ret_1m"]["weighted_score"] == 16.0
    assert by_name["ret_1m"]["raw_value"] == 12.5

    # vol_ratio: percentile=0.90, weight=0.15 → weighted=13.5
    assert by_name["vol_ratio"]["percentile_value"] == 90.0
    assert by_name["vol_ratio"]["weighted_score"] == 13.5


def test_extract_score_components_has_required_keys():
    components = scoring_service.extract_score_components(_full_row())
    for c in components:
        assert set(c.keys()) == {
            "component_name", "label", "raw_value", "percentile_value", "weighted_score"
        }
        assert isinstance(c["label"], str) and c["label"]


def test_extract_score_components_nan_treated_as_zero():
    row = _full_row()
    row["score_ret_1m"] = float("nan")
    row["ret_1m"] = float("nan")
    components = scoring_service.extract_score_components(row)
    by_name = {c["component_name"]: c for c in components}
    assert by_name["ret_1m"]["percentile_value"] == 0.0
    assert by_name["ret_1m"]["weighted_score"] == 0.0
    assert by_name["ret_1m"]["raw_value"] == 0.0


def test_extract_score_components_missing_keys_default_to_zero():
    components = scoring_service.extract_score_components({})
    assert len(components) == 6
    for c in components:
        assert c["percentile_value"] == 0.0
        assert c["weighted_score"] == 0.0
        assert c["raw_value"] == 0.0


def test_extract_score_components_rounds_raw_to_four_decimals():
    row = _full_row()
    row["ret_1m"] = 1.23456789
    components = scoring_service.extract_score_components(row)
    by_name = {c["component_name"]: c for c in components}
    assert by_name["ret_1m"]["raw_value"] == 1.2346


def test_extract_score_components_rounds_percentile_to_one_decimal():
    row = _full_row()
    row["score_ret_1m"] = 0.12345
    components = scoring_service.extract_score_components(row)
    by_name = {c["component_name"]: c for c in components}
    # 0.12345 * 100 = 12.345 → round to 1 decimal = 12.3
    assert by_name["ret_1m"]["percentile_value"] == 12.3
