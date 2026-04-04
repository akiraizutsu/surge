"""Sprint 1: Score Attribution Service.

Extracts per-component percentile scores from the momentum score DataFrame row.
These are the exact same values used in compute_momentum_score(), just exposed
for display purposes (no re-computation).
"""

# Component metadata: key = DataFrame column name, weight = momentum weight
SCORE_COMPONENTS = [
    {"key": "score_ret_1m",  "component_name": "ret_1m",    "label": "1Mリターン",   "weight": 0.20},
    {"key": "score_ret_3m",  "component_name": "ret_3m",    "label": "3Mリターン",   "weight": 0.20},
    {"key": "score_vol",     "component_name": "vol_ratio", "label": "出来高比",     "weight": 0.15},
    {"key": "score_ma50",    "component_name": "ma50_dev",  "label": "50日MA乖離",   "weight": 0.15},
    {"key": "score_macd",    "component_name": "macd_hist", "label": "MACDヒスト",   "weight": 0.15},
    {"key": "score_rsi",     "component_name": "rsi",       "label": "RSI",          "weight": 0.15},
]

# Raw value column mapping (from screener result dict)
_RAW_FIELD = {
    "ret_1m":    "ret_1m",
    "ret_3m":    "ret_3m",
    "vol_ratio": "vol_ratio",
    "ma50_dev":  "ma50_dev",
    "macd_hist": "macd_hist",
    "rsi":       "rsi",
}


def extract_score_components(df_row) -> list:
    """Build score_components list from a pandas DataFrame row.

    df_row: pandas Series from compute_momentum_score() output.
            Must contain score_ret_1m, score_ret_3m, score_vol,
            score_ma50, score_macd, score_rsi columns (0-1 range).

    Returns: list of dicts with component_name, label, raw_value,
             percentile_value (0-100), weighted_score (0-weight*100).
    """
    import math
    components = []
    for comp in SCORE_COMPONENTS:
        try:
            pct_raw = float(df_row[comp["key"]])
            if math.isnan(pct_raw):
                pct_raw = 0.0
        except (KeyError, TypeError, ValueError):
            pct_raw = 0.0

        raw_field = _RAW_FIELD[comp["component_name"]]
        try:
            raw_val = float(df_row[raw_field])
            if math.isnan(raw_val):
                raw_val = 0.0
        except (KeyError, TypeError, ValueError):
            raw_val = 0.0

        components.append({
            "component_name": comp["component_name"],
            "label": comp["label"],
            "raw_value": round(raw_val, 4),
            "percentile_value": round(pct_raw * 100, 1),
            "weighted_score": round(pct_raw * comp["weight"] * 100, 2),
        })
    return components
