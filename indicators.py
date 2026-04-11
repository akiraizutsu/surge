"""Technical indicator calculations.

Pure functions that operate on pandas Series / numpy arrays.
Extracted from screener.py to keep the main screening engine focused on
orchestration rather than per-indicator math.
"""

import numpy as np
import pandas as pd


def compute_rsi(series, period=14):
    """Compute RSI."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(series, fast=12, slow=26, signal=9):
    """Compute MACD line, signal line, and histogram."""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def compute_obv(close, volume):
    """Compute On-Balance Volume, its 20-day slope (normalised %), and divergence signal."""
    direction = np.sign(close.diff().fillna(0))
    obv = (direction * volume).cumsum()

    # 20-day OBV slope (linear regression, normalised as % of mean OBV)
    lookback = min(20, len(obv))
    obv_tail = obv.iloc[-lookback:].values.astype(float)
    x = np.arange(lookback, dtype=float)
    if lookback >= 5 and np.std(obv_tail) > 0:
        slope = np.polyfit(x, obv_tail, 1)[0]
        mean_obv = np.mean(np.abs(obv_tail)) or 1.0
        obv_slope = round(float(slope / mean_obv * 100), 2)
    else:
        obv_slope = 0.0

    # Divergence: compare 20-day price trend vs OBV trend
    if len(close) >= 20:
        price_up = float(close.iloc[-1]) > float(close.iloc[-20])
        obv_up = float(obv.iloc[-1]) > float(obv.iloc[-20])
        if price_up and not obv_up:
            obv_divergence = "bearish_div"
        elif not price_up and obv_up:
            obv_divergence = "bullish_div"
        else:
            obv_divergence = "none"
    else:
        obv_divergence = "none"

    return obv_slope, obv_divergence


def compute_drawdown(close):
    """Compute max drawdown and current drawdown over last 3 months (66 trading days)."""
    lookback = min(66, len(close))
    window = close.iloc[-lookback:]
    rolling_max = window.cummax()
    drawdown_series = (window / rolling_max - 1) * 100

    max_dd = round(float(drawdown_series.min()), 2)
    current_dd = round(float(drawdown_series.iloc[-1]), 2)
    return max_dd, current_dd


def compute_adx(high, low, close, period=14):
    """Compute Average Directional Index (ADX) — measures trend strength regardless of direction."""
    if len(close) < period * 2:
        return 0.0

    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm < 0] = 0

    # Where +DM > -DM, keep +DM, else 0 (and vice versa)
    plus_dm[plus_dm <= minus_dm] = 0
    minus_dm[minus_dm <= plus_dm] = 0

    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.ewm(span=period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(span=period, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(span=period, adjust=False).mean() / atr)

    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1)
    adx = dx.ewm(span=period, adjust=False).mean()

    return round(float(adx.iloc[-1]), 1) if not np.isnan(adx.iloc[-1]) else 0.0


def compute_support_resistance(close, high, low, n_levels=3):
    """Detect key support/resistance levels using pivot point clustering."""
    if len(close) < 60:
        return [], []

    lookback = min(120, len(close))
    h = high.iloc[-lookback:].values
    l = low.iloc[-lookback:].values
    c = close.iloc[-lookback:].values
    price = float(c[-1])

    # Find local peaks (resistance) and troughs (support)
    pivots = []
    for i in range(2, len(c) - 2):
        if h[i] > h[i-1] and h[i] > h[i-2] and h[i] > h[i+1] and h[i] > h[i+2]:
            pivots.append(float(h[i]))
        if l[i] < l[i-1] and l[i] < l[i-2] and l[i] < l[i+1] and l[i] < l[i+2]:
            pivots.append(float(l[i]))

    if not pivots:
        return [], []

    # Cluster nearby pivots (within 2% of each other)
    pivots.sort()
    clusters = []
    current_cluster = [pivots[0]]
    for p in pivots[1:]:
        if p / current_cluster[0] - 1 < 0.02:
            current_cluster.append(p)
        else:
            clusters.append(round(sum(current_cluster) / len(current_cluster), 2))
            current_cluster = [p]
    clusters.append(round(sum(current_cluster) / len(current_cluster), 2))

    support = sorted([lv for lv in clusters if lv < price], reverse=True)[:n_levels]
    resistance = sorted([lv for lv in clusters if lv >= price])[:n_levels]

    return support, resistance
