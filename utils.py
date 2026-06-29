from __future__ import annotations
import math


def safe(v, fallback=0.0):
    try:
        if v is None:
            return fallback
        if isinstance(v, (int, float)) and math.isnan(v):
            return fallback
        return v
    except (TypeError, ValueError):
        return fallback


def atr(highs, lows, closes, period):
    trs = [float("nan")]
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    result = [float("nan")] * len(closes)
    if len(trs) < period + 1:
        return result
    result[period] = sum(trs[1:period + 1]) / period
    for i in range(period + 1, len(trs)):
        result[i] = (result[i - 1] * (period - 1) + trs[i]) / period
    return result
