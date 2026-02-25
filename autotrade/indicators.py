from __future__ import annotations

from typing import Iterable, Sequence


def ema(values: Sequence[float], period: int) -> list[float]:
    if not values:
        return []
    if period <= 1:
        return list(values)
    alpha = 2 / (period + 1)
    out: list[float] = [float(values[0])]
    for v in values[1:]:
        out.append((float(v) * alpha) + (out[-1] * (1 - alpha)))
    return out


def sma(values: Sequence[float], period: int) -> list[float]:
    if not values:
        return []
    out: list[float] = []
    run = 0.0
    for i, v in enumerate(values):
        run += float(v)
        if i >= period:
            run -= float(values[i - period])
        denom = min(i + 1, period)
        out.append(run / denom)
    return out


def atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int) -> list[float]:
    if not highs or len(highs) != len(lows) or len(lows) != len(closes):
        return []
    trs: list[float] = []
    prev_close = closes[0]
    for h, l, c in zip(highs, lows, closes):
        tr = max(float(h) - float(l), abs(float(h) - float(prev_close)), abs(float(l) - float(prev_close)))
        trs.append(tr)
        prev_close = float(c)
    return ema(trs, period)


def pct_change(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return (b - a) / a * 100.0


def slope(values: Sequence[float], lookback: int) -> float:
    if len(values) < max(2, lookback):
        return 0.0
    window = [float(v) for v in values[-lookback:]]
    x = list(range(len(window)))
    x_mean = sum(x) / len(x)
    y_mean = sum(window) / len(window)
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, window))
    den = sum((xi - x_mean) ** 2 for xi in x) or 1.0
    return num / den


def average(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        return 0.0
    return sum(float(v) for v in values) / len(values)


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))

