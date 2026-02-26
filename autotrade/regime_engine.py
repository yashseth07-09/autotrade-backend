from __future__ import annotations

from typing import Any

from autotrade.config import AppConfig
from autotrade.indicators import atr, average, ema
from autotrade.models import Candle


def _regime_thresholds(config: AppConfig) -> dict[str, float | int]:
    # Backward-compatible: prefer optional strategy.regime config if present, else sane defaults.
    regime_cfg = getattr(config.strategy, "regime", None)
    return {
        "ema_fast": int(getattr(regime_cfg, "ema_fast", config.strategy.indicators.ema_fast)),
        "ema_slow": int(getattr(regime_cfg, "ema_slow", config.strategy.indicators.ema_slow)),
        "atr_period": int(getattr(regime_cfg, "atr_period", config.strategy.indicators.atr_period)),
        "atr_rank_lookback": int(getattr(regime_cfg, "atr_rank_lookback", 80)),
        "high_vol_percentile": float(getattr(regime_cfg, "high_vol_percentile", 0.75)),
        "low_vol_percentile": float(getattr(regime_cfg, "low_vol_percentile", 0.25)),
        "volume_ratio_lookback": int(getattr(regime_cfg, "volume_ratio_lookback", config.strategy.indicators.volume_ratio_lookback)),
        "high_vol_ratio": float(getattr(regime_cfg, "high_vol_ratio", 1.5)),
        "low_vol_ratio": float(getattr(regime_cfg, "low_vol_ratio", 0.75)),
        "structure_lookback": int(getattr(regime_cfg, "structure_lookback", 12)),
    }


def _percentile_rank(values: list[float], current: float) -> float:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return 0.5
    less_or_equal = sum(1 for v in clean if v <= current)
    return less_or_equal / max(1, len(clean))


def _structure_label(candles: list[Candle], lookback: int) -> str:
    if len(candles) < max(6, lookback):
        return "RANGE"
    window = candles[-lookback:]
    half = max(2, len(window) // 2)
    a = window[:half]
    b = window[half:]
    if not a or not b:
        return "RANGE"
    a_high = max(c.high for c in a)
    a_low = min(c.low for c in a)
    b_high = max(c.high for c in b)
    b_low = min(c.low for c in b)
    if b_high > a_high and b_low > a_low:
        return "HH_HL"
    if b_high < a_high and b_low < a_low:
        return "LH_LL"
    return "RANGE"


def compute_regime(symbol: str, timeframe: str, candles: list[Candle], config: AppConfig) -> dict[str, Any]:
    tf = str(timeframe).lower()
    params = _regime_thresholds(config)
    if not candles or len(candles) < max(int(params["ema_slow"]) + 5, int(params["atr_period"]) + 5):
        return {
            "symbol": symbol,
            "timeframe": tf,
            "trend": "NEUTRAL",
            "volatility": "LOW_VOL",
            "structure": "RANGE",
            "reason": "insufficient_candles",
        }

    bars = list(candles)
    closes = [float(c.close) for c in bars]
    highs = [float(c.high) for c in bars]
    lows = [float(c.low) for c in bars]
    volumes = [float(c.volume) for c in bars]
    price = closes[-1]

    ema_fast_series = ema(closes, int(params["ema_fast"]))
    ema_slow_series = ema(closes, int(params["ema_slow"]))
    atr_series = atr(highs, lows, closes, int(params["atr_period"]))

    ema_fast_last = float(ema_fast_series[-1]) if ema_fast_series else price
    ema_slow_last = float(ema_slow_series[-1]) if ema_slow_series else price
    atr_last = float(atr_series[-1]) if atr_series else 0.0
    atr_pct = (atr_last / price * 100.0) if price else 0.0

    atr_pct_series = []
    for idx, atr_val in enumerate(atr_series):
        close_val = closes[idx] if idx < len(closes) else closes[-1]
        if close_val:
            atr_pct_series.append(float(atr_val) / float(close_val) * 100.0)
    atr_rank_window = atr_pct_series[-int(params["atr_rank_lookback"]) :] if atr_pct_series else []
    atr_percentile = _percentile_rank(atr_rank_window, atr_pct)

    vol_lb = int(params["volume_ratio_lookback"])
    vol_avg = average(volumes[-(vol_lb + 1) : -1]) if len(volumes) > vol_lb else average(volumes[:-1])
    vol_ratio = (volumes[-1] / vol_avg) if vol_avg > 0 else 1.0

    if ema_fast_last > ema_slow_last and price > ema_slow_last:
        trend = "BULLISH"
    elif ema_fast_last < ema_slow_last and price < ema_slow_last:
        trend = "BEARISH"
    else:
        trend = "NEUTRAL"

    high_vol = atr_percentile >= float(params["high_vol_percentile"]) or vol_ratio >= float(params["high_vol_ratio"])
    low_vol = atr_percentile <= float(params["low_vol_percentile"]) and vol_ratio <= float(params["low_vol_ratio"])
    if high_vol:
        volatility = "HIGH_VOL"
    elif low_vol:
        volatility = "LOW_VOL"
    else:
        volatility = "NORMAL"

    structure = _structure_label(bars, int(params["structure_lookback"]))
    if trend == "BULLISH" and structure == "LH_LL":
        trend = "NEUTRAL"
    if trend == "BEARISH" and structure == "HH_HL":
        trend = "NEUTRAL"

    return {
        "symbol": symbol,
        "timeframe": tf,
        "trend": trend,
        "volatility": volatility,
        "structure": structure,
        "price": round(price, 8),
        "ema20": round(ema_fast_last, 8),
        "ema50": round(ema_slow_last, 8),
        "atr": round(atr_last, 8),
        "atr_pct": round(atr_pct, 6),
        "atr_percentile": round(atr_percentile, 4),
        "volume_ratio": round(float(vol_ratio), 4),
    }

