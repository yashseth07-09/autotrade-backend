from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from autotrade.config import AppConfig
from autotrade.indicators import atr, average, ema, slope
from autotrade.models import BTCMacroState, Bias, Candle, InstrumentInfo, SignalCandidate, StageRecord
from autotrade.utils import utc_now_iso


@dataclass(slots=True)
class ExecutionGateResult:
    status: str
    passed: bool
    failed: bool
    message: str
    meta: dict[str, Any]


class BTCMacroEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def disabled_state(self) -> BTCMacroState:
        return BTCMacroState(
            enabled=False,
            provider="none",
            btc_profile_15m="DISABLED",
            btc_profile_supportive=True,
            updated_at=utc_now_iso(),
            notes=["BTC macro disabled; CoinDCX price data drives all entries/stops."],
        )

    def evaluate_from_candles(
        self,
        candles_4h: list[Candle] | None,
        candles_15m: list[Candle] | None,
        oi_series: list[float] | None = None,
        resistance_distance_pct: float | None = None,
    ) -> BTCMacroState:
        if not self.config.exchange.use_btc_macro:
            return self.disabled_state()
        if not candles_4h or len(candles_4h) < 60 or not candles_15m or len(candles_15m) < 25:
            return BTCMacroState(
                enabled=True,
                provider=self.config.exchange.btc_macro_provider,
                btc_profile_15m="DATA_MISSING",
                btc_profile_supportive=False,
                updated_at=utc_now_iso(),
                notes=["BTC macro data missing; risk mode cautious."],
            )

        closes4 = [c.close for c in candles_4h]
        ef = ema(closes4, self.config.strategy.indicators.ema_fast)[-1]
        es = ema(closes4, self.config.strategy.indicators.ema_slow)[-1]
        price4 = closes4[-1]
        oi_s = slope(oi_series or [0.0] * 6, max(2, self.config.strategy.indicators.oi_slope_lookback))
        if ef > es and price4 > ef and oi_s >= 0:
            bias: Bias = "BULL"
        elif ef < es and price4 < ef and oi_s <= 0:
            bias = "BEAR"
        else:
            bias = "NEUTRAL"

        closes15 = [c.close for c in candles_15m]
        vols15 = [c.volume for c in candles_15m]
        price_rising = closes15[-1] > closes15[-4]
        vol_avg = average(vols15[-20:-1]) if len(vols15) >= 21 else average(vols15[:-1])
        vol_ratio = vols15[-1] / max(1e-9, vol_avg) if vol_avg > 0 else 1.0
        if price_rising and oi_s > 0 and vol_ratio >= 1.0:
            profile = "HEALTHY_TREND"
            supportive = True
        elif price_rising and oi_s < 0:
            profile = "SHORT_COVER_RALLY"
            supportive = True
        else:
            profile = "UNSUPPORTIVE"
            supportive = False
        if resistance_distance_pct is not None and resistance_distance_pct <= self.config.strategy.setup.liquidity_pool_block_distance_pct:
            supportive = False
            profile = "STALLING_UNDER_RESISTANCE"
        return BTCMacroState(
            enabled=True,
            provider=self.config.exchange.btc_macro_provider,
            bias_4h=bias,
            ema20_4h=ef,
            ema50_4h=es,
            price_4h=price4,
            oi_slope_4h=oi_s,
            btc_profile_15m=profile,
            btc_profile_supportive=supportive,
            volume_ratio_15m=vol_ratio,
            resistance_distance_pct=resistance_distance_pct,
            updated_at=utc_now_iso(),
        )


class StrategyEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def evaluate_symbol(
        self,
        *,
        instrument: InstrumentInfo,
        candles_4h: list[Candle],
        candles_15m: list[Candle],
        candles_5m: list[Candle],
        ltp: float | None,
        mark_price: float | None,
        liquidity_distance_long_pct: float | None,
        liquidity_distance_short_pct: float | None,
        btc_macro: BTCMacroState,
    ) -> tuple[SignalCandidate | None, list[StageRecord]]:
        stages: list[StageRecord] = []
        bias, bias_meta, bias_ok = self._compute_4h_bias(instrument, candles_4h)
        stages.append(self._stage(instrument.pair, "bias_4h", "EMA20/EMA50 and structure aligned", bias_meta, bias_ok, bias_meta.get("message", "")))
        if not bias_ok or bias == "NEUTRAL":
            return None, stages

        macro_ok, macro_msg, macro_meta = self._macro_gate(bias, btc_macro)
        stages.append(self._stage(instrument.pair, "macro_gate", "BTC macro supportive or disabled", macro_meta, macro_ok, macro_msg))
        if not macro_ok:
            return None, stages

        setup_res = self._evaluate_15m_setups(
            instrument=instrument,
            bias=bias,
            candles_15m=candles_15m,
            ltp=ltp,
            mark_price=mark_price,
            liquidity_distance_long_pct=liquidity_distance_long_pct,
            liquidity_distance_short_pct=liquidity_distance_short_pct,
            btc_macro=btc_macro,
        )
        candidate: SignalCandidate | None = setup_res["candidate"]
        stages.append(self._stage(instrument.pair, "setup_candidate_15m", "Breakout close or pullback continuation", setup_res["meta"], candidate is not None, setup_res["meta"].get("message", "")))
        if candidate is None:
            return None, stages

        confirmed, confirm_meta = self._signal_confirmed_15m_close(candles_15m, candidate)
        stages.append(self._stage(instrument.pair, "signal_confirmed_15m_close", f"Closed 15m candle and score >= {self.config.strategy.min_signal_score}", confirm_meta, confirmed, confirm_meta.get("message", "")))
        if not confirmed:
            return None, stages

        gate = self.evaluate_execution_gate(candidate, candles_5m)
        candidate.execution_gate_passed = gate.passed
        candidate.execution_gate_failed = gate.failed
        stages.append(self._stage(instrument.pair, "execution_gate_5m", "Next 1-2 5m bars must not invalidate", gate.meta, gate.passed, gate.message))
        return candidate, stages

    def evaluate_execution_gate(self, signal: SignalCandidate, candles_5m: list[Candle]) -> ExecutionGateResult:
        if not candles_5m or signal.signal_candle_close_ms is None:
            return ExecutionGateResult("WAIT", False, False, "awaiting_5m_bars", {"bars_seen": 0})
        post = [c for c in candles_5m if c.open_time > signal.signal_candle_close_ms]
        if not post:
            return ExecutionGateResult("WAIT", False, False, "awaiting_5m_bars", {"bars_seen": 0})
        bars = post[: signal.execution_window_bars]
        opposite_impulse = False
        for idx, c in enumerate(bars, start=1):
            if signal.side == "LONG" and c.low <= signal.invalidation_price:
                return ExecutionGateResult("FAIL", False, True, "execution_gate_failed_invalidation", {"bar": idx, "bars_seen": len(post)})
            if signal.side == "SHORT" and c.high >= signal.invalidation_price:
                return ExecutionGateResult("FAIL", False, True, "execution_gate_failed_invalidation", {"bar": idx, "bars_seen": len(post)})
            rng = max(1e-9, c.high - c.low)
            body_ratio = abs(c.close - c.open) / rng
            if signal.side == "LONG" and c.close < c.open and body_ratio > 0.6:
                opposite_impulse = True
            if signal.side == "SHORT" and c.close > c.open and body_ratio > 0.6:
                opposite_impulse = True
        if opposite_impulse:
            return ExecutionGateResult("FAIL", False, True, "execution_gate_failed_opposite_impulse", {"bars_seen": len(post)})
        return ExecutionGateResult("PASS", True, False, "execution_gate_passed", {"bars_seen": len(post), "evaluated": len(bars)})

    def _compute_4h_bias(self, instrument: InstrumentInfo, candles_4h: list[Candle]) -> tuple[Bias, dict[str, Any], bool]:
        if len(candles_4h) < self.config.strategy.indicators.ema_slow + 5:
            return "NEUTRAL", {"message": "insufficient_4h_candles", "count": len(candles_4h)}, False
        closes = [c.close for c in candles_4h]
        ef = ema(closes, self.config.strategy.indicators.ema_fast)[-1]
        es = ema(closes, self.config.strategy.indicators.ema_slow)[-1]
        price = closes[-1]
        if ef > es and price > ef:
            bias: Bias = "BULL"
        elif ef < es and price < ef:
            bias = "BEAR"
        else:
            bias = "NEUTRAL"
        return bias, {"pair": instrument.pair, "ema20": ef, "ema50": es, "price": price, "bias": bias, "message": f"4h_bias_{bias.lower()}"}, True

    def _macro_gate(self, symbol_bias: Bias, btc_macro: BTCMacroState) -> tuple[bool, str, dict[str, Any]]:
        if not self.config.exchange.use_btc_macro:
            return True, "btc_macro_disabled", {"enabled": False}
        if btc_macro.bias_4h == "NEUTRAL":
            if self.config.strategy.reduce_trades_on_btc_neutral:
                return True, "btc_neutral_reduce_size_only", {"enabled": True, "btc_bias": btc_macro.bias_4h, "profile": btc_macro.btc_profile_15m}
            return False, "btc_neutral_block", {"enabled": True, "btc_bias": btc_macro.bias_4h}
        if symbol_bias == "BULL" and btc_macro.liquidity_blocked_long:
            return False, "btc_liquidity_block_long", {"enabled": True}
        if symbol_bias == "BEAR" and btc_macro.liquidity_blocked_short:
            return False, "btc_liquidity_block_short", {"enabled": True}
        return btc_macro.btc_profile_supportive, ("btc_macro_supportive" if btc_macro.btc_profile_supportive else "btc_macro_not_supportive"), {
            "enabled": True,
            "btc_bias": btc_macro.bias_4h,
            "profile": btc_macro.btc_profile_15m,
        }

    def _evaluate_15m_setups(
        self,
        *,
        instrument: InstrumentInfo,
        bias: Bias,
        candles_15m: list[Candle],
        ltp: float | None,
        mark_price: float | None,
        liquidity_distance_long_pct: float | None,
        liquidity_distance_short_pct: float | None,
        btc_macro: BTCMacroState,
    ) -> dict[str, Any]:
        if len(candles_15m) < max(40, self.config.strategy.indicators.ema_slow + 5):
            return {"candidate": None, "meta": {"message": "insufficient_15m_candles", "count": len(candles_15m)}}
        idx = self._last_closed_index(candles_15m)
        if idx is None or idx < 20:
            return {"candidate": None, "meta": {"message": "no_closed_15m_candle"}}
        bars = candles_15m[: idx + 1]
        last = bars[-1]
        closes = [c.close for c in bars]
        highs = [c.high for c in bars]
        lows = [c.low for c in bars]
        vols = [c.volume for c in bars]
        ef_all = ema(closes, self.config.strategy.indicators.ema_fast)
        es_all = ema(closes, self.config.strategy.indicators.ema_slow)
        atr_all = atr(highs, lows, closes, self.config.strategy.indicators.atr_period)
        if not atr_all:
            return {"candidate": None, "meta": {"message": "atr_unavailable"}}
        ef = ef_all[-1]
        es = es_all[-1]
        atr_val = max(1e-9, atr_all[-1])
        lookback = self.config.strategy.setup.breakout_lookback_bars
        vol_lb = self.config.strategy.indicators.volume_ratio_lookback
        vol_avg = average(vols[-(vol_lb + 1) : -1]) if len(vols) > vol_lb else average(vols[:-1])
        vol_ratio = last.volume / max(1e-9, vol_avg) if vol_avg > 0 else 1.0
        side = "LONG" if bias == "BULL" else "SHORT"

        # Liquidity filter against direction.
        if side == "LONG" and liquidity_distance_long_pct is not None and liquidity_distance_long_pct <= self.config.strategy.setup.liquidity_pool_block_distance_pct:
            return {"candidate": None, "meta": {"message": "near_ask_liquidity_wall", "distance_pct": liquidity_distance_long_pct}}
        if side == "SHORT" and liquidity_distance_short_pct is not None and liquidity_distance_short_pct <= self.config.strategy.setup.liquidity_pool_block_distance_pct:
            return {"candidate": None, "meta": {"message": "near_bid_liquidity_wall", "distance_pct": liquidity_distance_short_pct}}

        choices: list[SignalCandidate] = []
        meta_details: dict[str, Any] = {"vol_ratio": vol_ratio, "side": side}

        # Setup A: breakout close.
        if len(bars) >= lookback + 1:
            ref = bars[-(lookback + 1) : -1]
            level = max(c.high for c in ref) if side == "LONG" else min(c.low for c in ref)
            broke = last.close > level if side == "LONG" else last.close < level
            breakout_atr = abs(last.close - level) / atr_val
            extended_atr = abs(last.close - ef) / atr_val
            breakout_ok = (
                broke
                and breakout_atr <= self.config.strategy.setup.breakout_max_atr_distance
                and vol_ratio >= self.config.strategy.setup.breakout_min_volume_ratio
                and extended_atr <= self.config.strategy.setup.breakout_max_extension_from_ema20_atr
            )
            if breakout_ok:
                stop = (min(last.low, ef) - 0.1 * atr_val) if side == "LONG" else (max(last.high, ef) + 0.1 * atr_val)
                score = 0.62 + min(0.18, max(0.0, (vol_ratio - 1.0) * 0.25)) + (0.1 if btc_macro.btc_profile_supportive else 0.0)
                choices.append(
                    self._candidate(
                        instrument=instrument,
                        side=side,
                        setup="BREAKOUT_CLOSE",
                        entry=last.close,
                        stop=stop,
                        atr_val=atr_val,
                        score=score,
                        vol_ratio=vol_ratio,
                        btc_macro=btc_macro,
                        ltp=ltp,
                        mark_price=mark_price,
                        signal_close_ms=last.close_time,
                        reasons=["breakout_close", f"breakout_atr={breakout_atr:.2f}", f"vol_ratio={vol_ratio:.2f}"],
                    )
                )
                meta_details["breakout_atr"] = breakout_atr

        # Setup B: pullback continuation.
        if len(bars) >= 3:
            prev = bars[-2]
            tol = self.config.strategy.setup.pullback_zone_tolerance_atr * atr_val
            if side == "LONG":
                trend_ok = ef > es
                pullback_zone = (prev.low <= ef + tol) and (prev.low >= es - tol)
                reclaim = last.close > ef
                body = max(0.0, last.close - last.open)
                upper_wick = max(0.0, last.high - last.close)
                body_wick_ok = body > upper_wick
                stop = min(prev.low, last.low) - 0.1 * atr_val
            else:
                trend_ok = ef < es
                pullback_zone = (prev.high >= ef - tol) and (prev.high <= es + tol)
                reclaim = last.close < ef
                body = max(0.0, last.open - last.close)
                lower_wick = max(0.0, last.close - last.low)
                body_wick_ok = body > lower_wick
                stop = max(prev.high, last.high) + 0.1 * atr_val
            if trend_ok and pullback_zone and reclaim and body_wick_ok:
                score = 0.56 + (0.08 if vol_ratio >= 1 else 0.0) + (0.06 if btc_macro.btc_profile_supportive else 0.0)
                choices.append(
                    self._candidate(
                        instrument=instrument,
                        side=side,
                        setup="PULLBACK_CONTINUATION",
                        entry=last.close,
                        stop=stop,
                        atr_val=atr_val,
                        score=score,
                        vol_ratio=vol_ratio,
                        btc_macro=btc_macro,
                        ltp=ltp,
                        mark_price=mark_price,
                        signal_close_ms=last.close_time,
                        reasons=["pullback_reclaim_ema20", f"vol_ratio={vol_ratio:.2f}"],
                    )
                )
                meta_details["pullback"] = True

        if not choices:
            return {"candidate": None, "meta": {"message": "no_valid_15m_setup", **meta_details}}
        selected = max(choices, key=lambda c: c.score)
        return {"candidate": selected, "meta": {"message": "setup_candidate_selected", "setup": selected.setup, "score": selected.score, **meta_details}}

    def _candidate(
        self,
        *,
        instrument: InstrumentInfo,
        side: str,
        setup: str,
        entry: float,
        stop: float,
        atr_val: float,
        score: float,
        vol_ratio: float,
        btc_macro: BTCMacroState,
        ltp: float | None,
        mark_price: float | None,
        signal_close_ms: int | None,
        reasons: list[str],
    ) -> SignalCandidate:
        return SignalCandidate(
            id=str(uuid4()),
            symbol=instrument.underlying or instrument.pair,
            pair=instrument.pair,
            margin_currency=instrument.margin_currency,
            side=side,  # type: ignore[arg-type]
            setup=setup,  # type: ignore[arg-type]
            entry_price=entry,
            stop_price=stop,
            invalidation_price=stop,
            atr=atr_val,
            score=round(score, 4),
            volume_ratio=round(vol_ratio, 4),
            btc_profile=btc_macro.btc_profile_15m,
            btc_bias=btc_macro.bias_4h,
            mark_price=mark_price,
            ltp=ltp,
            signal_candle_close_ms=signal_close_ms,
            reasons=reasons,
            stage_flow=["bias_4h", "macro_gate", "setup_candidate_15m"],
            created_at=utc_now_iso(),
        )

    def _signal_confirmed_15m_close(self, candles_15m: list[Candle], candidate: SignalCandidate) -> tuple[bool, dict[str, Any]]:
        idx = self._last_closed_index(candles_15m)
        if idx is None:
            return False, {"message": "no_closed_15m_candle"}
        last = candles_15m[idx]
        if candidate.signal_candle_close_ms is not None and last.close_time != candidate.signal_candle_close_ms:
            return False, {"message": "candidate_not_on_latest_15m_close", "candidate_close_ms": candidate.signal_candle_close_ms, "latest_close_ms": last.close_time}
        if candidate.score < self.config.strategy.min_signal_score:
            return False, {"message": "score_below_threshold", "score": candidate.score, "min_score": self.config.strategy.min_signal_score}
        return True, {"message": "signal_confirmed", "score": candidate.score}

    @staticmethod
    def _last_closed_index(candles: list[Candle]) -> int | None:
        if not candles:
            return None
        now_ms = int(time.time() * 1000)
        if candles[-1].close_time <= now_ms:
            return len(candles) - 1
        return len(candles) - 2 if len(candles) >= 2 else None

    @staticmethod
    def _stage(symbol: str, stage: str, expected: str, actual: dict[str, Any], passed: bool, message: str) -> StageRecord:
        return StageRecord(
            ts=utc_now_iso(),
            symbol=symbol,
            stage=stage,
            rule=stage,
            expected=expected,
            actual=actual,
            passed=passed,
            message=message,
        )

