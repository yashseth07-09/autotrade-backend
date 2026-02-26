from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from autotrade.config import AppConfig
from autotrade.indicators import atr, average, ema, slope
from autotrade.models import BTCMacroState, Bias, Candle, InstrumentInfo, SignalCandidate, StageRecord
from autotrade.regime_engine import compute_regime
from autotrade.rejection_codes import classify_rejection_code
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
        spread_pct: float | None = None,
        cycle_id: str | None = None,
    ) -> tuple[SignalCandidate | None, list[StageRecord]]:
        stages: list[StageRecord] = []
        symbol_label = instrument.underlying or instrument.pair
        bias, bias_meta, bias_ok = self._compute_4h_bias(instrument, candles_4h)
        bias_label = self._bias_label(bias)
        regime_15m = compute_regime(symbol_label, "15m", candles_15m, self.config)
        regime_5m = compute_regime(symbol_label, "5m", candles_5m, self.config)
        stages.append(
            self._stage(
                instrument.pair,
                "bias_4h",
                "EMA20/EMA50 and structure aligned",
                bias_meta,
                bias_ok,
                bias_meta.get("message", ""),
                timeframe="4h",
                bias_4h=bias_label,
                cycle_id=cycle_id,
                meta={"regime_15m": regime_15m, "regime_5m": regime_5m},
            )
        )

        macro_ok = False
        macro_msg = "macro_gate_skipped_due_to_bias"
        macro_meta: dict[str, Any] = {"enabled": bool(self.config.exchange.use_btc_macro), "skipped_due_to_bias": True}
        if bias_ok and bias != "NEUTRAL":
            macro_ok, macro_msg, macro_meta = self._macro_gate(bias, btc_macro)
            stages.append(
                self._stage(
                    instrument.pair,
                    "macro_gate",
                    "BTC macro supportive or disabled",
                    macro_meta,
                    macro_ok,
                    macro_msg,
                    timeframe="15m",
                    bias_4h=bias_label,
                    cycle_id=cycle_id,
                )
            )

        setup_res = self._evaluate_15m_setups(
            instrument=instrument,
            bias=bias,
            bias_ok=bias_ok,
            macro_ok=macro_ok,
            macro_msg=macro_msg,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            ltp=ltp,
            mark_price=mark_price,
            liquidity_distance_long_pct=liquidity_distance_long_pct,
            liquidity_distance_short_pct=liquidity_distance_short_pct,
            btc_macro=btc_macro,
            spread_pct=spread_pct,
            bias_label=bias_label,
            regime_15m=regime_15m,
            regime_5m=regime_5m,
            cycle_id=cycle_id,
        )
        stages.extend(setup_res.get("stages") or [])
        candidate: SignalCandidate | None = setup_res["candidate"]
        stages.append(
            self._stage(
                instrument.pair,
                "setup_candidate_15m",
                "Breakout close or pullback continuation",
                setup_res["meta"],
                candidate is not None,
                setup_res["meta"].get("message", ""),
                timeframe="15m",
                side=(candidate.side if candidate is not None else None),
                bias_4h=bias_label,
                cycle_id=cycle_id,
                rejection_code=(None if candidate is not None else setup_res["meta"].get("rejection_code")),
                meta={"regime_15m": regime_15m, "regime_5m": regime_5m, "price": mark_price or ltp},
            )
        )
        if not bias_ok or bias == "NEUTRAL" or not macro_ok:
            return None, stages
        if candidate is None:
            return None, stages

        confirmed, confirm_meta = self._signal_confirmed_15m_close(candles_15m, candidate)
        stages.append(
            self._stage(
                instrument.pair,
                "signal_confirmed_15m_close",
                f"Closed 15m candle and score >= {self.config.strategy.min_signal_score}",
                confirm_meta,
                confirmed,
                confirm_meta.get("message", ""),
                timeframe="15m",
                side=candidate.side,
                bias_4h=bias_label,
                cycle_id=cycle_id,
                rejection_code=(None if confirmed else classify_rejection_code(stage="signal_confirmed_15m_close", message=confirm_meta.get("message"), actual=confirm_meta)),
                meta={"regime_15m": regime_15m, "regime_5m": regime_5m, "price": mark_price or ltp},
            )
        )
        if not confirmed:
            return None, stages

        gate = self.evaluate_execution_gate(candidate, candles_5m)
        candidate.execution_gate_passed = gate.passed
        candidate.execution_gate_failed = gate.failed
        stages.append(
            self._stage(
                instrument.pair,
                "execution_gate_5m",
                "Next 1-2 5m bars must not invalidate",
                gate.meta,
                gate.passed,
                gate.message,
                timeframe="5m",
                side=candidate.side,
                bias_4h=bias_label,
                cycle_id=cycle_id,
                rejection_code=(None if gate.passed else "EXECUTION_GATE_BLOCK"),
                meta={"regime_15m": regime_15m, "regime_5m": regime_5m, "price": mark_price or ltp},
            )
        )
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
        bias_ok: bool,
        macro_ok: bool,
        macro_msg: str,
        candles_15m: list[Candle],
        candles_5m: list[Candle],
        ltp: float | None,
        mark_price: float | None,
        liquidity_distance_long_pct: float | None,
        liquidity_distance_short_pct: float | None,
        btc_macro: BTCMacroState,
        spread_pct: float | None,
        bias_label: str,
        regime_15m: dict[str, Any],
        regime_5m: dict[str, Any],
        cycle_id: str | None,
    ) -> dict[str, Any]:
        long_res = self.evaluate_long_setup(
            instrument=instrument,
            bias=bias,
            bias_ok=bias_ok,
            macro_ok=macro_ok,
            macro_msg=macro_msg,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            ltp=ltp,
            mark_price=mark_price,
            liquidity_distance_pct=liquidity_distance_long_pct,
            btc_macro=btc_macro,
            spread_pct=spread_pct,
            bias_label=bias_label,
            regime_15m=regime_15m,
            regime_5m=regime_5m,
        )
        short_res = self.evaluate_short_setup(
            instrument=instrument,
            bias=bias,
            bias_ok=bias_ok,
            macro_ok=macro_ok,
            macro_msg=macro_msg,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            ltp=ltp,
            mark_price=mark_price,
            liquidity_distance_pct=liquidity_distance_short_pct,
            btc_macro=btc_macro,
            spread_pct=spread_pct,
            bias_label=bias_label,
            regime_15m=regime_15m,
            regime_5m=regime_5m,
        )

        side_results = [long_res, short_res]
        side_stage_records = [
            self._side_eval_stage(
                instrument=instrument,
                cycle_id=cycle_id,
                bias_label=bias_label,
                audit=res["audit"],
            )
            for res in side_results
        ]

        candidates = [res["candidate"] for res in side_results if res.get("candidate") is not None]
        selected = max(candidates, key=lambda c: c.score) if candidates else None

        summaries = [
            {
                "side": res["audit"].get("side"),
                "passed": bool(res["audit"].get("passed")),
                "rule": res["audit"].get("rule"),
                "rejection_code": res["audit"].get("rejection_code"),
                "message": res["audit"].get("message"),
            }
            for res in side_results
        ]
        selected_side_res = None
        if selected is not None:
            selected_side_res = next((res for res in side_results if res.get("candidate") is selected), None)
        elif bias == "BULL":
            selected_side_res = long_res
        elif bias == "BEAR":
            selected_side_res = short_res

        if selected is None:
            rejection_code = (
                (selected_side_res or {}).get("audit", {}).get("rejection_code")
                or next((res["audit"].get("rejection_code") for res in side_results if res["audit"].get("rejection_code")), None)
            )
            message = (
                (selected_side_res or {}).get("audit", {}).get("message")
                or next((res["audit"].get("message") for res in side_results if res["audit"].get("message")), "no_valid_15m_setup")
            )
            return {
                "candidate": None,
                "stages": side_stage_records,
                "meta": {
                    "message": message,
                    "rejection_code": rejection_code or "NO_STRUCTURE_BREAK",
                    "bias_4h": bias_label,
                    "side_evaluations": summaries,
                    "price": mark_price or ltp,
                    "regime_15m": regime_15m,
                    "regime_5m": regime_5m,
                },
            }

        return {
            "candidate": selected,
            "stages": side_stage_records,
            "meta": {
                "message": "setup_candidate_selected",
                "setup": selected.setup,
                "score": selected.score,
                "side": selected.side,
                "bias_4h": bias_label,
                "side_evaluations": summaries,
                "price": mark_price or ltp,
                "regime_15m": regime_15m,
                "regime_5m": regime_5m,
            },
        }

    def evaluate_long_setup(
        self,
        *,
        instrument: InstrumentInfo,
        bias: Bias,
        bias_ok: bool,
        macro_ok: bool,
        macro_msg: str,
        candles_15m: list[Candle],
        candles_5m: list[Candle],
        ltp: float | None,
        mark_price: float | None,
        liquidity_distance_pct: float | None,
        btc_macro: BTCMacroState,
        spread_pct: float | None,
        bias_label: str,
        regime_15m: dict[str, Any],
        regime_5m: dict[str, Any],
    ) -> dict[str, Any]:
        return self._evaluate_15m_setup_for_side(
            instrument=instrument,
            side="LONG",
            bias=bias,
            bias_ok=bias_ok,
            macro_ok=macro_ok,
            macro_msg=macro_msg,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            ltp=ltp,
            mark_price=mark_price,
            liquidity_distance_pct=liquidity_distance_pct,
            btc_macro=btc_macro,
            spread_pct=spread_pct,
            bias_label=bias_label,
            regime_15m=regime_15m,
            regime_5m=regime_5m,
        )

    def evaluate_short_setup(
        self,
        *,
        instrument: InstrumentInfo,
        bias: Bias,
        bias_ok: bool,
        macro_ok: bool,
        macro_msg: str,
        candles_15m: list[Candle],
        candles_5m: list[Candle],
        ltp: float | None,
        mark_price: float | None,
        liquidity_distance_pct: float | None,
        btc_macro: BTCMacroState,
        spread_pct: float | None,
        bias_label: str,
        regime_15m: dict[str, Any],
        regime_5m: dict[str, Any],
    ) -> dict[str, Any]:
        return self._evaluate_15m_setup_for_side(
            instrument=instrument,
            side="SHORT",
            bias=bias,
            bias_ok=bias_ok,
            macro_ok=macro_ok,
            macro_msg=macro_msg,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            ltp=ltp,
            mark_price=mark_price,
            liquidity_distance_pct=liquidity_distance_pct,
            btc_macro=btc_macro,
            spread_pct=spread_pct,
            bias_label=bias_label,
            regime_15m=regime_15m,
            regime_5m=regime_5m,
        )

    def _evaluate_15m_setup_for_side(
        self,
        *,
        instrument: InstrumentInfo,
        side: str,
        bias: Bias,
        bias_ok: bool,
        macro_ok: bool,
        macro_msg: str,
        candles_15m: list[Candle],
        candles_5m: list[Candle],
        ltp: float | None,
        mark_price: float | None,
        liquidity_distance_pct: float | None,
        btc_macro: BTCMacroState,
        spread_pct: float | None,
        bias_label: str,
        regime_15m: dict[str, Any],
        regime_5m: dict[str, Any],
    ) -> dict[str, Any]:
        _ = candles_5m  # reserved for future side-specific pre-entry checks
        expected_side = "LONG" if bias == "BULL" else "SHORT" if bias == "BEAR" else None
        base_meta = {
            "price": mark_price or ltp,
            "regime_15m": regime_15m,
            "regime_5m": regime_5m,
            "spread_pct": spread_pct,
            "bias_4h": bias_label,
        }

        def fail(
            *,
            rule: str,
            expected: str,
            actual: Any,
            message: str,
            rejection_code: str,
            delta: Any = None,
            extra_meta: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return {
                "candidate": None,
                "audit": {
                    "side": side,
                    "timeframe": "15m",
                    "passed": False,
                    "rule": rule,
                    "expected": expected,
                    "actual": actual,
                    "delta": delta,
                    "message": message,
                    "rejection_code": rejection_code,
                    "meta": {**base_meta, **(extra_meta or {})},
                },
            }

        if not bias_ok or bias == "NEUTRAL" or expected_side != side:
            return fail(
                rule="bias_alignment_4h",
                expected=f"4H bias aligned for {side}",
                actual=f"bias_4h={bias_label}",
                message="no_4h_bias_alignment",
                rejection_code="NO_4H_BIAS_ALIGNMENT",
            )

        if not macro_ok:
            return fail(
                rule="macro_gate",
                expected="BTC macro supportive or disabled",
                actual=macro_msg,
                message="macro_gate_blocked",
                rejection_code="EXECUTION_GATE_BLOCK",
            )

        if len(candles_15m) < max(40, self.config.strategy.indicators.ema_slow + 5):
            return fail(
                rule="candle_history",
                expected=f">= {max(40, self.config.strategy.indicators.ema_slow + 5)} candles",
                actual=f"count={len(candles_15m)}",
                message="insufficient_15m_candles",
                rejection_code="NO_STRUCTURE_BREAK",
            )

        idx = self._last_closed_index(candles_15m)
        if idx is None or idx < 20:
            return fail(
                rule="closed_candle",
                expected="latest 15m candle closed",
                actual="missing_closed_15m_candle",
                message="no_closed_15m_candle",
                rejection_code="NO_STRUCTURE_BREAK",
            )

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
            return fail(
                rule="atr_available",
                expected="ATR available",
                actual="atr_unavailable",
                message="atr_unavailable",
                rejection_code="LOW_ATR",
            )

        ef = float(ef_all[-1])
        es = float(es_all[-1])
        atr_val = max(1e-9, float(atr_all[-1]))
        lookback = self.config.strategy.setup.breakout_lookback_bars
        vol_lb = self.config.strategy.indicators.volume_ratio_lookback
        vol_avg = average(vols[-(vol_lb + 1) : -1]) if len(vols) > vol_lb else average(vols[:-1])
        vol_ratio = float(last.volume / max(1e-9, vol_avg)) if vol_avg > 0 else 1.0
        atr_pct = (atr_val / max(1e-9, last.close)) * 100.0

        max_spread_pct = 0.25
        if spread_pct is not None and spread_pct > max_spread_pct:
            return fail(
                rule="spread_filter",
                expected=f"spread_pct <= {max_spread_pct}",
                actual=f"spread_pct={round(float(spread_pct), 6)}",
                message="high_spread",
                rejection_code="HIGH_SPREAD",
                delta=round(float(spread_pct) - max_spread_pct, 6),
                extra_meta={"spread_limit_pct": max_spread_pct, "vol_ratio": round(vol_ratio, 4), "atr_pct": round(atr_pct, 6)},
            )

        if liquidity_distance_pct is not None and liquidity_distance_pct <= self.config.strategy.setup.liquidity_pool_block_distance_pct:
            return fail(
                rule="liquidity_wall_distance",
                expected=f"distance_pct > {self.config.strategy.setup.liquidity_pool_block_distance_pct}",
                actual=f"distance_pct={round(float(liquidity_distance_pct), 6)}",
                message="high_spread" if side == "LONG" else "high_spread",
                rejection_code="HIGH_SPREAD",
                delta=round(float(liquidity_distance_pct) - float(self.config.strategy.setup.liquidity_pool_block_distance_pct), 6),
                extra_meta={"distance_pct": liquidity_distance_pct, "vol_ratio": round(vol_ratio, 4), "atr_pct": round(atr_pct, 6)},
            )

        choices: list[SignalCandidate] = []
        failure_hint: dict[str, Any] | None = None
        meta_details: dict[str, Any] = {
            "vol_ratio": round(vol_ratio, 4),
            "atr_pct": round(atr_pct, 6),
            "side": side,
            "ema20": round(ef, 8),
            "ema50": round(es, 8),
            "price": round(float(last.close), 8),
        }

        if len(bars) >= lookback + 1:
            ref = bars[-(lookback + 1) : -1]
            level = max(c.high for c in ref) if side == "LONG" else min(c.low for c in ref)
            broke = last.close > level if side == "LONG" else last.close < level
            signed_delta = (last.close - level) if side == "LONG" else (level - last.close)
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
                meta_details["breakout_atr"] = round(breakout_atr, 4)
            else:
                if not broke:
                    failure_hint = failure_hint or {
                        "rule": "breakout_confirmation",
                        "expected": ("close > range_high" if side == "LONG" else "close < range_low"),
                        "actual": ("close below range_high" if side == "LONG" else "close above range_low"),
                        "delta": round(signed_delta, 6),
                        "message": "breakout_not_confirmed",
                        "rejection_code": "NO_STRUCTURE_BREAK",
                    }
                elif vol_ratio < self.config.strategy.setup.breakout_min_volume_ratio:
                    failure_hint = failure_hint or {
                        "rule": "volume_confirmation",
                        "expected": f"vol_ratio >= {self.config.strategy.setup.breakout_min_volume_ratio}",
                        "actual": f"vol_ratio={round(vol_ratio, 4)}",
                        "delta": round(vol_ratio - self.config.strategy.setup.breakout_min_volume_ratio, 6),
                        "message": "low_volume",
                        "rejection_code": "LOW_VOLUME",
                    }
                elif breakout_atr > self.config.strategy.setup.breakout_max_atr_distance:
                    failure_hint = failure_hint or {
                        "rule": "breakout_distance_atr",
                        "expected": f"breakout_atr <= {self.config.strategy.setup.breakout_max_atr_distance}",
                        "actual": f"breakout_atr={round(breakout_atr, 4)}",
                        "delta": round(breakout_atr - self.config.strategy.setup.breakout_max_atr_distance, 6),
                        "message": "no_structure_break",
                        "rejection_code": "NO_STRUCTURE_BREAK",
                    }
                elif extended_atr > self.config.strategy.setup.breakout_max_extension_from_ema20_atr:
                    failure_hint = failure_hint or {
                        "rule": "extension_from_ema20_atr",
                        "expected": f"extension_atr <= {self.config.strategy.setup.breakout_max_extension_from_ema20_atr}",
                        "actual": f"extension_atr={round(extended_atr, 4)}",
                        "delta": round(extended_atr - self.config.strategy.setup.breakout_max_extension_from_ema20_atr, 6),
                        "message": "no_structure_break",
                        "rejection_code": "NO_STRUCTURE_BREAK",
                    }

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
            elif failure_hint is None:
                failure_hint = {
                    "rule": "pullback_continuation",
                    "expected": "trend + pullback zone + reclaim + body/wick confirmation",
                    "actual": {
                        "trend_ok": trend_ok,
                        "pullback_zone": pullback_zone,
                        "reclaim": reclaim,
                        "body_wick_ok": body_wick_ok,
                    },
                    "delta": None,
                    "message": "no_structure_break",
                    "rejection_code": "NO_STRUCTURE_BREAK",
                }

        if not choices:
            hint = failure_hint or {
                "rule": "entry_structure",
                "expected": "breakout or pullback continuation",
                "actual": "no_valid_15m_setup",
                "delta": None,
                "message": "no_valid_15m_setup",
                "rejection_code": "NO_STRUCTURE_BREAK",
            }
            return fail(
                rule=str(hint.get("rule") or "entry_structure"),
                expected=str(hint.get("expected") or "breakout or pullback continuation"),
                actual=hint.get("actual"),
                message=str(hint.get("message") or "no_valid_15m_setup"),
                rejection_code=str(hint.get("rejection_code") or "NO_STRUCTURE_BREAK"),
                delta=hint.get("delta"),
                extra_meta=meta_details,
            )

        selected = max(choices, key=lambda c: c.score)
        selected_reason = "breakout_confirmation" if selected.setup == "BREAKOUT_CLOSE" else "pullback_continuation"
        return {
            "candidate": selected,
            "audit": {
                "side": side,
                "timeframe": "15m",
                "passed": True,
                "rule": selected_reason,
                "expected": "all entry filters pass",
                "actual": f"{selected.setup} selected",
                "delta": None,
                "message": "entry_eval_passed",
                "rejection_code": None,
                "meta": {
                    **base_meta,
                    **meta_details,
                    "selected_setup": selected.setup,
                    "score": round(float(selected.score), 4),
                    "signal_price": round(float(selected.entry_price), 8),
                },
            },
        }

    def _side_eval_stage(
        self,
        *,
        instrument: InstrumentInfo,
        cycle_id: str | None,
        bias_label: str,
        audit: dict[str, Any],
    ) -> StageRecord:
        return self._stage(
            instrument.pair,
            "ENTRY_EVAL",
            str(audit.get("expected") or "entry_eval"),
            audit.get("actual"),
            bool(audit.get("passed")),
            str(audit.get("message") or ""),
            timeframe=str(audit.get("timeframe") or "15m"),
            side=str(audit.get("side") or ""),
            bias_4h=bias_label,
            cycle_id=cycle_id,
            rule=str(audit.get("rule") or "entry_eval"),
            delta=audit.get("delta"),
            rejection_code=(None if audit.get("passed") else audit.get("rejection_code")),
            meta=(audit.get("meta") if isinstance(audit.get("meta"), dict) else {}),
        )

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
            stage_flow=["bias_4h", "macro_gate", "ENTRY_EVAL", "setup_candidate_15m"],
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
    def _bias_label(bias: Bias) -> str:
        return {"BULL": "BULLISH", "BEAR": "BEARISH", "NEUTRAL": "NEUTRAL"}.get(str(bias), str(bias))

    @staticmethod
    def _stage(
        symbol: str,
        stage: str,
        expected: str,
        actual: Any,
        passed: bool,
        message: str,
        *,
        rule: str | None = None,
        timeframe: str | None = None,
        side: str | None = None,
        bias_4h: str | None = None,
        rejection_code: str | None = None,
        cycle_id: str | None = None,
        delta: Any = None,
        meta: dict[str, Any] | None = None,
    ) -> StageRecord:
        return StageRecord(
            ts=utc_now_iso(),
            symbol=symbol,
            stage=stage,
            timeframe=timeframe,
            side=side,
            bias_4h=bias_4h,
            rejection_code=rejection_code,
            cycle_id=cycle_id,
            rule=rule or stage,
            expected=expected,
            actual=actual,
            delta=delta,
            passed=passed,
            message=message,
            meta=meta or {},
        )
