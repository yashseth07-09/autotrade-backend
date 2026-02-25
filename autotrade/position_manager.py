from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from autotrade.config import AppConfig
from autotrade.indicators import atr
from autotrade.models import BTCMacroState, Candle, Position
from autotrade.utils import utc_now_iso


@dataclass(slots=True)
class ManageDecision:
    action: str
    message: str
    new_stop: float | None = None
    exit_price: float | None = None
    exit_qty: float | None = None
    meta: dict[str, Any] | None = None


class PositionManager:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def evaluate(self, position: Position, candles_5m: list[Candle], btc_macro: BTCMacroState) -> list[ManageDecision]:
        if not candles_5m:
            return [ManageDecision("HOLD", "missing_5m_data")]
        current = candles_5m[-1]
        current_price = current.close
        position.mark_price = current_price
        position.ltp = current_price
        initial_risk = abs(position.entry_price - position.initial_stop_price)
        if initial_risk <= 0:
            return [ManageDecision("HOLD", "invalid_initial_risk")]

        # Stop-loss hit check (using candle extremes).
        if position.side == "LONG" and current.low <= position.stop_price:
            return [ManageDecision("EXIT", "stop_hit", exit_price=position.stop_price, exit_qty=position.remaining_qty, meta={"reason": "stop"})]
        if position.side == "SHORT" and current.high >= position.stop_price:
            return [ManageDecision("EXIT", "stop_hit", exit_price=position.stop_price, exit_qty=position.remaining_qty, meta={"reason": "stop"})]

        side_mult = 1.0 if position.side == "LONG" else -1.0
        progress_r = ((current_price - position.entry_price) * side_mult) / initial_risk
        actions: list[ManageDecision] = []

        # Dynamic reward policy.
        target_r = self._dynamic_target_r(position, btc_macro)
        target_price = position.entry_price + initial_risk * target_r * side_mult
        position.target_price = target_price

        # Partial at 1R.
        if not position.partial_taken and progress_r >= 1.0:
            partial_qty = max(0.0, position.remaining_qty * self.config.strategy.partial_take_pct)
            if partial_qty > 0:
                actions.append(ManageDecision("PARTIAL", "partial_take_1R", exit_price=current_price, exit_qty=partial_qty, meta={"progress_r": progress_r}))
                if self.config.strategy.break_even_after_partial:
                    actions.append(ManageDecision("UPDATE_STOP", "move_stop_to_breakeven", new_stop=position.entry_price))

        # Contradictory BTC macro: exit full at 1R.
        if btc_macro.enabled and not btc_macro.btc_profile_supportive and progress_r >= 1.0:
            actions.append(ManageDecision("EXIT", "btc_macro_contradictory_exit_1R", exit_price=current_price, exit_qty=position.remaining_qty))
            return actions

        # Pullback setups are capped near 1.5R by design.
        if position.setup == "PULLBACK_CONTINUATION" and progress_r >= 1.5:
            actions.append(ManageDecision("EXIT", "pullback_target_1_5R_hit", exit_price=current_price, exit_qty=position.remaining_qty))
            return actions

        # Conservative breakout exits if target profile is not runner mode.
        if position.setup == "BREAKOUT_CLOSE" and target_r <= 2.0 and progress_r >= target_r:
            actions.append(ManageDecision("EXIT", "breakout_target_hit", exit_price=current_price, exit_qty=position.remaining_qty, meta={"target_r": target_r}))
            return actions

        # 5m trailing stop.
        trail = self._trail_stop(position, candles_5m)
        if trail is not None:
            if position.side == "LONG" and trail > position.stop_price:
                actions.append(ManageDecision("UPDATE_STOP", "trail_stop_5m", new_stop=trail))
            elif position.side == "SHORT" and trail < position.stop_price:
                actions.append(ManageDecision("UPDATE_STOP", "trail_stop_5m", new_stop=trail))

        # Optional add rule; disabled by default for week-1.
        if (
            self.config.strategy.aggressive_adds_enabled
            and not self.config.strategy.conservative_adds_week1
            and not position.added_once
            and progress_r >= 0.5
            and btc_macro.btc_profile_supportive
        ):
            actions.append(ManageDecision("ADD", "add_25pct_condition_met", meta={"add_pct": 0.25, "progress_r": progress_r}))

        if not actions:
            actions.append(ManageDecision("HOLD", "managed_hold", meta={"progress_r": round(progress_r, 3), "target_r": target_r}))
        return actions

    def apply_partial(self, position: Position, qty: float, price: float) -> None:
        qty = min(max(0.0, qty), position.remaining_qty)
        if qty <= 0:
            return
        self._apply_pnl(position, qty, price)
        position.remaining_qty -= qty
        position.partial_taken = True
        position.status = "PARTIAL" if position.remaining_qty > 1e-12 else "CLOSED"
        position.updated_at = utc_now_iso()

    def close(self, position: Position, price: float, qty: float | None = None, reason: str | None = None) -> None:
        qty = position.remaining_qty if qty is None else min(max(0.0, qty), position.remaining_qty)
        if qty <= 0:
            return
        self._apply_pnl(position, qty, price)
        position.remaining_qty -= qty
        position.exit_price = price
        position.updated_at = utc_now_iso()
        if position.remaining_qty <= 1e-12:
            position.status = "CLOSED"
            position.closed_at = utc_now_iso()
        if reason:
            position.notes["exit_reason"] = reason

    def _apply_pnl(self, position: Position, qty: float, price: float) -> None:
        side_mult = 1.0 if position.side == "LONG" else -1.0
        pnl_usdt = (price - position.entry_price) * side_mult * qty
        initial_risk_total = abs(position.entry_price - position.initial_stop_price) * max(position.qty, 1e-12)
        position.pnl_usdt += pnl_usdt
        position.pnl_r += pnl_usdt / max(initial_risk_total, 1e-12)

    def _dynamic_target_r(self, position: Position, btc_macro: BTCMacroState) -> float:
        if position.setup == "PULLBACK_CONTINUATION":
            return 1.5
        if btc_macro.enabled and not btc_macro.btc_profile_supportive:
            return 1.0
        vol_ratio = float(position.notes.get("signal_volume_ratio", 1.0))
        if vol_ratio >= 1.5 and (not btc_macro.enabled or btc_macro.btc_profile_supportive):
            return 3.0
        return 2.0

    def _trail_stop(self, position: Position, candles_5m: list[Candle]) -> float | None:
        if len(candles_5m) < max(10, self.config.strategy.indicators.atr_period + 2):
            return None
        lookback = self.config.strategy.indicators.swing_lookback_5m
        if self.config.strategy.trail_mode.lower() == "atr":
            highs = [c.high for c in candles_5m]
            lows = [c.low for c in candles_5m]
            closes = [c.close for c in candles_5m]
            a = atr(highs, lows, closes, self.config.strategy.indicators.atr_period)
            if not a:
                return None
            return closes[-1] - a[-1] if position.side == "LONG" else closes[-1] + a[-1]
        recent = candles_5m[-(lookback + 1) : -1]
        if not recent:
            return None
        return min(c.low for c in recent) if position.side == "LONG" else max(c.high for c in recent)

