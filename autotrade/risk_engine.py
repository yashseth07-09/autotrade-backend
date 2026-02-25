from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from autotrade.config import AppConfig
from autotrade.models import Position, RiskMetrics
from autotrade.utils import utc_now, utc_now_iso


@dataclass
class RiskDecision:
    allowed: bool
    reason: str | None = None
    size_qty: float | None = None
    risk_amount_usdt: float | None = None
    leverage: int | None = None


class RiskEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._day_key: str | None = None
        self._realized_r_today: float = 0.0
        self._realized_usdt_today: float = 0.0
        self._consecutive_losses: int = 0
        self._cooldown_until: datetime | None = None

    def _reset_if_new_day(self) -> None:
        now = utc_now()
        key = now.strftime("%Y-%m-%d")
        if self._day_key != key:
            self._day_key = key
            self._realized_r_today = 0.0
            self._realized_usdt_today = 0.0
            self._consecutive_losses = 0
            self._cooldown_until = None

    def current_metrics(self, *, open_positions: int) -> RiskMetrics:
        self._reset_if_new_day()
        now = utc_now()
        in_cooldown = bool(self._cooldown_until and now < self._cooldown_until)
        cooldown_remaining_s = None
        if in_cooldown and self._cooldown_until is not None:
            cooldown_remaining_s = max(0, int((self._cooldown_until - now).total_seconds()))
        can_trade = True
        reason = None
        if self._realized_r_today <= -abs(self.config.strategy.max_daily_loss_r):
            can_trade = False
            reason = "max_daily_loss_reached"
        elif in_cooldown:
            can_trade = False
            reason = "cooldown_after_consecutive_losses"
        elif open_positions >= self.config.strategy.max_concurrent_trades:
            can_trade = False
            reason = "max_concurrent_trades_reached"

        return RiskMetrics(
            equity_usdt=self.config.strategy.account_equity_usdt + self._realized_usdt_today,
            realized_pnl_usdt_today=self._realized_usdt_today,
            realized_pnl_r_today=self._realized_r_today,
            consecutive_losses=self._consecutive_losses,
            in_cooldown=in_cooldown,
            cooldown_until=self._cooldown_until.isoformat() if self._cooldown_until else None,
            max_daily_loss_r=self.config.strategy.max_daily_loss_r,
            max_concurrent_trades=self.config.strategy.max_concurrent_trades,
            open_positions=open_positions,
            can_trade=can_trade,
            trade_block_reason=reason,
            cooldown_remaining_s=cooldown_remaining_s,
            daily_realized_pnl=self._realized_usdt_today,
            daily_R=self._realized_r_today,
        )

    def position_size_for_signal(
        self,
        *,
        entry_price: float,
        stop_price: float,
        neutral_btc_scale: float = 1.0,
    ) -> RiskDecision:
        self._reset_if_new_day()
        metrics = self.current_metrics(open_positions=0)
        if not metrics.can_trade:
            return RiskDecision(False, metrics.trade_block_reason)

        stop_distance = abs(entry_price - stop_price)
        if stop_distance <= 0:
            return RiskDecision(False, "invalid_stop_distance")

        equity = metrics.equity_usdt
        risk_pct = max(0.01, float(self.config.strategy.risk_per_trade_pct))
        risk_amount = equity * (risk_pct / 100.0)
        risk_amount *= max(0.0, neutral_btc_scale)
        if risk_amount <= 0:
            return RiskDecision(False, "risk_amount_zero_after_scaling")

        qty = risk_amount / stop_distance
        if qty <= 0:
            return RiskDecision(False, "computed_qty_non_positive")

        leverage = int(self.config.exchange.default_leverage)
        return RiskDecision(True, size_qty=qty, risk_amount_usdt=risk_amount, leverage=leverage)

    def register_trade_close(self, position: Position) -> None:
        self._reset_if_new_day()
        pnl_r = float(position.pnl_r or 0.0)
        pnl_usdt = float(position.pnl_usdt or 0.0)
        self._realized_r_today += pnl_r
        self._realized_usdt_today += pnl_usdt

        if pnl_r < 0:
            self._consecutive_losses += 1
        elif pnl_r > 0:
            self._consecutive_losses = 0

        if self._consecutive_losses >= self.config.strategy.cooldown_after_consecutive_losses:
            self._cooldown_until = utc_now() + timedelta(minutes=self.config.strategy.cooldown_minutes)

    def force_regime_cooldown(self, reason: str = "btc_regime_flip") -> dict[str, Any]:
        self._reset_if_new_day()
        # Short cooldown for regime flips to reduce immediate re-entry churn.
        self._cooldown_until = utc_now() + timedelta(minutes=min(10, self.config.strategy.cooldown_minutes))
        return {"reason": reason, "cooldown_until": self._cooldown_until.isoformat(), "ts": utc_now_iso()}

    def restore_daily_state(
        self,
        *,
        day_key: str,
        realized_usdt_today: float,
        realized_r_today: float,
        consecutive_losses: int,
        cooldown_until: str | None = None,
    ) -> None:
        self._day_key = day_key
        self._realized_usdt_today = float(realized_usdt_today or 0.0)
        self._realized_r_today = float(realized_r_today or 0.0)
        self._consecutive_losses = max(0, int(consecutive_losses or 0))
        self._cooldown_until = None
        if cooldown_until:
            try:
                dt = datetime.fromisoformat(str(cooldown_until).replace("Z", "+00:00"))
                self._cooldown_until = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
            except Exception:
                self._cooldown_until = None

    def export_state(self) -> dict[str, Any]:
        self._reset_if_new_day()
        return {
            "day_key": self._day_key,
            "daily_realized_pnl": self._realized_usdt_today,
            "daily_R": self._realized_r_today,
            "consecutive_losses": self._consecutive_losses,
            "cooldown_until_ts": self._cooldown_until.isoformat() if self._cooldown_until else None,
            "can_trade": self.current_metrics(open_positions=0).can_trade,
        }
