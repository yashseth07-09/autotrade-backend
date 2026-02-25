from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from autotrade.config import AppConfig
from autotrade.models import BTCMacroState, Position, RiskMetrics, SignalCandidate, Snapshot
from autotrade.utils import utc_now, utc_now_iso


@dataclass
class RuntimeState:
    config: AppConfig
    session_id: str = field(default_factory=lambda: str(uuid4()))
    btc_macro: BTCMacroState = field(default_factory=BTCMacroState)
    signals: dict[str, SignalCandidate] = field(default_factory=dict)
    positions: dict[str, Position] = field(default_factory=dict)
    health: dict[str, Any] = field(default_factory=lambda: {"bot_running": False, "last_cycle_at": None, "cycle_errors": 0})
    pnl: dict[str, Any] = field(
        default_factory=lambda: {
            "realized_today_usdt": 0.0,
            "realized_today_r": 0.0,
            "unrealized_usdt": 0.0,
        }
    )
    risk_metrics: RiskMetrics | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(
        default_factory=lambda: {
            "last_http_error": None,
            "last_http_error_ts": None,
            "last_exchange_latency_ms": None,
        }
    )
    runtime_meta: dict[str, Any] = field(
        default_factory=lambda: {
            "dry_run": True,
            "cycle_ms": None,
            "last_cycle_ok": None,
            "last_cycle_error": None,
            "snapshot_age_s": 0.0,
        }
    )
    top_rejects: list[dict[str, Any]] = field(default_factory=list)

    def set_signal(self, signal: SignalCandidate) -> None:
        self.signals[signal.pair] = signal

    def clear_signal(self, symbol_or_pair: str) -> None:
        self.signals.pop(symbol_or_pair, None)

    def upsert_position(self, position: Position) -> None:
        self.positions[position.id] = position

    def remove_position(self, position_id: str) -> None:
        self.positions.pop(position_id, None)

    def build_snapshot(self, config_view: dict[str, Any], recent_events_tail: list[dict[str, Any]]) -> Snapshot:
        risk = self.risk_metrics or RiskMetrics(
            equity_usdt=self.config.strategy.account_equity_usdt,
            realized_pnl_usdt_today=float(self.pnl["realized_today_usdt"]),
            realized_pnl_r_today=float(self.pnl["realized_today_r"]),
            consecutive_losses=0,
            in_cooldown=False,
            max_daily_loss_r=self.config.strategy.max_daily_loss_r,
            max_concurrent_trades=self.config.strategy.max_concurrent_trades,
            open_positions=len(self.positions),
        )
        risk.cooldown_remaining_s = risk.cooldown_remaining_s
        risk.daily_realized_pnl = float(risk.realized_pnl_usdt_today)
        risk.daily_R = float(risk.realized_pnl_r_today)
        runtime = dict(self.runtime_meta)
        runtime.setdefault("dry_run", self.config.runtime.dry_run)
        runtime.setdefault("session_id", self.session_id)
        runtime.setdefault("started_at", self.health.get("started_at"))
        runtime["snapshot_age_s"] = 0.0
        metrics = dict(self.metrics)
        diagnostics = dict(self.diagnostics)
        return Snapshot(
            ts=utc_now_iso(),
            health=self.health,
            btc_macro=self.btc_macro,
            top_candidates=sorted(self.signals.values(), key=lambda s: s.score, reverse=True)[:20],
            open_positions=list(self.positions.values()),
            risk=risk,
            pnl=self.pnl,
            config_view=config_view,
            recent_events_tail=recent_events_tail[-100:],
            runtime=runtime,
            metrics=metrics,
            diagnostics=diagnostics,
            top_rejects=self.top_rejects[:3],
        )

    def add_top_reject(self, item: dict[str, Any], max_items: int = 20) -> None:
        item = {"ts": utc_now_iso(), **item}
        self.top_rejects = [item, *self.top_rejects]
        if len(self.top_rejects) > max_items:
            self.top_rejects = self.top_rejects[:max_items]

    def refresh_runtime_snapshot_age(self) -> None:
        ts_raw = self.runtime_meta.get("snapshot_ts")
        if not ts_raw:
            self.runtime_meta["snapshot_age_s"] = 0.0
            return
        try:
            ts = ts_raw if hasattr(ts_raw, "tzinfo") else None
            if ts is None:
                # Stored as ISO string in most paths.
                from datetime import datetime

                dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                ts = dt if dt.tzinfo else dt.replace(tzinfo=utc_now().tzinfo)
            self.runtime_meta["snapshot_age_s"] = max(0.0, (utc_now() - ts).total_seconds())
        except Exception:
            self.runtime_meta["snapshot_age_s"] = 0.0
