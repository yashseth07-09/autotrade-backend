from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


Bias = Literal["BULL", "BEAR", "NEUTRAL"]
Side = Literal["LONG", "SHORT"]
SetupType = Literal["BREAKOUT_CLOSE", "PULLBACK_CONTINUATION"]


class Candle(BaseModel):
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class MacroProfile(BaseModel):
    profile: str
    reason: str
    supportive: bool


class StageRecord(BaseModel):
    ts: str
    symbol: str
    stage: str
    timeframe: str | None = None
    side: str | None = None
    bias_4h: str | None = None
    rejection_code: str | None = None
    cycle_id: str | None = None
    rule: str
    expected: str
    actual: Any
    delta: Any = None
    passed: bool
    message: str = ""
    meta: dict[str, Any] = Field(default_factory=dict)


class InstrumentInfo(BaseModel):
    pair: str
    margin_currency: str
    status: str = "unknown"
    underlying: str | None = None
    quote: str | None = None
    price_increment: float | None = None
    quantity_increment: float | None = None
    min_quantity: float | None = None
    max_quantity: float | None = None
    min_trade_size: float | None = None
    max_leverage_long: float | None = None
    max_leverage_short: float | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class SignalCandidate(BaseModel):
    id: str
    symbol: str
    pair: str
    margin_currency: str = "USDT"
    side: Side
    setup: SetupType
    timeframe: str = "15m"
    entry_price: float
    stop_price: float
    invalidation_price: float
    atr: float
    score: float
    volume_ratio: float
    btc_profile: str
    btc_bias: Bias
    mark_price: float | None = None
    ltp: float | None = None
    signal_candle_close_ms: int | None = None
    execution_window_bars: int = 2
    reasons: list[str] = Field(default_factory=list)
    stage_flow: list[str] = Field(default_factory=list)
    created_at: str
    execution_gate_passed: bool = False
    execution_gate_failed: bool = False

    @property
    def risk_per_unit(self) -> float:
        return abs(self.entry_price - self.stop_price)


class Position(BaseModel):
    id: str
    symbol: str
    pair: str
    margin_currency: str = "USDT"
    side: Side
    setup: SetupType
    status: Literal["OPEN", "CLOSED", "PARTIAL"]
    qty: float
    remaining_qty: float
    leverage: int
    entry_price: float
    stop_price: float
    initial_stop_price: float
    target_price: float | None = None
    partial_taken: bool = False
    added_once: bool = False
    opened_at: str
    updated_at: str
    closed_at: str | None = None
    exit_price: float | None = None
    mark_price: float | None = None
    ltp: float | None = None
    liquidation_price: float | None = None
    pnl_usdt: float = 0.0
    pnl_r: float = 0.0
    notes: dict[str, Any] = Field(default_factory=dict)


class RiskMetrics(BaseModel):
    equity_usdt: float
    realized_pnl_usdt_today: float
    realized_pnl_r_today: float
    consecutive_losses: int
    in_cooldown: bool
    cooldown_until: str | None = None
    max_daily_loss_r: float = 2.0
    max_concurrent_trades: int = 0
    open_positions: int = 0
    can_trade: bool = True
    trade_block_reason: str | None = None
    cooldown_remaining_s: int | None = None
    daily_realized_pnl: float | None = None
    daily_R: float | None = None


class BTCMacroState(BaseModel):
    symbol: str = "BTCUSDT"
    enabled: bool = False
    provider: str = "none"
    bias_4h: Bias = "NEUTRAL"
    ema20_4h: float = 0.0
    ema50_4h: float = 0.0
    price_4h: float = 0.0
    oi_slope_4h: float = 0.0
    btc_profile_15m: str = "UNKNOWN"
    btc_profile_supportive: bool = False
    volume_ratio_15m: float = 0.0
    resistance_distance_pct: float | None = None
    liquidity_blocked_long: bool = False
    liquidity_blocked_short: bool = False
    updated_at: str = ""
    notes: list[str] = Field(default_factory=list)


class EventEnvelope(BaseModel):
    ts: str
    type: str
    payload: dict[str, Any]


class Snapshot(BaseModel):
    ts: str
    health: dict[str, Any]
    btc_macro: BTCMacroState
    market_regime: dict[str, Any] = Field(default_factory=dict)
    top_candidates: list[SignalCandidate] = Field(default_factory=list)
    open_positions: list[Position] = Field(default_factory=list)
    risk: RiskMetrics
    pnl: dict[str, Any] = Field(default_factory=dict)
    config_view: dict[str, Any] = Field(default_factory=dict)
    recent_events_tail: list[dict[str, Any]] = Field(default_factory=list)
    runtime: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    top_rejects: list[dict[str, Any]] = Field(default_factory=list)
