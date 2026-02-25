from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from autotrade.utils import resolve_env_placeholders


class RuntimeConfig(BaseModel):
    data_dir: str = "./data"
    snapshot_file: str = "latest_snapshot.json"
    events_file: str = "events.jsonl"
    trades_db: str = "trades.sqlite"
    state_resume_file: str = "state_resume.json"
    command_queue_file: str = "command_queue.jsonl"
    loop_interval_seconds: float = 10.0
    snapshot_interval_seconds: float = 2.0
    state_persist_interval_s: float = 2.0
    dry_run: bool = True


class ExchangeConfig(BaseModel):
    provider: str = "COINDCX_FUTURES"
    base_url: str = "https://api.coindcx.com"
    public_base_url: str = "https://public.coindcx.com"
    api_key_env: str = "COINDCX_API_KEY"
    api_secret_env: str = "COINDCX_API_SECRET"
    margin_currency: str = "USDT"
    default_leverage: int = 3
    auth_timeout_ms: int = 10_000
    timeout_seconds: float = 10.0
    rate_limit_backoff_seconds: float = 2.0
    use_btc_macro: bool = False
    btc_macro_provider: str = "BINANCE_FUTURES"
    btc_macro_base_url: str = "https://fapi.binance.com"
    btc_macro_oi_enabled: bool = True
    btc_macro_liquidity_enabled: bool = True


class TimeframeConfig(BaseModel):
    bias_tf: str = "4h"
    signal_tf: str = "15m"
    exec_tf: str = "5m"


class SetupConfig(BaseModel):
    breakout_lookback_bars: int = 10
    breakout_max_atr_distance: float = 1.0
    breakout_min_volume_ratio: float = 1.2
    breakout_max_extension_from_ema20_atr: float = 1.5
    pullback_zone_tolerance_atr: float = 0.2
    liquidity_pool_block_distance_pct: float = 0.3


class IndicatorConfig(BaseModel):
    ema_fast: int = 20
    ema_slow: int = 50
    atr_period: int = 14
    oi_slope_lookback: int = 6
    volume_ratio_lookback: int = 20
    resistance_lookback: int = 20
    swing_lookback_5m: int = 6


class StrategyConfig(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    watchlist_replace: list[str] = Field(default_factory=list)
    watchlist_size: int = 10
    watchlist_accept_exact_pairs: bool = True
    btc_symbol: str = "BTCUSDT"
    risk_per_trade_pct: float = 0.4
    account_equity_usdt: float = 1000.0
    max_daily_loss_r: float = 2.0
    max_concurrent_trades: int = 5
    cooldown_after_consecutive_losses: int = 3
    cooldown_minutes: int = 30
    reduce_trades_on_btc_neutral: bool = True
    btc_neutral_trade_multiplier: float = 0.5
    aggressive_adds_enabled: bool = True
    partial_take_pct: float = 0.5
    break_even_after_partial: bool = True
    trail_mode: str = "swing"
    min_signal_score: float = 0.65
    conservative_adds_week1: bool = True
    timeframe: TimeframeConfig = Field(default_factory=TimeframeConfig)
    setup: SetupConfig = Field(default_factory=SetupConfig)
    indicators: IndicatorConfig = Field(default_factory=IndicatorConfig)


class RiskConfig(BaseModel):
    default_stop_atr_multiple: float = 1.2
    slippage_bps: float = 5
    fee_bps_per_side: float = 4


class ObserverConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    snapshot_stale_after_seconds: int = 30
    event_stream_poll_ms: int = 250
    stream_heartbeat_seconds: float = 5.0
    max_trade_rows: int = 200
    max_events_limit: int = 500
    snapshot_cache_ttl_ms: int = 350
    enable_commands: bool = False
    cors_allowed_origins: list[str] = Field(default_factory=lambda: ["*"])
    cors_allow_credentials: bool = False


class AppConfig(BaseModel):
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    exchange: ExchangeConfig = Field(default_factory=ExchangeConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    observer: ObserverConfig = Field(default_factory=ObserverConfig)

    def runtime_paths(self, root: Path) -> dict[str, Path]:
        data_dir = (root / self.runtime.data_dir).resolve()
        return {
            "data_dir": data_dir,
            "snapshot": data_dir / self.runtime.snapshot_file,
            "events": data_dir / self.runtime.events_file,
            "trades_db": data_dir / self.runtime.trades_db,
            "state_resume": data_dir / self.runtime.state_resume_file,
            "command_queue": data_dir / self.runtime.command_queue_file,
        }

    def public_config_view(self) -> dict[str, Any]:
        data = self.model_dump()
        exchange = data.get("exchange", {})
        exchange.pop("api_key_env", None)
        exchange.pop("api_secret_env", None)
        exchange.pop("auth_timeout_ms", None)
        return data


def load_config(path: str | Path) -> AppConfig:
    path = Path(path)
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    resolved = resolve_env_placeholders(raw)
    return AppConfig.model_validate(resolved)
