from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from autotrade.config import AppConfig, load_config
from autotrade.execution_engine import CoinDCXFuturesBroker, ExecutionEngine
from autotrade.exchanges.binance_macro_provider import BinanceMacroProvider
from autotrade.exchanges.coindcx_futures_client import CoinDCXAuth, CoinDCXFuturesClient
from autotrade.exchanges.instrument_resolver import InstrumentResolver
from autotrade.market_data_provider import MarketDataProvider, SymbolMarketBundle
from autotrade.models import BTCMacroState, Position, StageRecord
from autotrade.persistence.event_logger import EventLogger
from autotrade.persistence.state_resume_store import StateResumeStore
from autotrade.persistence.snapshot_writer import SnapshotWriter
from autotrade.persistence.trade_store import TradeStore
from autotrade.position_manager import PositionManager
from autotrade.risk_engine import RiskEngine
from autotrade.runtime.command_queue import FileCommandQueue
from autotrade.runtime.state import RuntimeState
from autotrade.settings import RuntimeSettings, apply_env_overrides
from autotrade.strategy_engine import BTCMacroEngine, StrategyEngine
from autotrade.utils import ensure_dir, utc_now_iso


def _quantize(value: float, step: float | None) -> float:
    if value <= 0:
        return 0.0
    if not step or step <= 0:
        return value
    n = int(value / step)
    return n * step


def _safe_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


class BotEngine:
    def __init__(self, config: AppConfig, root: Path, *, settings: RuntimeSettings | None = None) -> None:
        self.config = config
        self.root = root
        self.settings = settings
        paths = config.runtime_paths(root)
        ensure_dir(paths["data_dir"])
        self.paths = paths

        self.state = RuntimeState(config=config)
        self.state.runtime_meta["dry_run"] = config.runtime.dry_run
        max_bytes = None
        max_rotations = 5
        if settings is not None:
            max_bytes = max(1, int(settings.max_events_mb)) * 1024 * 1024
            max_rotations = max(1, int(settings.max_event_rotations))
        self.logger = EventLogger(paths["events"], max_bytes=max_bytes, max_rotations=max_rotations)
        self.snapshot_writer = SnapshotWriter(paths["snapshot"])
        self.state_store = StateResumeStore(paths["state_resume"])
        self.trade_store = TradeStore(paths["trades_db"], root / "sql" / "schema.sql")
        self.command_queue = FileCommandQueue(paths["command_queue"])

        api_key = os.getenv(config.exchange.api_key_env, "")
        api_secret = os.getenv(config.exchange.api_secret_env, "")
        auth = CoinDCXAuth(api_key=api_key, api_secret=api_secret) if (api_key and api_secret) else None
        self.coindcx_client = CoinDCXFuturesClient(
            api_base_url=config.exchange.base_url,
            public_base_url=config.exchange.public_base_url,
            timeout_seconds=config.exchange.timeout_seconds,
            rate_limit_backoff_seconds=config.exchange.rate_limit_backoff_seconds,
            auth=auth,
            diagnostics_callback=self._on_coindcx_diag,
        )
        self.instrument_resolver = InstrumentResolver(self.coindcx_client, margin_currency=config.exchange.margin_currency)
        self.market_data = MarketDataProvider(self.coindcx_client)
        self.strategy = StrategyEngine(config)
        self.btc_macro_engine = BTCMacroEngine(config)
        self.risk_engine = RiskEngine(config)
        self.position_manager = PositionManager(config)

        broker = None if config.runtime.dry_run else CoinDCXFuturesBroker(self.coindcx_client)
        self.execution = ExecutionEngine(config, broker=broker, dry_run=config.runtime.dry_run)

        self.binance_macro: BinanceMacroProvider | None = None
        if config.exchange.use_btc_macro:
            self.binance_macro = BinanceMacroProvider(config.exchange.btc_macro_base_url, config.exchange.timeout_seconds)

        self._shutdown = asyncio.Event()
        self._executed_signal_keys: set[str] = set()
        self._last_btc_bias: str | None = None
        self._last_state_persist_monotonic: float = 0.0

    async def _on_coindcx_diag(self, payload: dict[str, Any]) -> None:
        kind = str(payload.get("kind") or "").upper()
        latency = _safe_float(payload.get("latency_ms"))
        if latency is not None:
            self.state.diagnostics["last_exchange_latency_ms"] = latency
        if kind == "HTTP_ERROR":
            self.state.diagnostics["last_http_error"] = {
                "url": payload.get("url"),
                "status": payload.get("status"),
                "exception": payload.get("exception"),
                "retry_count": payload.get("retry_count"),
            }
            self.state.diagnostics["last_http_error_ts"] = utc_now_iso()
            await self.logger.emit(
                "DIAG_HTTP_ERROR",
                {
                    "url": payload.get("url"),
                    "status": payload.get("status"),
                    "exception": payload.get("exception"),
                    "retry_count": payload.get("retry_count"),
                    "latency_ms": payload.get("latency_ms"),
                },
            )

    async def _restore_runtime_state_on_startup(self) -> None:
        resume = await self.state_store.load()
        await self._emit_file_staleness_diagnostics()
        restored_positions = 0
        prev_session_id = None
        if resume:
            prev_session_id = resume.get("session_id")
            if isinstance(resume.get("last_cycle"), dict):
                last_cycle = resume["last_cycle"]
                self.state.runtime_meta["cycle_ms"] = last_cycle.get("cycle_ms")
                self.state.runtime_meta["last_cycle_ok"] = last_cycle.get("last_cycle_ok")
                self.state.runtime_meta["last_cycle_error"] = last_cycle.get("last_cycle_error")
            if isinstance(resume.get("diagnostics"), dict):
                self.state.diagnostics.update(resume["diagnostics"])
            if isinstance(resume.get("last_signal_summary"), dict):
                top_rejects = resume.get("top_rejects")
                if isinstance(top_rejects, list):
                    self.state.top_rejects = [x for x in top_rejects if isinstance(x, dict)][:20]

        # Rebuild day metrics from sqlite (source of truth) before trading resumes.
        metrics = await self.trade_store.daily_metrics()
        self.state.metrics.update(metrics)
        day_start = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        cooldown_until = None
        if resume and isinstance(resume.get("risk_state"), dict):
            cooldown_until = resume["risk_state"].get("cooldown_until_ts")
        self.risk_engine.restore_daily_state(
            day_key=datetime.now(tz=UTC).strftime("%Y-%m-%d"),
            realized_usdt_today=float(metrics.get("realized_today") or 0.0),
            realized_r_today=float(metrics.get("realized_today_r") or 0.0),
            consecutive_losses=int(metrics.get("consecutive_losses") or 0),
            cooldown_until=str(cooldown_until) if cooldown_until else None,
        )
        self.state.pnl["realized_today_usdt"] = float(metrics.get("realized_today") or 0.0)
        self.state.pnl["realized_today_r"] = float(metrics.get("realized_today_r") or 0.0)

        if self.config.runtime.dry_run:
            restored_positions = await self._restore_dry_run_positions_from_resume(resume)
        else:
            restored_positions = await self._reconcile_live_positions(resume)

        await self.logger.emit(
            "RECOVERY_RECONCILE",
            {
                "mode": "dry_run" if self.config.runtime.dry_run else "live",
                "resume_loaded": bool(resume),
                "previous_session_id": prev_session_id,
                "current_session_id": self.state.session_id,
                "restored_positions": restored_positions,
                "risk_state": {
                    "daily_realized_pnl": metrics.get("realized_today"),
                    "daily_R": metrics.get("realized_today_r"),
                    "trades_today": metrics.get("trades_today"),
                    "win_rate_today": metrics.get("win_rate_today"),
                    "max_consecutive_losses_today": metrics.get("max_consecutive_losses_today"),
                    "day_start_ts": day_start,
                },
            },
        )

    async def _restore_dry_run_positions_from_resume(self, resume: dict[str, Any] | None) -> int:
        if not resume:
            return 0
        items = resume.get("open_positions")
        if not isinstance(items, list):
            return 0
        count = 0
        for raw in items:
            if not isinstance(raw, dict):
                continue
            try:
                pos = Position.model_validate(raw)
            except Exception:
                continue
            if pos.status == "CLOSED":
                continue
            self.state.upsert_position(pos)
            count += 1
        return count

    async def _reconcile_live_positions(self, resume: dict[str, Any] | None) -> int:
        file_positions: dict[str, Position] = {}
        if resume and isinstance(resume.get("open_positions"), list):
            for raw in resume["open_positions"]:
                if not isinstance(raw, dict):
                    continue
                try:
                    p = Position.model_validate(raw)
                    if p.status != "CLOSED":
                        file_positions[p.pair] = p
                except Exception:
                    continue

        restored = 0
        try:
            exchange_positions = await self.execution.broker.get_open_positions() if self.execution.broker else []
        except Exception as exc:
            await self.logger.emit("RECOVERY_RECONCILE", {"error": str(exc), "mode": "live", "step": "list_open_positions"})
            return 0

        seen_pairs: set[str] = set()
        for raw in exchange_positions:
            if not isinstance(raw, dict):
                continue
            pos = self._position_from_exchange(raw, file_positions.get(str(raw.get("pair") or raw.get("symbol") or "")))
            if pos is None:
                continue
            seen_pairs.add(pos.pair)
            self.state.upsert_position(pos)
            await self.trade_store.upsert_open_position(pos)
            restored += 1
            await self.logger.emit("RECOVERY_RECONCILE", {"action": "import_or_restore_open_position", "pair": pos.pair, "position_id": pos.id})

        for pair, pos in file_positions.items():
            if pair in seen_pairs:
                continue
            pos.status = "CLOSED"
            pos.closed_at = utc_now_iso()
            pos.updated_at = pos.closed_at
            pos.notes["recovery_reconcile"] = "file_position_missing_on_exchange_marked_closed"
            await self.trade_store.close_position(pos)
            await self.trade_store.record_trade_event(pos.id, "RECOVERY_RECONCILE", {"action": "mark_closed_missing_on_exchange", "pair": pair})
            await self.logger.emit("RECOVERY_RECONCILE", {"action": "mark_closed_missing_on_exchange", "pair": pair, "position_id": pos.id})
        return restored

    def _position_from_exchange(self, raw: dict[str, Any], existing: Position | None) -> Position | None:
        if not isinstance(raw, dict):
            return None
        pair = str(raw.get("pair") or raw.get("symbol") or raw.get("market") or "").strip()
        if not pair:
            return None
        qty_val = _safe_float(
            raw.get("total_quantity")
            or raw.get("quantity")
            or raw.get("qty")
            or raw.get("size")
            or raw.get("position_size")
        )
        if qty_val is None:
            return None
        qty = abs(float(qty_val))
        if qty <= 0:
            return None
        side_raw = str(raw.get("side") or raw.get("position_side") or "").upper()
        if not side_raw:
            side_raw = "LONG" if qty_val >= 0 else "SHORT"
        side = "LONG" if "BUY" in side_raw or "LONG" in side_raw else "SHORT"
        entry = _safe_float(raw.get("entry_price") or raw.get("avg_entry_price") or raw.get("average_price")) or 0.0
        mark = _safe_float(raw.get("mark_price") or raw.get("ltp") or raw.get("last_price"))
        liq = _safe_float(raw.get("liquidation_price"))
        lev = int(_safe_float(raw.get("leverage")) or self.config.exchange.default_leverage)
        now = utc_now_iso()
        if existing is not None:
            existing.qty = qty
            existing.remaining_qty = min(existing.remaining_qty or qty, qty) if existing.remaining_qty else qty
            existing.leverage = lev
            if entry > 0:
                existing.entry_price = entry
            existing.mark_price = mark
            existing.ltp = mark
            existing.liquidation_price = liq
            existing.updated_at = now
            existing.notes["management_context"] = self._management_context_for_position(existing)
            return existing
        stop = entry * (0.99 if side == "LONG" else 1.01) if entry > 0 else entry
        pos = Position(
            id=str(raw.get("id") or raw.get("position_id") or f"recovered:{pair}:{side}"),
            symbol=str(raw.get("symbol") or pair),
            pair=pair,
            margin_currency=self.config.exchange.margin_currency,
            side=side,  # type: ignore[arg-type]
            setup="BREAKOUT_CLOSE",
            status="OPEN",
            qty=qty,
            remaining_qty=qty,
            leverage=lev,
            entry_price=entry,
            stop_price=stop,
            initial_stop_price=stop,
            target_price=None,
            partial_taken=False,
            added_once=False,
            opened_at=str(raw.get("created_at") or raw.get("opened_at") or now),
            updated_at=now,
            mark_price=mark,
            ltp=mark,
            liquidation_price=liq,
            notes={"recovered_from_exchange": True},
        )
        pos.notes["management_context"] = self._management_context_for_position(pos)
        return pos

    async def _emit_file_staleness_diagnostics(self) -> None:
        now = datetime.now(tz=UTC).timestamp()
        for key in ("snapshot", "events", "trades_db"):
            path = self.paths.get(key)
            if not path or not path.exists():
                continue
            try:
                age_s = max(0.0, now - path.stat().st_mtime)
            except Exception:
                continue
            if age_s <= float(self.config.observer.snapshot_stale_after_seconds):
                continue
            await self.logger.emit(
                "DIAG_DATA_STALE",
                {"file": key, "path": str(path), "age_s": round(age_s, 2), "mtime": datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()},
            )

    def _management_context_for_position(self, position: Position) -> dict[str, Any]:
        return {
            "trail_mode": self.config.strategy.trail_mode,
            "last_trail_price": position.stop_price,
            "last_swing": position.notes.get("last_swing"),
            "BE_moved": abs(float(position.stop_price) - float(position.entry_price)) < 1e-12,
            "invalidation_price": float(position.initial_stop_price),
            "1R_taken": bool(position.partial_taken),
        }

    def _build_resume_state_payload(self) -> dict[str, Any]:
        open_positions = []
        management_context: dict[str, Any] = {}
        for pos in self.state.positions.values():
            if pos.status == "CLOSED":
                continue
            p = pos.model_dump()
            open_positions.append(p)
            management_context[pos.id] = self._management_context_for_position(pos)

        top_candidates = [
            {
                "pair": s.pair,
                "symbol": s.symbol,
                "side": s.side,
                "setup": s.setup,
                "score": s.score,
                "reasons": list(s.reasons),
                "created_at": s.created_at,
            }
            for s in sorted(self.state.signals.values(), key=lambda x: x.score, reverse=True)[:10]
        ]
        self.state.metrics.setdefault("trades_today", 0)
        risk_export = self.risk_engine.export_state()
        risk_export["day_start_ts"] = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        risk_export["daily_realized_pnl"] = self.state.metrics.get("realized_today", risk_export.get("daily_realized_pnl", 0.0))
        risk_export["daily_R"] = self.state.metrics.get("realized_today_r", risk_export.get("daily_R", 0.0))
        payload = {
            "session_id": self.state.session_id,
            "started_at": self.state.health.get("started_at"),
            "open_positions": open_positions,
            "management_context": management_context,
            "risk_state": risk_export,
            "last_cycle": {
                "cycle_ms": self.state.runtime_meta.get("cycle_ms"),
                "last_cycle_ok": self.state.runtime_meta.get("last_cycle_ok"),
                "last_cycle_error": self.state.runtime_meta.get("last_cycle_error"),
                "last_cycle_at": self.state.health.get("last_cycle_at"),
            },
            "last_signal_summary": {"top_candidates": top_candidates},
            "top_rejects": self.state.top_rejects[:10],
            "diagnostics": self.state.diagnostics,
        }
        return payload

    async def _persist_runtime_state_if_due(self, force: bool = False) -> None:
        now_m = asyncio.get_running_loop().time()
        if not force and (now_m - self._last_state_persist_monotonic) < float(self.config.runtime.state_persist_interval_s):
            return
        await self.state_store.write(self._build_resume_state_payload())
        self._last_state_persist_monotonic = now_m

    def _capture_reject_from_stage(self, rec: StageRecord) -> None:
        if rec.passed:
            return
        if rec.stage not in {"signal_confirmed_15m_close", "setup_candidate_15m", "execution_gate_5m"}:
            return
        reason = rec.message or str(rec.actual.get("message") if isinstance(rec.actual, dict) else "")
        self.state.add_top_reject(
            {
                "symbol": rec.symbol,
                "stage": rec.stage,
                "reason": reason,
                "actual": rec.actual,
                "passed": rec.passed,
            }
        )

    async def start(self) -> None:
        self.state.health["bot_running"] = True
        self.state.health["started_at"] = utc_now_iso()
        await self.trade_store.initialize()
        await self.logger.start()
        await self.logger.emit(
            "BOT_START",
            {
                "mode": "dry_run" if self.config.runtime.dry_run else "live",
                "exchange": self.config.exchange.provider,
                "margin_currency": self.config.exchange.margin_currency,
                "data_paths": {k: str(v) for k, v in self.paths.items()},
            },
        )
        await self._restore_runtime_state_on_startup()
        self.state.risk_metrics = self.risk_engine.current_metrics(
            open_positions=len([p for p in self.state.positions.values() if p.status != "CLOSED"])
        )
        await self._write_snapshot()
        await self._persist_runtime_state_if_due(force=True)

    async def stop(self) -> None:
        self.state.health["bot_running"] = False
        await self.logger.emit("BOT_STOP", {"ts": utc_now_iso()})
        await self._write_snapshot()
        await self._persist_runtime_state_if_due(force=True)
        await self.logger.stop()
        if self.binance_macro is not None:
            await self.binance_macro.aclose()
        await self.coindcx_client.aclose()

    async def run_forever(self) -> None:
        await self.start()
        try:
            while not self._shutdown.is_set():
                start = asyncio.get_running_loop().time()
                try:
                    await self.run_cycle()
                    self.state.health["last_cycle_ok"] = True
                    self.state.health["last_cycle_error"] = None
                    self.state.runtime_meta["last_cycle_ok"] = True
                    self.state.runtime_meta["last_cycle_error"] = None
                except Exception as exc:
                    self.state.health["cycle_errors"] = int(self.state.health.get("cycle_errors", 0)) + 1
                    self.state.health["last_cycle_ok"] = False
                    self.state.health["last_cycle_error"] = str(exc)
                    self.state.runtime_meta["last_cycle_ok"] = False
                    self.state.runtime_meta["last_cycle_error"] = str(exc)
                    await self.logger.emit("CYCLE_ERROR", {"error": str(exc)})
                self.state.health["last_cycle_at"] = utc_now_iso()
                cycle_elapsed = asyncio.get_running_loop().time() - start
                self.state.runtime_meta["cycle_ms"] = round(cycle_elapsed * 1000.0, 2)
                self.state.risk_metrics = self.risk_engine.current_metrics(
                    open_positions=len([p for p in self.state.positions.values() if p.status != "CLOSED"])
                )
                await self._write_snapshot()
                await self._persist_runtime_state_if_due()
                elapsed = asyncio.get_running_loop().time() - start
                await asyncio.sleep(max(0.1, float(self.config.runtime.loop_interval_seconds) - elapsed))
        finally:
            await self.stop()

    async def shutdown(self) -> None:
        self._shutdown.set()

    async def run_cycle(self) -> None:
        instruments = await self.instrument_resolver.resolve_watchlist(
            requested=self.config.strategy.symbols,
            replacements=self.config.strategy.watchlist_replace,
            max_size=self.config.strategy.watchlist_size,
        )
        if not instruments:
            await self.logger.emit("NO_WATCHLIST_INSTRUMENTS", {"reason": "resolver_returned_empty"})
            return

        btc_macro = await self._compute_btc_macro()
        if self._last_btc_bias and self._last_btc_bias != btc_macro.bias_4h:
            regime_evt = self.risk_engine.force_regime_cooldown("btc_regime_flip")
            await self.logger.emit("BTC_REGIME_FLIP", {"from": self._last_btc_bias, "to": btc_macro.bias_4h, **regime_evt})
            self._tighten_stops_on_regime_flip()
        self._last_btc_bias = btc_macro.bias_4h
        self.state.btc_macro = btc_macro

        tasks = [asyncio.create_task(self.market_data.fetch_symbol_bundle(instr)) for instr in instruments]
        bundles: list[SymbolMarketBundle] = []
        for t in tasks:
            try:
                bundles.append(await t)
            except Exception as exc:
                await self.logger.emit("MARKET_DATA_ERROR", {"error": str(exc)})

        bundle_by_pair = {b.instrument.pair: b for b in bundles}
        for bundle in bundles:
            await self._process_symbol_bundle(bundle, btc_macro)

        await self._manage_positions(bundle_by_pair, btc_macro)

        if self.config.observer.enable_commands:
            # Future phase: command polling and risk/idempotency validation.
            _ = self.command_queue  # keep interface wired without execution.

    async def _process_symbol_bundle(self, bundle: SymbolMarketBundle, btc_macro: BTCMacroState) -> None:
        ltp = bundle.orderbook.ltp
        mark = bundle.orderbook.mark_price
        liq_long = self.market_data.liquidity_block_distance_pct(bundle.orderbook, (mark or ltp or 0.0), "LONG")
        liq_short = self.market_data.liquidity_block_distance_pct(bundle.orderbook, (mark or ltp or 0.0), "SHORT")

        candidate, stages = self.strategy.evaluate_symbol(
            instrument=bundle.instrument,
            candles_4h=bundle.candles_4h,
            candles_15m=bundle.candles_15m,
            candles_5m=bundle.candles_5m,
            ltp=ltp,
            mark_price=mark,
            liquidity_distance_long_pct=liq_long,
            liquidity_distance_short_pct=liq_short,
            btc_macro=btc_macro,
        )
        for rec in stages:
            await self.logger.stage(rec)
            self._capture_reject_from_stage(rec)

        if candidate is None:
            self.state.clear_signal(bundle.instrument.pair)
            return

        self.state.set_signal(candidate)
        if candidate.execution_gate_failed:
            return
        if not candidate.execution_gate_passed:
            return

        if any(p.pair == candidate.pair and p.status != "CLOSED" for p in self.state.positions.values()):
            await self.logger.stage(
                StageRecord(
                    ts=utc_now_iso(),
                    symbol=candidate.pair,
                    stage="entered",
                    rule="skip_duplicate_pair_position",
                    expected="No open position on pair",
                    actual={"pair": candidate.pair},
                    passed=False,
                    message="position_already_open_for_pair",
                )
            )
            return

        signal_key = f"{candidate.pair}:{candidate.setup}:{candidate.signal_candle_close_ms}"
        if signal_key in self._executed_signal_keys:
            return

        metrics = self.risk_engine.current_metrics(open_positions=len([p for p in self.state.positions.values() if p.status != "CLOSED"]))
        self.state.risk_metrics = metrics
        if not metrics.can_trade:
            await self.logger.stage(
                StageRecord(
                    ts=utc_now_iso(),
                    symbol=candidate.pair,
                    stage="entered",
                    rule="risk_gate",
                    expected="risk controls allow new trade",
                    actual={"can_trade": metrics.can_trade, "reason": metrics.trade_block_reason},
                    passed=False,
                    message="risk_gate_blocked",
                )
            )
            return

        neutral_scale = 1.0
        if self.config.exchange.use_btc_macro and btc_macro.bias_4h == "NEUTRAL" and self.config.strategy.reduce_trades_on_btc_neutral:
            neutral_scale = max(0.0, float(self.config.strategy.btc_neutral_trade_multiplier))

        size_decision = self.risk_engine.position_size_for_signal(
            entry_price=candidate.entry_price,
            stop_price=candidate.stop_price,
            neutral_btc_scale=neutral_scale,
        )
        if not size_decision.allowed or not size_decision.size_qty or not size_decision.leverage:
            await self.logger.stage(
                StageRecord(
                    ts=utc_now_iso(),
                    symbol=candidate.pair,
                    stage="entered",
                    rule="risk_position_sizing",
                    expected="valid size quantity",
                    actual={"allowed": size_decision.allowed, "reason": size_decision.reason},
                    passed=False,
                    message="position_size_rejected",
                )
            )
            return

        qty = _quantize(size_decision.size_qty, bundle.instrument.quantity_increment)
        if bundle.instrument.min_quantity and qty < bundle.instrument.min_quantity:
            await self.logger.stage(
                StageRecord(
                    ts=utc_now_iso(),
                    symbol=candidate.pair,
                    stage="entered",
                    rule="instrument_min_qty",
                    expected=f"qty >= {bundle.instrument.min_quantity}",
                    actual={"qty": qty},
                    passed=False,
                    message="qty_below_min_instrument_limit",
                )
            )
            return
        if bundle.instrument.max_quantity and qty > bundle.instrument.max_quantity:
            qty = _quantize(bundle.instrument.max_quantity, bundle.instrument.quantity_increment)
        if bundle.instrument.min_trade_size and (qty * candidate.entry_price) < bundle.instrument.min_trade_size:
            await self.logger.stage(
                StageRecord(
                    ts=utc_now_iso(),
                    symbol=candidate.pair,
                    stage="entered",
                    rule="instrument_min_notional",
                    expected=f"notional >= {bundle.instrument.min_trade_size}",
                    actual={"qty": qty, "entry_price": candidate.entry_price, "notional": qty * candidate.entry_price},
                    passed=False,
                    message="notional_below_min_instrument_limit",
                )
            )
            return

        result = await self.execution.enter_from_signal(candidate, qty=qty, leverage=size_decision.leverage)
        if not result.accepted or result.position is None:
            await self.logger.emit("ORDER_REJECTED", {"pair": candidate.pair, "message": result.message, "payload": result.order_payload})
            return

        self._executed_signal_keys.add(signal_key)
        pos = result.position
        self.state.upsert_position(pos)
        pos.notes["management_context"] = self._management_context_for_position(pos)
        await self.trade_store.upsert_open_position(pos)
        await self.trade_store.record_trade_event(pos.id, "ENTER", result.order_payload)
        await self.logger.stage(
            StageRecord(
                ts=utc_now_iso(),
                symbol=candidate.pair,
                stage="entered",
                rule="execution_gate + risk + broker",
                expected="Order accepted and position opened",
                actual={"qty": qty, "entry_price": pos.entry_price, "leverage": pos.leverage, "margin_currency": pos.margin_currency},
                passed=True,
                message="position_opened",
            )
        )

    async def _manage_positions(self, bundles: dict[str, SymbolMarketBundle], btc_macro: BTCMacroState) -> None:
        unrealized = 0.0
        for position in list(self.state.positions.values()):
            if position.status == "CLOSED":
                continue
            bundle = bundles.get(position.pair)
            if bundle is None:
                instr = await self.instrument_resolver.get_instrument(position.pair)
                if instr is None:
                    await self.logger.emit("POSITION_MANAGE_SKIP", {"position_id": position.id, "pair": position.pair, "reason": "instrument_missing"})
                    continue
                try:
                    bundle = await self.market_data.fetch_symbol_bundle(instr)
                except Exception as exc:
                    await self.logger.emit("POSITION_MANAGE_SKIP", {"position_id": position.id, "pair": position.pair, "reason": str(exc)})
                    continue

            decisions = self.position_manager.evaluate(position, bundle.candles_5m, btc_macro)
            for d in decisions:
                if d.action == "HOLD":
                    await self.logger.stage(
                        StageRecord(
                            ts=utc_now_iso(),
                            symbol=position.pair,
                            stage="manage",
                            rule="5m_position_manager",
                            expected="Manage position per 1R/BE/trail rules",
                            actual=d.meta or {"message": d.message},
                            passed=True,
                            message=d.message,
                        )
                    )
                    continue

                if d.action == "UPDATE_STOP" and d.new_stop is not None:
                    position.stop_price = float(d.new_stop)
                    position.updated_at = utc_now_iso()
                    position.notes["management_context"] = self._management_context_for_position(position)
                    await self.trade_store.upsert_open_position(position)
                    await self.trade_store.record_trade_event(position.id, "STOP_UPDATE", {"stop_price": position.stop_price, "message": d.message})
                    await self.logger.stage(
                        StageRecord(
                            ts=utc_now_iso(),
                            symbol=position.pair,
                            stage="manage",
                            rule="trail_or_breakeven",
                            expected="stop tightens per rules",
                            actual={"new_stop": position.stop_price, **(d.meta or {})},
                            passed=True,
                            message=d.message,
                        )
                    )
                    continue

                if d.action == "PARTIAL" and d.exit_price is not None and d.exit_qty:
                    await self.execution.exit_position_market(position, qty=d.exit_qty)
                    self.position_manager.apply_partial(position, qty=d.exit_qty, price=d.exit_price)
                    position.notes["management_context"] = self._management_context_for_position(position)
                    await self.trade_store.upsert_open_position(position)
                    await self.trade_store.record_trade_event(position.id, "PARTIAL", {"qty": d.exit_qty, "price": d.exit_price, "message": d.message})
                    await self.logger.stage(
                        StageRecord(
                            ts=utc_now_iso(),
                            symbol=position.pair,
                            stage="manage",
                            rule="partial_at_1R",
                            expected="Take 40-60% at 1R and optionally move BE",
                            actual={"qty": d.exit_qty, "price": d.exit_price, **(d.meta or {})},
                            passed=True,
                            message=d.message,
                        )
                    )
                    continue

                if d.action == "ADD":
                    step = bundle.instrument.quantity_increment if bundle else None
                    add_qty = _quantize(position.qty * 0.25, step)
                    if add_qty > 0:
                        await self.execution.add_to_position(position, add_qty)
                        position.qty += add_qty
                        position.remaining_qty += add_qty
                        position.added_once = True
                        position.updated_at = utc_now_iso()
                        position.notes["management_context"] = self._management_context_for_position(position)
                        await self.trade_store.upsert_open_position(position)
                        await self.trade_store.record_trade_event(position.id, "ADD", {"qty": add_qty, "message": d.message})
                        await self.logger.stage(
                            StageRecord(
                                ts=utc_now_iso(),
                                symbol=position.pair,
                                stage="manage",
                                rule="aggressive_add_rule",
                                expected="add only when +0.5R and supportive macro",
                                actual={"add_qty": add_qty, **(d.meta or {})},
                                passed=True,
                                message=d.message,
                            )
                        )
                    continue

                if d.action == "EXIT" and d.exit_price is not None:
                    exit_qty = d.exit_qty or position.remaining_qty
                    await self.execution.exit_position_market(position, qty=exit_qty)
                    self.position_manager.close(position, price=d.exit_price, qty=d.exit_qty, reason=d.message)
                    await self.trade_store.record_trade_event(position.id, "EXIT", {"qty": exit_qty, "price": d.exit_price, "message": d.message})
                    if position.status == "CLOSED":
                        await self.trade_store.close_position(position)
                        self.risk_engine.register_trade_close(position)
                        remaining_open = len([p for p in self.state.positions.values() if p.status != "CLOSED" and p.id != position.id])
                        m = self.risk_engine.current_metrics(open_positions=remaining_open)
                        self.state.pnl["realized_today_usdt"] = m.realized_pnl_usdt_today
                        self.state.pnl["realized_today_r"] = m.realized_pnl_r_today
                        self.state.remove_position(position.id)
                    else:
                        position.notes["management_context"] = self._management_context_for_position(position)
                        await self.trade_store.upsert_open_position(position)
                    await self.logger.stage(
                        StageRecord(
                            ts=utc_now_iso(),
                            symbol=position.pair,
                            stage="exit",
                            rule="stop/target/macro/management",
                            expected="Position exits per management rules",
                            actual={"price": d.exit_price, "qty": exit_qty, "pnl_usdt": position.pnl_usdt, "pnl_r": position.pnl_r},
                            passed=True,
                            message=d.message,
                        )
                    )

            if position.status != "CLOSED" and position.mark_price is not None:
                side_mult = 1.0 if position.side == "LONG" else -1.0
                unrealized += (position.mark_price - position.entry_price) * side_mult * position.remaining_qty

        self.state.pnl["unrealized_usdt"] = round(unrealized, 8)

    async def _compute_btc_macro(self) -> BTCMacroState:
        if not self.config.exchange.use_btc_macro:
            return self.btc_macro_engine.disabled_state()
        if self.binance_macro is None:
            return BTCMacroState(enabled=True, provider=self.config.exchange.btc_macro_provider, btc_profile_15m="PROVIDER_NOT_INITIALIZED", btc_profile_supportive=False, updated_at=utc_now_iso())
        try:
            candles_4h, candles_15m, oi_series, resistance_pct = await asyncio.gather(
                self.binance_macro.get_klines("BTCUSDT", "4h", 120),
                self.binance_macro.get_klines("BTCUSDT", "15m", 120),
                self.binance_macro.get_open_interest_hist("BTCUSDT", "4h", 30),
                self.binance_macro.resistance_distance_pct("BTCUSDT"),
            )
            return self.btc_macro_engine.evaluate_from_candles(candles_4h, candles_15m, oi_series=oi_series, resistance_distance_pct=resistance_pct)
        except Exception as exc:
            await self.logger.emit("BTC_MACRO_ERROR", {"error": str(exc)})
            return BTCMacroState(enabled=True, provider=self.config.exchange.btc_macro_provider, btc_profile_15m="ERROR", btc_profile_supportive=False, updated_at=utc_now_iso(), notes=[str(exc)])

    def _tighten_stops_on_regime_flip(self) -> None:
        for p in self.state.positions.values():
            if p.status == "CLOSED":
                continue
            if p.side == "LONG" and p.stop_price < p.entry_price:
                p.stop_price = p.entry_price
            elif p.side == "SHORT" and p.stop_price > p.entry_price:
                p.stop_price = p.entry_price
            p.updated_at = utc_now_iso()

    async def _write_snapshot(self) -> None:
        open_count = len([p for p in self.state.positions.values() if p.status != "CLOSED"])
        self.state.metrics.update(await self.trade_store.daily_metrics())
        self.state.pnl["realized_today_usdt"] = float(self.state.metrics.get("realized_today") or self.state.pnl.get("realized_today_usdt") or 0.0)
        self.state.pnl["realized_today_r"] = float(self.state.metrics.get("realized_today_r") or self.state.pnl.get("realized_today_r") or 0.0)
        self.state.risk_metrics = self.risk_engine.current_metrics(open_positions=open_count)
        if self.state.risk_metrics:
            self.state.risk_metrics.daily_realized_pnl = float(self.state.metrics.get("realized_today") or self.state.risk_metrics.realized_pnl_usdt_today)
            self.state.risk_metrics.daily_R = float(self.state.metrics.get("realized_today_r") or self.state.risk_metrics.realized_pnl_r_today)
        snapshot = self.state.build_snapshot(
            config_view=self.config.public_config_view(),
            recent_events_tail=self.logger.tail_items(100),
        )
        snapshot.runtime["session_id"] = self.state.session_id
        snapshot.runtime["started_at"] = self.state.health.get("started_at")
        snapshot.runtime["cycle_ms"] = self.state.runtime_meta.get("cycle_ms")
        snapshot.runtime["last_cycle_ok"] = self.state.health.get("last_cycle_ok")
        snapshot.runtime["last_cycle_error"] = self.state.health.get("last_cycle_error")
        snapshot.runtime["snapshot_age_s"] = 0.0
        snapshot.risk.cooldown_remaining_s = snapshot.risk.cooldown_remaining_s
        snapshot.risk.daily_realized_pnl = float(self.state.metrics.get("realized_today") or snapshot.risk.realized_pnl_usdt_today)
        snapshot.risk.daily_R = float(self.state.metrics.get("realized_today_r") or snapshot.risk.realized_pnl_r_today)
        snapshot.metrics = {
            "trades_today": int(self.state.metrics.get("trades_today") or 0),
            "win_rate_today": float(self.state.metrics.get("win_rate_today") or 0.0),
            "avg_R_today": float(self.state.metrics.get("avg_R_today") or 0.0),
            "max_drawdown_today": float(self.state.metrics.get("max_drawdown_today") or 0.0),
            "max_consecutive_losses_today": int(self.state.metrics.get("max_consecutive_losses_today") or 0),
            "last_trade_ts": self.state.metrics.get("last_trade_ts"),
            "last_entry_symbol": self.state.metrics.get("last_entry_symbol"),
            "last_exit_symbol": self.state.metrics.get("last_exit_symbol"),
        }
        snapshot.diagnostics = {
            **(snapshot.diagnostics or {}),
            "last_http_error": self.state.diagnostics.get("last_http_error"),
            "last_http_error_ts": self.state.diagnostics.get("last_http_error_ts"),
            "last_exchange_latency_ms": self.state.diagnostics.get("last_exchange_latency_ms"),
        }
        snapshot.top_rejects = self.state.top_rejects[:3]
        await self.snapshot_writer.write(snapshot)
        self.state.runtime_meta["snapshot_ts"] = snapshot.ts


async def _main_async(config_path: str) -> None:
    root = Path(__file__).resolve().parent
    config = load_config(config_path)
    settings = apply_env_overrides(config)
    logging.basicConfig(level=getattr(logging, settings.log_level, logging.INFO))
    engine = BotEngine(config, root, settings=settings)

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        if not hasattr(signal, sig_name):
            continue
        try:
            loop.add_signal_handler(getattr(signal, sig_name), lambda: asyncio.create_task(engine.shutdown()))
        except (NotImplementedError, RuntimeError):
            # Windows event loop / non-main thread fallback.
            pass
    await engine.run_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="CoinDCX Futures intraday bot engine")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = parser.parse_args()
    asyncio.run(_main_async(args.config))


if __name__ == "__main__":
    main()
