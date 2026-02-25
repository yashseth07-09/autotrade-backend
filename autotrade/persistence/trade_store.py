from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from autotrade.analytics import daily_trade_metrics, list_open_trades, list_trades
from autotrade.models import Position
from autotrade.utils import ensure_dir, json_dumps, utc_now_iso


class TradeStore:
    def __init__(self, db_path: Path, schema_path: Path) -> None:
        self.db_path = db_path
        self.schema_path = schema_path
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        ensure_dir(self.db_path.parent)
        schema = self.schema_path.read_text(encoding="utf-8")

        def _init() -> None:
            con = sqlite3.connect(self.db_path)
            try:
                con.executescript(schema)
                con.commit()
            finally:
                con.close()

        await asyncio.to_thread(_init)

    async def upsert_open_position(self, position: Position) -> None:
        async with self._lock:
            await asyncio.to_thread(self._upsert_open_position_sync, position)

    def _upsert_open_position_sync(self, position: Position) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                """
                INSERT INTO trades (
                  id,symbol,pair,margin_currency,side,setup,status,leverage,qty,entry_price,stop_price,target_price,
                  mark_price,ltp,liquidation_price,risk_r,opened_at,notes
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  status=excluded.status,
                  qty=excluded.qty,
                  stop_price=excluded.stop_price,
                  target_price=excluded.target_price,
                  mark_price=excluded.mark_price,
                  ltp=excluded.ltp,
                  liquidation_price=excluded.liquidation_price,
                  notes=excluded.notes
                """,
                (
                    position.id,
                    position.symbol,
                    position.pair,
                    position.margin_currency,
                    position.side,
                    position.setup,
                    position.status,
                    position.leverage,
                    position.qty,
                    position.entry_price,
                    position.stop_price,
                    position.target_price,
                    position.mark_price,
                    position.ltp,
                    position.liquidation_price,
                    1.0,
                    position.opened_at,
                    json_dumps(position.notes),
                ),
            )
            con.commit()
        finally:
            con.close()

    async def close_position(self, position: Position) -> None:
        async with self._lock:
            await asyncio.to_thread(self._close_position_sync, position)

    def _close_position_sync(self, position: Position) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                """
                UPDATE trades
                SET status=?, closed_at=?, exit_price=?, pnl_usdt=?, pnl_r=?, mark_price=?, ltp=?, liquidation_price=?, notes=?
                WHERE id=?
                """,
                (
                    position.status,
                    position.closed_at,
                    position.exit_price,
                    position.pnl_usdt,
                    position.pnl_r,
                    position.mark_price,
                    position.ltp,
                    position.liquidation_price,
                    json_dumps(position.notes),
                    position.id,
                ),
            )
            con.commit()
        finally:
            con.close()

    async def record_trade_event(self, trade_id: str, event_type: str, payload: dict[str, Any]) -> None:
        async with self._lock:
            await asyncio.to_thread(self._record_trade_event_sync, trade_id, event_type, payload)

    def _record_trade_event_sync(self, trade_id: str, event_type: str, payload: dict[str, Any]) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                "INSERT INTO trade_events (trade_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?)",
                (trade_id, event_type, json_dumps(payload), utc_now_iso()),
            )
            con.commit()
        finally:
            con.close()

    async def list_trades(self, limit: int = 200) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_trades_sync, limit)

    def _list_trades_sync(self, limit: int) -> list[dict[str, Any]]:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute(
                "SELECT * FROM trades ORDER BY opened_at DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()

    async def list_trades_filtered(
        self,
        *,
        symbol: str | None = None,
        from_ts: str | None = None,
        to_ts: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            list_trades,
            self.db_path,
            symbol=symbol,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            offset=offset,
        )

    async def list_open_trade_rows(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(list_open_trades, self.db_path)

    async def daily_metrics(self) -> dict[str, Any]:
        return await asyncio.to_thread(daily_trade_metrics, self.db_path)
