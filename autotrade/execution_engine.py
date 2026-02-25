from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from autotrade.config import AppConfig
from autotrade.models import Position, SignalCandidate
from autotrade.utils import utc_now_iso

from .exchanges.coindcx_futures_client import CoinDCXFuturesClient


class FuturesBroker(Protocol):
    async def set_leverage(self, pair: str, leverage: int) -> dict[str, Any]: ...
    async def place_order(
        self,
        pair: str,
        side: str,
        order_type: str,
        qty: float,
        price: float | None = None,
        reduce_only: bool = False,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        leverage: int | None = None,
    ) -> dict[str, Any]: ...
    async def get_open_positions(self) -> list[dict[str, Any]]: ...
    async def get_open_orders(self) -> list[dict[str, Any]]: ...
    async def cancel_order(self, order_id: str) -> dict[str, Any]: ...


class CoinDCXFuturesBroker:
    def __init__(self, client: CoinDCXFuturesClient) -> None:
        self.client = client

    async def set_leverage(self, pair: str, leverage: int) -> dict[str, Any]:
        return await self.client.set_leverage(pair, leverage)

    async def place_order(
        self,
        pair: str,
        side: str,
        order_type: str,
        qty: float,
        price: float | None = None,
        reduce_only: bool = False,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        leverage: int | None = None,
    ) -> dict[str, Any]:
        order = await self.client.create_order(
            side="buy" if side.upper() == "LONG" else "sell",
            order_type="market_order" if order_type.lower() == "market" else "limit_order",
            pair=pair,
            total_quantity=qty,
            price=price,
            reduce_only=reduce_only,
            leverage=leverage,
            client_order_id=str(uuid4()),
        )
        # Attach TP/SL if provided and supported.
        extras: dict[str, Any] = {}
        if stop_loss is not None:
            try:
                extras["stop_loss"] = await self.client.create_tpsl_order(
                    pair=pair,
                    side="sell" if side.upper() == "LONG" else "buy",
                    trigger_price=stop_loss,
                    order_type="stop_market",
                    total_quantity=qty,
                    reduce_only=True,
                )
            except Exception as exc:
                extras["stop_loss_error"] = str(exc)
        if take_profit is not None:
            try:
                extras["take_profit"] = await self.client.create_tpsl_order(
                    pair=pair,
                    side="sell" if side.upper() == "LONG" else "buy",
                    trigger_price=take_profit,
                    order_type="take_profit_market",
                    total_quantity=qty,
                    reduce_only=True,
                )
            except Exception as exc:
                extras["take_profit_error"] = str(exc)
        if extras:
            order["attached_extras"] = extras
        return order

    async def get_open_positions(self) -> list[dict[str, Any]]:
        return await self.client.list_positions(status="open")

    async def get_open_orders(self) -> list[dict[str, Any]]:
        return await self.client.list_orders()

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        return await self.client.cancel_order(order_id)


@dataclass(slots=True)
class ExecutionResult:
    accepted: bool
    message: str
    order_payload: dict[str, Any]
    position: Position | None = None


class ExecutionEngine:
    def __init__(self, config: AppConfig, broker: FuturesBroker | None, dry_run: bool = True) -> None:
        self.config = config
        self.broker = broker
        self.dry_run = dry_run

    async def enter_from_signal(self, signal: SignalCandidate, qty: float, leverage: int) -> ExecutionResult:
        entry_price = signal.ltp or signal.mark_price or signal.entry_price
        if qty <= 0:
            return ExecutionResult(False, "qty_non_positive", {})

        order_payload: dict[str, Any]
        if self.dry_run or self.broker is None:
            order_payload = {
                "mode": "dry_run",
                "pair": signal.pair,
                "side": signal.side,
                "type": "market",
                "qty": qty,
                "price": entry_price,
                "leverage": leverage,
            }
        else:
            await self.broker.set_leverage(signal.pair, leverage)
            order_payload = await self.broker.place_order(
                pair=signal.pair,
                side=signal.side,
                order_type="market",
                qty=qty,
                reduce_only=False,
                stop_loss=signal.stop_price,
                leverage=leverage,
            )

        now = utc_now_iso()
        position = Position(
            id=str(uuid4()),
            symbol=signal.symbol,
            pair=signal.pair,
            margin_currency=signal.margin_currency,
            side=signal.side,
            setup=signal.setup,
            status="OPEN",
            qty=qty,
            remaining_qty=qty,
            leverage=leverage,
            entry_price=float(entry_price),
            stop_price=float(signal.stop_price),
            initial_stop_price=float(signal.stop_price),
            target_price=None,
            partial_taken=False,
            added_once=False,
            opened_at=now,
            updated_at=now,
            mark_price=signal.mark_price,
            ltp=signal.ltp,
            notes={
                "signal_id": signal.id,
                "signal_score": signal.score,
                "signal_volume_ratio": signal.volume_ratio,
                "btc_profile": signal.btc_profile,
                "order": order_payload,
            },
        )
        return ExecutionResult(True, "entered", order_payload, position)

    async def exit_position_market(self, position: Position, qty: float | None = None) -> dict[str, Any]:
        qty = position.remaining_qty if qty is None else min(qty, position.remaining_qty)
        if qty <= 0:
            return {"accepted": False, "message": "qty_non_positive"}
        if self.dry_run or self.broker is None:
            return {"accepted": True, "mode": "dry_run", "pair": position.pair, "side": position.side, "qty": qty, "reduce_only": True}
        return await self.broker.place_order(
            pair=position.pair,
            side="SHORT" if position.side == "LONG" else "LONG",
            order_type="market",
            qty=qty,
            reduce_only=True,
            leverage=position.leverage,
        )

    async def add_to_position(self, position: Position, add_qty: float) -> dict[str, Any]:
        if add_qty <= 0:
            return {"accepted": False, "message": "add_qty_non_positive"}
        if self.dry_run or self.broker is None:
            return {"accepted": True, "mode": "dry_run", "add_qty": add_qty}
        return await self.broker.place_order(
            pair=position.pair,
            side=position.side,
            order_type="market",
            qty=add_qty,
            reduce_only=False,
            leverage=position.leverage,
        )
