from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from autotrade.models import InstrumentInfo

from .coindcx_futures_client import CoinDCXFuturesClient


def _first_present(d: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


@dataclass
class InstrumentResolver:
    client: CoinDCXFuturesClient
    margin_currency: str = "USDT"
    refresh_seconds: int = 300
    _cache: dict[str, InstrumentInfo] = field(default_factory=dict)
    _last_refresh_monotonic: float = 0.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def refresh(self, force: bool = False) -> dict[str, InstrumentInfo]:
        async with self._lock:
            now = asyncio.get_running_loop().time()
            if not force and self._cache and (now - self._last_refresh_monotonic) < self.refresh_seconds:
                return self._cache

            active = await self.client.get_active_instruments(self.margin_currency)
            next_cache: dict[str, InstrumentInfo] = {}
            for row in active:
                pair = str(_first_present(row, "pair", "symbol", "coindcx_name", "instrument")) or ""
                if not pair:
                    continue
                info = self._normalize_instrument(row)
                next_cache[pair] = info
            self._cache = next_cache
            self._last_refresh_monotonic = now
            return self._cache

    async def get_instrument(self, pair: str) -> InstrumentInfo | None:
        await self.refresh()
        if pair in self._cache:
            return self._cache[pair]
        raw = await self.client.get_instrument_details(pair, self.margin_currency)
        if not raw:
            return None
        info = self._normalize_instrument(raw)
        self._cache[info.pair] = info
        return info

    async def resolve_watchlist(self, requested: list[str], replacements: list[str] | None = None, max_size: int = 10) -> list[InstrumentInfo]:
        await self.refresh()
        replacements = replacements or []
        out: list[InstrumentInfo] = []
        seen: set[str] = set()
        for token in [*requested, *replacements]:
            if len(out) >= max_size:
                break
            match = self._match_requested_symbol(token)
            if not match:
                continue
            if match.pair in seen:
                continue
            seen.add(match.pair)
            out.append(match)
        return out

    def all_instruments(self) -> list[InstrumentInfo]:
        return list(self._cache.values())

    def _match_requested_symbol(self, token: str) -> InstrumentInfo | None:
        if not token:
            return None
        token_u = token.upper()
        # Exact pair first.
        if token_u in self._cache:
            return self._cache[token_u]

        compact = token_u.replace("/", "_").replace("-", "_")
        compact = compact.replace("USDT", "").replace("INR", "")
        for info in self._cache.values():
            pair = info.pair.upper()
            underlying = (info.underlying or "").upper()
            if token_u == underlying:
                return info
            if token_u in pair:
                return info
            if compact and compact in pair.replace("B_", "").replace("S_", ""):
                return info
        return None

    def _normalize_instrument(self, raw: dict[str, Any]) -> InstrumentInfo:
        pair = str(_first_present(raw, "pair", "symbol", "coindcx_name", "instrument"))
        margin = str(_first_present(raw, "margin_currency_short_name", "margin_currency", "marginCurrency", "USDT"))
        underlying = _first_present(raw, "base_currency_short_name", "base_currency", "underlying_asset", "base")
        quote = _first_present(raw, "quote_currency_short_name", "quote_currency", "quote")
        return InstrumentInfo(
            pair=pair.upper(),
            margin_currency=str(margin).upper(),
            status=str(_first_present(raw, "status", "instrument_status", "active") or "active"),
            underlying=str(underlying).upper() if underlying else None,
            quote=str(quote).upper() if quote else None,
            price_increment=_to_float(_first_present(raw, "price_precision", "tick_size", "price_increment")),
            quantity_increment=_to_float(_first_present(raw, "step_size", "quantity_increment")),
            min_quantity=_to_float(_first_present(raw, "min_quantity", "minimum_order_quantity")),
            max_quantity=_to_float(_first_present(raw, "max_quantity", "maximum_order_quantity")),
            min_trade_size=_to_float(_first_present(raw, "min_notional", "minimum_order_value")),
            max_leverage_long=_to_float(_first_present(raw, "max_leverage", "max_long_leverage")),
            max_leverage_short=_to_float(_first_present(raw, "max_leverage", "max_short_leverage")),
            raw=raw,
        )
