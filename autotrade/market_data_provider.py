from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from autotrade.models import Candle, InstrumentInfo

from .exchanges.coindcx_futures_client import CoinDCXFuturesClient, CoinDCXAPIError


TIMEFRAME_MINUTES: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
}


@dataclass(slots=True)
class OrderbookView:
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    ltp: float | None = None
    mark_price: float | None = None


@dataclass(slots=True)
class SymbolMarketBundle:
    instrument: InstrumentInfo
    candles_4h: list[Candle]
    candles_15m: list[Candle]
    candles_5m: list[Candle]
    orderbook: OrderbookView


class MarketDataProvider:
    def __init__(self, client: CoinDCXFuturesClient) -> None:
        self.client = client

    async def fetch_symbol_bundle(self, instrument: InstrumentInfo) -> SymbolMarketBundle:
        c4h_task = asyncio.create_task(self.get_candles(instrument.pair, "4h", limit=120))
        c15_task = asyncio.create_task(self.get_candles(instrument.pair, "15m", limit=200))
        c5_task = asyncio.create_task(self.get_candles(instrument.pair, "5m", limit=300))
        ob_task = asyncio.create_task(self.get_orderbook_view(instrument.pair))
        candles_4h, candles_15m, candles_5m, orderbook = await asyncio.gather(c4h_task, c15_task, c5_task, ob_task)
        return SymbolMarketBundle(
            instrument=instrument,
            candles_4h=candles_4h,
            candles_15m=candles_15m,
            candles_5m=candles_5m,
            orderbook=orderbook,
        )

    async def get_candles(self, pair: str, timeframe: str, limit: int = 200) -> list[Candle]:
        tf = timeframe.lower()
        if tf not in TIMEFRAME_MINUTES:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        now = int(datetime.now(tz=UTC).timestamp())
        minutes = TIMEFRAME_MINUTES[tf]

        # CoinDCX docs examples show 1/5/60/1D; 15m/4h may not always be directly supported.
        # Try native resolution first, then synthesize from a lower timeframe.
        native_res = self._resolution_for_timeframe(tf)
        if native_res:
            try:
                span_secs = max(limit * minutes * 60 * 2, 3600)
                raw = await self.client.get_candles(pair, resolution=native_res, from_unix=now - span_secs, to_unix=now)
                parsed = self._parse_candles(raw)
                if len(parsed) >= min(limit, 30):
                    return parsed[-limit:]
            except CoinDCXAPIError:
                pass

        fallback_tf = "5m" if tf in {"15m"} else ("1h" if tf == "4h" else None)
        if fallback_tf is None:
            return []

        lower = await self.get_candles(pair, fallback_tf, limit=max(limit * (TIMEFRAME_MINUTES[tf] // TIMEFRAME_MINUTES[fallback_tf]) + 10, 60))
        if not lower:
            # Last fallback: synthesize directly from trades if available and tf <= 15m.
            if tf in {"5m", "15m"}:
                trades = await self.client.get_recent_trades(pair)
                return self._synthesize_from_trades(trades, minutes)[-limit:]
            return []
        aggregated = self._aggregate_candles(lower, target_minutes=minutes)
        return aggregated[-limit:]

    async def get_orderbook_view(self, pair: str) -> OrderbookView:
        ltp: float | None = None
        mark_price: float | None = None
        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []

        try:
            ob = await self.client.get_orderbook(pair, depth=50)
            bids = self._parse_levels(ob.get("bids") or ob.get("buy") or [])
            asks = self._parse_levels(ob.get("asks") or ob.get("sell") or [])
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            if bids and asks:
                mark_price = (bids[0][0] + asks[0][0]) / 2
        except CoinDCXAPIError:
            pass

        try:
            trades = await self.client.get_recent_trades(pair)
            if trades:
                ltp = self._extract_trade_price(trades[0] if isinstance(trades, list) else trades)
        except CoinDCXAPIError:
            pass

        return OrderbookView(bids=bids, asks=asks, ltp=ltp, mark_price=mark_price or ltp)

    def liquidity_block_distance_pct(self, orderbook: OrderbookView, current_price: float, side: str) -> float | None:
        if current_price <= 0:
            return None
        side_u = side.upper()
        if side_u == "LONG":
            levels = orderbook.asks
            if not levels:
                return None
            # Approximate major wall by max quantity in top 10 asks.
            top = levels[:10]
            wall = max(top, key=lambda x: x[1]) if top else None
            if not wall:
                return None
            return ((wall[0] - current_price) / current_price) * 100.0
        if side_u == "SHORT":
            levels = orderbook.bids
            if not levels:
                return None
            top = levels[:10]
            wall = max(top, key=lambda x: x[1]) if top else None
            if not wall:
                return None
            return ((current_price - wall[0]) / current_price) * 100.0
        return None

    @staticmethod
    def _resolution_for_timeframe(tf: str) -> str | None:
        return {
            "1m": "1",
            "5m": "5",
            "15m": "15",
            "1h": "60",
            "4h": "240",
        }.get(tf)

    def _parse_candles(self, raw: list[Any]) -> list[Candle]:
        out: list[Candle] = []
        for item in raw:
            c = self._parse_single_candle(item)
            if c is not None:
                out.append(c)
        out.sort(key=lambda c: c.open_time)
        return out

    def _parse_single_candle(self, item: Any) -> Candle | None:
        try:
            if isinstance(item, dict):
                ot = int(item.get("open_time") or item.get("time") or item.get("t") or 0)
                ct = int(item.get("close_time") or item.get("T") or (ot + 1))
                o = float(item.get("open") or item.get("o"))
                h = float(item.get("high") or item.get("h"))
                l = float(item.get("low") or item.get("l"))
                c = float(item.get("close") or item.get("c"))
                v = float(item.get("volume") or item.get("v") or 0.0)
                if ot < 10_000_000_000:
                    ot *= 1000
                if ct < 10_000_000_000:
                    ct *= 1000
                return Candle(open_time=ot, close_time=ct, open=o, high=h, low=l, close=c, volume=v)
            if isinstance(item, (list, tuple)) and len(item) >= 6:
                # Common patterns:
                # [time, open, high, low, close, volume]
                # [open, high, low, volume, close, time]
                arr = list(item)
                if self._looks_like_timestamp(arr[0]):
                    ot = int(arr[0])
                    o, h, l, c, v = map(float, [arr[1], arr[2], arr[3], arr[4], arr[5]])
                elif self._looks_like_timestamp(arr[-1]):
                    ot = int(arr[-1])
                    o, h, l, v, c = map(float, [arr[0], arr[1], arr[2], arr[3], arr[4]])
                else:
                    return None
                if ot < 10_000_000_000:
                    ot *= 1000
                return Candle(open_time=ot, close_time=ot, open=o, high=h, low=l, close=c, volume=v)
        except Exception:
            return None
        return None

    @staticmethod
    def _looks_like_timestamp(value: Any) -> bool:
        try:
            v = int(value)
        except Exception:
            return False
        return v > 1_500_000_000 or v > 1_500_000_000_000

    def _aggregate_candles(self, candles: list[Candle], target_minutes: int) -> list[Candle]:
        if not candles:
            return []
        source_minutes = None
        if len(candles) >= 2:
            diff_ms = max(1, candles[1].open_time - candles[0].open_time)
            source_minutes = round(diff_ms / 60_000)
        if not source_minutes or target_minutes <= source_minutes:
            return candles

        bucket_ms = target_minutes * 60_000
        grouped: dict[int, list[Candle]] = {}
        for c in candles:
            bucket = (c.open_time // bucket_ms) * bucket_ms
            grouped.setdefault(bucket, []).append(c)

        out: list[Candle] = []
        for bucket in sorted(grouped.keys()):
            rows = sorted(grouped[bucket], key=lambda x: x.open_time)
            out.append(
                Candle(
                    open_time=rows[0].open_time,
                    close_time=rows[-1].close_time,
                    open=rows[0].open,
                    high=max(r.high for r in rows),
                    low=min(r.low for r in rows),
                    close=rows[-1].close,
                    volume=sum(r.volume for r in rows),
                )
            )
        return out

    def _synthesize_from_trades(self, trades: list[dict[str, Any]], minutes: int) -> list[Candle]:
        if not trades:
            return []
        bucket_ms = minutes * 60_000
        buckets: dict[int, list[tuple[int, float, float]]] = {}
        for t in trades:
            ts_ms = self._extract_trade_ts_ms(t)
            price = self._extract_trade_price(t)
            qty = self._extract_trade_qty(t)
            if ts_ms is None or price is None:
                continue
            bucket = (ts_ms // bucket_ms) * bucket_ms
            buckets.setdefault(bucket, []).append((ts_ms, price, qty or 0.0))
        out: list[Candle] = []
        for bucket, rows in sorted(buckets.items()):
            rows.sort(key=lambda x: x[0])
            prices = [r[1] for r in rows]
            volume = sum(r[2] for r in rows)
            out.append(
                Candle(
                    open_time=bucket,
                    close_time=bucket + bucket_ms - 1,
                    open=prices[0],
                    high=max(prices),
                    low=min(prices),
                    close=prices[-1],
                    volume=volume,
                )
            )
        return out

    @staticmethod
    def _extract_trade_price(t: Any) -> float | None:
        if isinstance(t, dict):
            for k in ("p", "price", "rate"):
                if k in t and t[k] is not None:
                    try:
                        return float(t[k])
                    except Exception:
                        return None
        return None

    @staticmethod
    def _extract_trade_qty(t: Any) -> float | None:
        if isinstance(t, dict):
            for k in ("q", "quantity", "size", "volume"):
                if k in t and t[k] is not None:
                    try:
                        return float(t[k])
                    except Exception:
                        return None
        return None

    @staticmethod
    def _extract_trade_ts_ms(t: Any) -> int | None:
        if isinstance(t, dict):
            for k in ("T", "timestamp", "time", "created_at"):
                if k in t and t[k] is not None:
                    try:
                        v = t[k]
                        if isinstance(v, str) and not v.isdigit():
                            # Attempt ISO timestamp parse.
                            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                            return int(dt.timestamp() * 1000)
                        iv = int(v)
                        return iv if iv > 10_000_000_000 else iv * 1000
                    except Exception:
                        continue
        return None

    @staticmethod
    def _parse_levels(levels: Any) -> list[tuple[float, float]]:
        out: list[tuple[float, float]] = []
        if isinstance(levels, dict):
            for price, qty in levels.items():
                try:
                    out.append((float(price), float(qty)))
                except Exception:
                    continue
        elif isinstance(levels, list):
            for row in levels:
                if isinstance(row, (list, tuple)) and len(row) >= 2:
                    try:
                        out.append((float(row[0]), float(row[1])))
                    except Exception:
                        continue
                elif isinstance(row, dict):
                    p = row.get("price") or row.get("p")
                    q = row.get("quantity") or row.get("q") or row.get("size")
                    if p is None or q is None:
                        continue
                    try:
                        out.append((float(p), float(q)))
                    except Exception:
                        continue
        return out
