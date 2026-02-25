from __future__ import annotations

from typing import Any

import httpx

from autotrade.models import Candle


class BinanceMacroProvider:
    """Optional macro-only provider (never used for entry/stop execution prices)."""

    def __init__(self, base_url: str = "https://fapi.binance.com", timeout_seconds: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> list[Candle]:
        url = f"{self.base_url}/fapi/v1/klines"
        resp = await self._client.get(url, params={"symbol": symbol, "interval": interval, "limit": limit})
        resp.raise_for_status()
        data = resp.json()
        out: list[Candle] = []
        for row in data:
            out.append(
                Candle(
                    open_time=int(row[0]),
                    close_time=int(row[6]),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                )
            )
        return out

    async def get_open_interest_hist(self, symbol: str, period: str = "4h", limit: int = 30) -> list[float]:
        url = f"{self.base_url}/futures/data/openInterestHist"
        resp = await self._client.get(url, params={"symbol": symbol, "period": period, "limit": limit})
        resp.raise_for_status()
        data = resp.json()
        out: list[float] = []
        for row in data:
            v = row.get("sumOpenInterestValue") or row.get("sumOpenInterest")
            if v is None:
                continue
            try:
                out.append(float(v))
            except Exception:
                continue
        return out

    async def get_orderbook(self, symbol: str, limit: int = 50) -> dict[str, Any]:
        url = f"{self.base_url}/fapi/v1/depth"
        resp = await self._client.get(url, params={"symbol": symbol, "limit": limit})
        resp.raise_for_status()
        return resp.json()

    async def resistance_distance_pct(self, symbol: str = "BTCUSDT") -> float | None:
        ob = await self.get_orderbook(symbol, limit=50)
        bids = [(float(p), float(q)) for p, q in ob.get("bids", [])]
        asks = [(float(p), float(q)) for p, q in ob.get("asks", [])]
        if not bids or not asks:
            return None
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])
        mid = (bids[0][0] + asks[0][0]) / 2
        wall = max(asks[:10], key=lambda x: x[1]) if asks[:10] else None
        if not wall or mid <= 0:
            return None
        return ((wall[0] - mid) / mid) * 100.0

