from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import httpx


class CoinDCXAPIError(RuntimeError):
    pass


@dataclass(slots=True)
class CoinDCXAuth:
    api_key: str
    api_secret: str


class CoinDCXFuturesClient:
    """Async CoinDCX futures REST client.

    Notes:
    - Public market-data futures candles use `public.coindcx.com` with `pcode=f`.
    - Authenticated futures endpoints use `api.coindcx.com/exchange/v1/derivatives/futures/...`.
    - CoinDCX signatures are HMAC-SHA256 over the JSON payload string (docs/PDF pattern).
    """

    def __init__(
        self,
        *,
        api_base_url: str = "https://api.coindcx.com",
        public_base_url: str = "https://public.coindcx.com",
        timeout_seconds: float = 10.0,
        rate_limit_backoff_seconds: float = 2.0,
        auth: CoinDCXAuth | None = None,
        diagnostics_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.public_base_url = public_base_url.rstrip("/")
        self.rate_limit_backoff_seconds = rate_limit_backoff_seconds
        self.auth = auth
        self.diagnostics_callback = diagnostics_callback
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> Any:
        headers: dict[str, str] = {}
        content = None
        if signed:
            if not self.auth:
                raise CoinDCXAPIError("Authenticated endpoint requested without API credentials")
            if json_body is None:
                json_body = {}
            json_body = dict(json_body)
            json_body.setdefault("timestamp", int(time.time() * 1000))
            payload = json.dumps(json_body, separators=(",", ":"))
            signature = hmac.new(
                self.auth.api_secret.encode("utf-8"),
                payload.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            headers["X-AUTH-APIKEY"] = self.auth.api_key
            headers["X-AUTH-SIGNATURE"] = signature
            headers["Content-Type"] = "application/json"
            content = payload

        for attempt in range(3):
            started = time.perf_counter()
            try:
                resp = await self._client.request(method, url, params=params, content=content, headers=headers)
            except httpx.HTTPError as exc:
                latency_ms = (time.perf_counter() - started) * 1000.0
                await self._emit_diag(
                    {
                        "kind": "HTTP_ERROR",
                        "method": method,
                        "url": url,
                        "status": None,
                        "exception": repr(exc),
                        "retry_count": attempt,
                        "latency_ms": round(latency_ms, 2),
                    }
                )
                raise CoinDCXAPIError(f"{method} {url} failed: {exc}") from exc
            latency_ms = (time.perf_counter() - started) * 1000.0
            await self._emit_diag(
                {
                    "kind": "HTTP_OK",
                    "method": method,
                    "url": url,
                    "status": resp.status_code,
                    "latency_ms": round(latency_ms, 2),
                }
            )
            if resp.status_code == 429 and attempt < 2:
                await self._emit_diag(
                    {
                        "kind": "HTTP_ERROR",
                        "method": method,
                        "url": url,
                        "status": 429,
                        "exception": "rate_limited",
                        "retry_count": attempt + 1,
                        "latency_ms": round(latency_ms, 2),
                    }
                )
                await asyncio.sleep(self.rate_limit_backoff_seconds * (attempt + 1))
                continue
            if resp.status_code >= 400:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text
                await self._emit_diag(
                    {
                        "kind": "HTTP_ERROR",
                        "method": method,
                        "url": url,
                        "status": resp.status_code,
                        "exception": str(detail)[:500],
                        "retry_count": attempt,
                        "latency_ms": round(latency_ms, 2),
                    }
                )
                raise CoinDCXAPIError(f"{method} {url} failed: {resp.status_code} {detail}")
            if not resp.content:
                return None
            ctype = resp.headers.get("content-type", "")
            if "application/json" in ctype or resp.text[:1] in ("{", "["):
                return resp.json()
            return resp.text
        raise CoinDCXAPIError(f"{method} {url} rate limited after retries")

    async def _emit_diag(self, payload: dict[str, Any]) -> None:
        cb = self.diagnostics_callback
        if cb is None:
            return
        try:
            res = cb(payload)
            if inspect.isawaitable(res):
                await res
        except Exception:
            return

    async def get_active_instruments(self, margin_currency: str = "USDT") -> list[dict[str, Any]]:
        url = f"{self.api_base_url}/exchange/v1/derivatives/futures/data/active_instruments"
        params = {"margin_currency_short_name[]": margin_currency.upper()}
        data = await self._request("GET", url, params=params)
        if isinstance(data, dict):
            if isinstance(data.get("data"), list):
                data = data["data"]
            elif isinstance(data.get("instruments"), list):
                data = data["instruments"]
            elif isinstance(data.get(margin_currency.upper()), list):
                data = data[margin_currency.upper()]
        if isinstance(data, list):
            normalized: list[dict[str, Any]] = []
            for item in data:
                if isinstance(item, dict):
                    if self._matches_margin(item, margin_currency):
                        normalized.append(item)
                    continue
                if isinstance(item, str):
                    normalized.append({"pair": item, "margin_currency_short_name": margin_currency.upper()})
            return normalized
        return []

    async def get_instrument_details(self, pair: str, margin_currency: str = "USDT") -> dict[str, Any]:
        url = f"{self.api_base_url}/exchange/v1/derivatives/futures/data/instrument"
        params = {"pair": pair, "margin_currency_short_name": margin_currency.upper()}
        data = await self._request("GET", url, params=params)
        if isinstance(data, list):
            return data[0] if data else {}
        return data if isinstance(data, dict) else {}

    async def get_candles(
        self,
        pair: str,
        *,
        resolution: str,
        from_unix: int,
        to_unix: int,
    ) -> list[dict[str, Any]]:
        url = f"{self.public_base_url}/market_data/candlesticks"
        params = {
            "pair": pair,
            "resolution": resolution,
            "from": from_unix,
            "to": to_unix,
            "pcode": "f",
        }
        data = await self._request("GET", url, params=params)
        if isinstance(data, dict):
            # Some CoinDCX public endpoints return wrapper objects.
            if isinstance(data.get("data"), list):
                return data["data"]
            if isinstance(data.get("candles"), list):
                return data["candles"]
        return data if isinstance(data, list) else []

    async def get_recent_trades(self, pair: str) -> list[dict[str, Any]]:
        url = f"{self.public_base_url}/market_data/trades"
        data = await self._request("GET", url, params={"pair": pair})
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            return data["data"]
        return data if isinstance(data, list) else []

    async def get_orderbook(self, pair: str, depth: int = 50) -> dict[str, Any]:
        url = f"{self.public_base_url}/market_data/orderbook/{pair}-futures/{int(depth)}"
        data = await self._request("GET", url)
        return data if isinstance(data, dict) else {}

    async def list_positions(self, status: str = "open") -> list[dict[str, Any]]:
        url = f"{self.api_base_url}/exchange/v1/derivatives/futures/positions"
        body = {"status": status}
        data = await self._request("POST", url, json_body=body, signed=True)
        return data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])

    async def list_orders(self) -> list[dict[str, Any]]:
        url = f"{self.api_base_url}/exchange/v1/derivatives/futures/orders"
        data = await self._request("POST", url, json_body={}, signed=True)
        return data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])

    async def create_order(
        self,
        *,
        side: str,
        order_type: str,
        pair: str,
        total_quantity: float,
        price: float | None = None,
        trigger_price: float | None = None,
        reduce_only: bool = False,
        time_in_force: str = "good_till_cancel",
        leverage: int | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        url = f"{self.api_base_url}/exchange/v1/derivatives/futures/orders/create"
        body: dict[str, Any] = {
            "side": side.lower(),
            "order_type": order_type.lower(),
            "pair": pair,
            "total_quantity": str(total_quantity),
            "reduce_only": reduce_only,
            "time_in_force": time_in_force,
        }
        if price is not None:
            body["price"] = str(price)
        if trigger_price is not None:
            body["trigger_price"] = str(trigger_price)
        if leverage is not None:
            body["leverage"] = int(leverage)
        if client_order_id:
            body["client_order_id"] = client_order_id
        data = await self._request("POST", url, json_body=body, signed=True)
        return data if isinstance(data, dict) else {"raw": data}

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        url = f"{self.api_base_url}/exchange/v1/derivatives/futures/orders/cancel"
        body = {"id": order_id}
        data = await self._request("POST", url, json_body=body, signed=True)
        return data if isinstance(data, dict) else {"raw": data}

    async def create_tpsl_order(
        self,
        *,
        pair: str,
        side: str,
        trigger_price: float,
        price: float | None = None,
        order_type: str = "stop_limit",
        total_quantity: float | None = None,
        reduce_only: bool = True,
    ) -> dict[str, Any]:
        url = f"{self.api_base_url}/exchange/v1/derivatives/futures/orders/create_tpsl"
        body: dict[str, Any] = {
            "pair": pair,
            "side": side.lower(),
            "order_type": order_type.lower(),
            "trigger_price": str(trigger_price),
            "reduce_only": reduce_only,
        }
        if price is not None:
            body["price"] = str(price)
        if total_quantity is not None:
            body["total_quantity"] = str(total_quantity)
        data = await self._request("POST", url, json_body=body, signed=True)
        return data if isinstance(data, dict) else {"raw": data}

    async def set_leverage(self, pair: str, leverage: int) -> dict[str, Any]:
        """No-op placeholder if API lacks dedicated leverage endpoint.

        CoinDCX futures docs commonly allow leverage on order payload; this method returns an
        explicit capability response so callers can log/verify instead of failing.
        """
        return {
            "supported": False,
            "pair": pair,
            "requested_leverage": leverage,
            "message": "Dedicated leverage endpoint not implemented; leverage should be passed on order placement or configured on exchange.",
        }

    @staticmethod
    def _matches_margin(item: dict[str, Any], margin_currency: str) -> bool:
        if not isinstance(item, dict):
            return False
        target = margin_currency.upper()
        candidates = [
            item.get("margin_currency_short_name"),
            item.get("margin_currency"),
            item.get("marginCurrency"),
        ]
        return any(str(c).upper() == target for c in candidates if c is not None) or target in str(item)
