from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import uvicorn
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from autotrade.analytics import aggregate_metrics_from_snapshot_and_db, list_trades
from autotrade.config import AppConfig, load_config
from autotrade.settings import RuntimeSettings, apply_env_overrides
from autotrade.utils import ensure_dir


def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _tail_jsonl(path: Path, limit: int = 100) -> list[dict[str, Any]]:
    if not path.exists() or limit <= 0:
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    out: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _parse_iso(ts_raw: str | None) -> datetime | None:
    if not ts_raw:
        return None
    try:
        dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return None


def _age_seconds(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        return max(0.0, (datetime.now(tz=UTC).timestamp() - path.stat().st_mtime))
    except Exception:
        return None


def _event_matches(
    event: dict[str, Any],
    *,
    event_type: str | None = None,
    symbol: str | None = None,
    since: str | None = None,
) -> bool:
    if event_type:
        t = str(event.get("type") or "").upper()
        et = event_type.upper()
        stage = str(((event.get("payload") or {}) if isinstance(event.get("payload"), dict) else {}).get("stage") or "").lower()
        if et == "DIAG":
            if not t.startswith("DIAG_"):
                return False
        elif et == "ENTER":
            if not (t == "ENTER" or stage == "entered"):
                return False
        elif et == "EXIT":
            if not (t == "EXIT" or stage == "exit"):
                return False
        elif t != et:
            return False
    if symbol:
        s = symbol.upper()
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        sym = str(payload.get("symbol") or payload.get("pair") or "")
        if s not in sym.upper():
            return False
    if since:
        since_dt = _parse_iso(since)
        evt_dt = _parse_iso(str(event.get("ts") or ""))
        if since_dt and evt_dt and evt_dt < since_dt:
            return False
    return True


class FileEventBroadcaster:
    def __init__(
        self,
        events_path: Path,
        poll_ms: int = 250,
        heartbeat_seconds: float = 5.0,
        heartbeat_payload_fn: Callable[[], Any] | None = None,
    ) -> None:
        self.events_path = events_path
        self.poll_ms = max(50, poll_ms)
        self.heartbeat_seconds = max(1.0, float(heartbeat_seconds))
        self.heartbeat_payload_fn = heartbeat_payload_fn
        self.ws_clients_count = 0

    async def stream(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.ws_clients_count += 1
        try:
            await websocket.send_json({"type": "hello", "payload": {"source": "events.jsonl", "ts": datetime.now(tz=UTC).isoformat()}})
            for evt in await asyncio.to_thread(_tail_jsonl, self.events_path, 50):
                await websocket.send_json({"type": "event", "payload": evt})

            offset = self.events_path.stat().st_size if self.events_path.exists() else 0
            last_heartbeat = asyncio.get_running_loop().time()
            while True:
                await asyncio.sleep(self.poll_ms / 1000.0)
                now_m = asyncio.get_running_loop().time()
                if (now_m - last_heartbeat) >= self.heartbeat_seconds:
                    heartbeat = {}
                    if self.heartbeat_payload_fn:
                        try:
                            maybe = self.heartbeat_payload_fn()
                            heartbeat = await maybe if asyncio.iscoroutine(maybe) else maybe
                        except Exception as exc:
                            heartbeat = {"error": str(exc)}
                    heartbeat = dict(heartbeat or {})
                    heartbeat["ws_clients_count"] = self.ws_clients_count
                    heartbeat["ts"] = datetime.now(tz=UTC).isoformat()
                    await websocket.send_json({"type": "heartbeat", "payload": heartbeat})
                    last_heartbeat = now_m

                if not self.events_path.exists():
                    continue
                try:
                    size = self.events_path.stat().st_size
                    if size < offset:
                        offset = 0
                    if size == offset:
                        continue
                    with self.events_path.open("r", encoding="utf-8", errors="ignore") as fh:
                        fh.seek(offset)
                        chunk = fh.read()
                        offset = fh.tell()
                    for line in chunk.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            evt = json.loads(line)
                        except Exception:
                            continue
                        await websocket.send_json({"type": "event", "payload": evt})
                except WebSocketDisconnect:
                    raise
                except Exception as exc:
                    await websocket.send_json({"type": "error", "payload": {"message": str(exc)}})
        finally:
            self.ws_clients_count = max(0, self.ws_clients_count - 1)


class ObserverRuntime:
    def __init__(self, config: AppConfig, root: Path, settings: RuntimeSettings) -> None:
        self.config = config
        self.root = root
        self.settings = settings
        self.paths = config.runtime_paths(root)
        ensure_dir(self.paths["data_dir"])
        self.started_at = datetime.now(tz=UTC)
        self._snapshot_cache: dict[str, Any] | None = None
        self._snapshot_cache_at_monotonic = 0.0
        self.broadcaster = FileEventBroadcaster(
            self.paths["events"],
            config.observer.event_stream_poll_ms,
            config.observer.stream_heartbeat_seconds,
            heartbeat_payload_fn=self.heartbeat_payload,
        )

    async def read_snapshot(self) -> dict[str, Any]:
        ttl_s = max(0.05, float(self.config.observer.snapshot_cache_ttl_ms) / 1000.0)
        now_m = asyncio.get_running_loop().time()
        if self._snapshot_cache is not None and (now_m - self._snapshot_cache_at_monotonic) <= ttl_s:
            return self._snapshot_cache
        snapshot = await asyncio.to_thread(_read_json_file, self.paths["snapshot"])
        if snapshot is None:
            raise HTTPException(status_code=404, detail="snapshot not found")
        stale = self.is_snapshot_stale(snapshot)
        wrapper = {"snapshot": snapshot, "stale": stale, "source": str(self.paths["snapshot"])}
        self._snapshot_cache = wrapper
        self._snapshot_cache_at_monotonic = now_m
        return wrapper

    def is_snapshot_stale(self, snapshot_wrapper_or_snapshot: dict[str, Any]) -> bool:
        snapshot = snapshot_wrapper_or_snapshot.get("snapshot", snapshot_wrapper_or_snapshot)
        ts_raw = snapshot.get("ts")
        ts = _parse_iso(str(ts_raw) if ts_raw is not None else None)
        if not ts:
            return True
        age = (datetime.now(tz=UTC) - ts).total_seconds()
        return age > self.config.observer.snapshot_stale_after_seconds

    async def snapshot_age_s(self) -> float | None:
        wrapper = await self.read_snapshot()
        ts = _parse_iso(wrapper["snapshot"].get("ts"))
        if not ts:
            return None
        return max(0.0, (datetime.now(tz=UTC) - ts).total_seconds())

    async def heartbeat_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        try:
            wrapper = await self.read_snapshot()
            snap = wrapper.get("snapshot") or {}
            age = await self.snapshot_age_s()
            payload["snapshot_age_s"] = round(age, 2) if age is not None else None
            payload["cycle_ms"] = ((snap.get("runtime") or {}).get("cycle_ms") if isinstance(snap, dict) else None)
        except HTTPException:
            payload["snapshot_age_s"] = None
            payload["cycle_ms"] = None
        return payload

    async def list_trades(
        self,
        *,
        symbol: str | None,
        from_ts: str | None,
        to_ts: str | None,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            list_trades,
            self.paths["trades_db"],
            symbol=symbol,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            offset=offset,
        )

    async def list_metrics(self) -> dict[str, Any]:
        snapshot_wrapper = await asyncio.to_thread(_read_json_file, self.paths["snapshot"])
        snapshot = snapshot_wrapper if isinstance(snapshot_wrapper, dict) else None
        return await asyncio.to_thread(aggregate_metrics_from_snapshot_and_db, snapshot, self.paths["trades_db"])

    async def tail_events(
        self,
        *,
        limit: int,
        offset: int = 0,
        event_type: str | None = None,
        symbol: str | None = None,
        since: str | None = None,
    ) -> list[dict[str, Any]]:
        # Simplicity-first tailing: read more than requested, then filter and paginate newest-first.
        raw = await asyncio.to_thread(_tail_jsonl, self.paths["events"], max(limit + offset + 1000, 1000))
        items = [e for e in raw if _event_matches(e, event_type=event_type, symbol=symbol, since=since)]
        items = list(reversed(items))
        if offset > 0:
            items = items[offset:]
        return items[:limit]

    async def diagnostics(self) -> dict[str, Any]:
        snapshot_wrapper = await asyncio.to_thread(_read_json_file, self.paths["snapshot"])
        snapshot = snapshot_wrapper if isinstance(snapshot_wrapper, dict) else {}
        health = (snapshot or {}).get("health") or {}
        runtime = (snapshot or {}).get("runtime") or {}
        diag = (snapshot or {}).get("diagnostics") or {}
        snapshot_age_s = None
        if snapshot:
            ts = _parse_iso(snapshot.get("ts"))
            if ts:
                snapshot_age_s = max(0.0, (datetime.now(tz=UTC) - ts).total_seconds())
        return {
            "snapshot_age_s": round(snapshot_age_s, 2) if snapshot_age_s is not None else None,
            "events_file_age_s": round(_age_seconds(self.paths["events"]) or 0.0, 2) if self.paths["events"].exists() else None,
            "trades_db_age_s": round(_age_seconds(self.paths["trades_db"]) or 0.0, 2) if self.paths["trades_db"].exists() else None,
            "last_cycle_ok": runtime.get("last_cycle_ok", health.get("last_cycle_ok")),
            "cycle_ms": runtime.get("cycle_ms"),
            "ws_clients_count": self.broadcaster.ws_clients_count,
            "last_http_error": diag.get("last_http_error"),
            "last_http_error_ts": diag.get("last_http_error_ts"),
        }


def create_app(config_path: str = "config.yaml") -> FastAPI:
    root = Path(__file__).resolve().parent
    config = load_config(config_path)
    settings = apply_env_overrides(config)
    logging.basicConfig(level=getattr(logging, settings.log_level, logging.INFO))
    runtime = ObserverRuntime(config, root, settings)
    app = FastAPI(title="AutoTrade Observer API", version="0.2.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.observer.cors_allowed_origins or ["*"],
        allow_credentials=config.observer.cors_allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/snapshot")
    async def get_snapshot() -> JSONResponse:
        return JSONResponse(await runtime.read_snapshot())

    @app.get("/signals")
    async def get_signals() -> JSONResponse:
        wrapper = await runtime.read_snapshot()
        snapshot = wrapper["snapshot"]
        return JSONResponse(
            {
                "ts": snapshot.get("ts"),
                "stale": wrapper["stale"],
                "items": snapshot.get("top_candidates", []),
                "count": len(snapshot.get("top_candidates", [])),
            }
        )

    @app.get("/positions")
    async def get_positions() -> JSONResponse:
        wrapper = await runtime.read_snapshot()
        snapshot = wrapper["snapshot"]
        items = snapshot.get("open_positions", [])
        return JSONResponse(
            {
                "ts": snapshot.get("ts"),
                "stale": wrapper["stale"],
                "margin_currency": runtime.config.exchange.margin_currency,
                "items": items,
                "count": len(items),
            }
        )

    @app.get("/trades")
    async def get_trades(
        symbol: str | None = None,
        from_ts: str | None = Query(None, alias="from"),
        to_ts: str | None = Query(None, alias="to"),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> JSONResponse:
        max_limit = min(int(limit), 500, max(1, int(runtime.config.observer.max_trade_rows)))
        rows = await runtime.list_trades(symbol=symbol, from_ts=from_ts, to_ts=to_ts, limit=max_limit, offset=offset)
        return JSONResponse({"items": rows, "count": len(rows), "limit": max_limit, "offset": offset})

    @app.get("/metrics")
    async def get_metrics() -> JSONResponse:
        return JSONResponse(await runtime.list_metrics())

    @app.get("/diagnostics")
    async def get_diagnostics() -> JSONResponse:
        return JSONResponse(await runtime.diagnostics())

    @app.get("/health")
    async def get_health() -> JSONResponse:
        snapshot_data = await asyncio.to_thread(_read_json_file, runtime.paths["snapshot"])
        stale = True
        health: dict[str, Any] = {"bot_running": False}
        ts = None
        if snapshot_data:
            stale = runtime.is_snapshot_stale(snapshot_data)
            health = snapshot_data.get("health", {}) or {}
            ts = snapshot_data.get("ts")
        return JSONResponse(
            {
                "observer": {
                    "status": "ok",
                    "started_at": runtime.started_at.isoformat(),
                    "uptime_seconds": int((datetime.now(tz=UTC) - runtime.started_at).total_seconds()),
                },
                "bot": health,
                "snapshot_ts": ts,
                "snapshot_stale": stale,
                "paths": {
                    "snapshot": str(runtime.paths["snapshot"]),
                    "events": str(runtime.paths["events"]),
                    "trades_db": str(runtime.paths["trades_db"]),
                },
            }
        )

    @app.get("/version")
    async def get_version() -> JSONResponse:
        return JSONResponse(
            {
                "app": "autotrade-observer",
                "git_commit": runtime.settings.git_commit,
                "build_time": runtime.settings.build_time,
                "data_dir": str(runtime.paths["data_dir"]),
                "dry_run": bool(runtime.config.runtime.dry_run),
            }
        )

    @app.get("/events")
    async def get_events(
        type: str | None = Query(None),
        symbol: str | None = Query(None),
        since: str | None = Query(None),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> JSONResponse:
        max_limit = min(int(limit), 500, max(1, int(runtime.config.observer.max_events_limit)))
        events = await runtime.tail_events(limit=max_limit, offset=offset, event_type=type, symbol=symbol, since=since)
        return JSONResponse({"items": events, "count": len(events), "limit": max_limit, "offset": offset})

    @app.websocket("/stream")
    async def stream(websocket: WebSocket) -> None:
        try:
            await runtime.broadcaster.stream(websocket)
        except WebSocketDisconnect:
            return

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="AutoTrade Observer API")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    config = load_config(args.config)
    settings = apply_env_overrides(config)
    logging.basicConfig(level=getattr(logging, settings.log_level, logging.INFO))
    uvicorn.run(create_app(args.config), host=config.observer.host, port=config.observer.port)


app = create_app(os.getenv("AUTOTRADE_CONFIG", "config.yaml"))


if __name__ == "__main__":
    main()
