from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path
from typing import Any

from autotrade.models import StageRecord
from autotrade.utils import ensure_dir, json_dumps, utc_now_iso


class EventLogger:
    """Async JSONL event logger with a bounded in-memory tail."""

    def __init__(
        self,
        path: Path,
        tail_size: int = 2000,
        *,
        max_bytes: int | None = None,
        max_rotations: int = 5,
    ) -> None:
        self.path = path
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=10_000)
        self.tail: deque[dict[str, Any]] = deque(maxlen=tail_size)
        self._worker_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()
        self.max_bytes = max_bytes if (max_bytes and max_bytes > 0) else None
        self.max_rotations = max(1, int(max_rotations))

    async def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker(), name="event-logger")

    async def stop(self) -> None:
        self._stopped.set()
        if self._worker_task:
            await self._worker_task
            self._worker_task = None

    async def emit(self, event_type: str, payload: dict[str, Any]) -> None:
        event = {"ts": utc_now_iso(), "type": event_type, "payload": payload}
        self.tail.append(event)
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            # Drop oldest-style behavior via an explicit warning event.
            overflow = {
                "ts": utc_now_iso(),
                "type": "LOGGER_OVERFLOW",
                "payload": {"dropped_type": event_type},
            }
            self.tail.append(overflow)

    async def stage(self, record: StageRecord) -> None:
        await self.emit("STAGE", record.model_dump())

    async def _worker(self) -> None:
        while not self._stopped.is_set() or not self.queue.empty():
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                await asyncio.to_thread(self._write_event_sync, item)
            except Exception as exc:  # pragma: no cover - safety path
                self.tail.append(
                    {
                        "ts": utc_now_iso(),
                        "type": "LOGGER_WRITE_ERROR",
                        "payload": {"error": str(exc)},
                    }
                )

    def _write_event_sync(self, item: dict[str, Any]) -> None:
        self._rotate_if_needed()
        ensure_dir(self.path.parent)
        line = json_dumps(item) + "\n"
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line)

    def _rotate_if_needed(self) -> None:
        if self.max_bytes is None:
            return
        try:
            if not self.path.exists():
                return
            if self.path.stat().st_size < self.max_bytes:
                return
        except Exception:
            return

        # Shift older files upward: events.jsonl.4 -> events.jsonl.5, ..., events.jsonl -> events.jsonl.1
        for idx in range(self.max_rotations, 0, -1):
            src = self.path if idx == 1 else self.path.with_suffix(self.path.suffix + f".{idx-1}")
            dst = self.path.with_suffix(self.path.suffix + f".{idx}")
            try:
                if dst.exists():
                    dst.unlink()
            except Exception:
                pass
            try:
                if src.exists():
                    src.replace(dst)
            except Exception:
                continue

    def tail_items(self, limit: int = 200) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        items = list(self.tail)
        return items[-limit:]
