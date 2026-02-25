from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from autotrade.utils import append_jsonl, utc_now_iso


class FileCommandQueue:
    """Future-phase command queue. Bot can poll this file; API may write commands later."""

    def __init__(self, path: Path) -> None:
        self.path = path

    async def enqueue(self, command_type: str, payload: dict[str, Any], ttl_seconds: int = 30) -> None:
        await append_jsonl(
            self.path,
            {
                "ts": utc_now_iso(),
                "type": command_type,
                "ttl_seconds": ttl_seconds,
                "payload": payload,
            },
        )

    async def read_all(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []

        def _read() -> list[dict[str, Any]]:
            import json

            out = []
            with self.path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    out.append(json.loads(line))
            return out

        return await asyncio.to_thread(_read)

