from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from autotrade.utils import atomic_write_json


class StateResumeStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = asyncio.Lock()

    async def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None

        def _read() -> dict[str, Any] | None:
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return None

        return await asyncio.to_thread(_read)

    async def write(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            await atomic_write_json(self.path, payload)

