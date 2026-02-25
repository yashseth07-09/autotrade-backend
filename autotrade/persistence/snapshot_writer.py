from __future__ import annotations

import asyncio
from pathlib import Path

from autotrade.models import Snapshot
from autotrade.utils import atomic_write_json


class SnapshotWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = asyncio.Lock()

    async def write(self, snapshot: Snapshot) -> None:
        async with self._lock:
            await atomic_write_json(self.path, snapshot.model_dump())

