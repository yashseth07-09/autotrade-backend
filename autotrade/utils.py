from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def json_dumps(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=True, default=str)


async def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(payload, indent=2, ensure_ascii=True, default=str)

    def _write() -> None:
        ensure_dir(path.parent)
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)

    await asyncio.to_thread(_write)


async def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    line = json_dumps(payload) + "\n"

    def _append() -> None:
        ensure_dir(path.parent)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line)

    await asyncio.to_thread(_append)


def resolve_env_placeholders(value: Any) -> Any:
    if isinstance(value, str):
        return ENV_PATTERN.sub(lambda m: os.getenv(m.group(1), ""), value)
    if isinstance(value, dict):
        return {k: resolve_env_placeholders(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_env_placeholders(v) for v in value]
    return value

