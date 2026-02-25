from __future__ import annotations

import os
from dataclasses import dataclass

from autotrade.config import AppConfig


def _env_bool(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    v = raw.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _env_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    try:
        return int(raw)
    except Exception:
        return None


@dataclass(slots=True)
class RuntimeSettings:
    data_dir: str
    port: int
    log_level: str
    dry_run: bool
    max_events_mb: int
    max_event_rotations: int
    git_commit: str
    build_time: str
    config_path: str


def apply_env_overrides(config: AppConfig) -> RuntimeSettings:
    data_dir = os.getenv("DATA_DIR", config.runtime.data_dir)
    port = _env_int("PORT") or int(config.observer.port)
    dry_run = _env_bool("RUNTIME_DRY_RUN")
    if dry_run is None:
        dry_run = bool(config.runtime.dry_run)
    log_level = (os.getenv("LOG_LEVEL") or "INFO").upper()
    max_events_mb = _env_int("MAX_EVENTS_MB") or 128
    max_event_rotations = _env_int("MAX_EVENTS_ROTATIONS") or 5
    config.runtime.data_dir = data_dir
    config.runtime.dry_run = dry_run
    config.observer.port = port
    return RuntimeSettings(
        data_dir=data_dir,
        port=port,
        log_level=log_level,
        dry_run=dry_run,
        max_events_mb=max_events_mb,
        max_event_rotations=max_event_rotations,
        git_commit=os.getenv("GIT_COMMIT", "unknown"),
        build_time=os.getenv("BUILD_TIME", "unknown"),
        config_path=os.getenv("AUTOTRADE_CONFIG", "config.yaml"),
    )

