from __future__ import annotations

from typing import Any


STANDARD_REJECTION_CODES = {
    "NO_4H_BIAS_ALIGNMENT",
    "NO_STRUCTURE_BREAK",
    "LOW_VOLUME",
    "LOW_ATR",
    "HIGH_SPREAD",
    "COOLDOWN_ACTIVE",
    "DAILY_LOSS_LIMIT",
    "ALREADY_IN_POSITION",
    "EXECUTION_GATE_BLOCK",
}


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, (int, float, bool)):
        return str(value).lower()
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_text(v) for v in value)
    return str(value).lower()


def classify_rejection_code(
    *,
    stage: str | None = None,
    rule: str | None = None,
    message: str | None = None,
    actual: Any = None,
    meta: dict[str, Any] | None = None,
) -> str | None:
    st = str(stage or "").strip().lower()
    rl = str(rule or "").strip().lower()
    msg = str(message or "").strip().lower()
    actual_text = _flatten_text(actual)
    meta_text = _flatten_text(meta or {})
    text = " ".join([st, rl, msg, actual_text, meta_text]).strip()
    if not text:
        return None

    if "no_4h_bias_alignment" in text or "bias_mismatch" in text:
        return "NO_4H_BIAS_ALIGNMENT"
    if "4h bias" in text and "align" in text:
        return "NO_4H_BIAS_ALIGNMENT"

    if "low_volume" in text:
        return "LOW_VOLUME"
    if "vol_ratio" in text and ("below" in text or "min_volume" in text):
        return "LOW_VOLUME"

    if "low_atr" in text or "atr_unavailable" in text:
        return "LOW_ATR"

    if "high_spread" in text or ("spread" in text and ("high" in text or "max_spread" in text)):
        return "HIGH_SPREAD"

    if "max_daily_loss" in text or "daily_loss" in text:
        return "DAILY_LOSS_LIMIT"

    if "cooldown" in text:
        return "COOLDOWN_ACTIVE"

    if "position_already_open" in text or "duplicate_pair_position" in text or "already_in_position" in text:
        return "ALREADY_IN_POSITION"

    if st == "execution_gate_5m" or "execution_gate" in text:
        return "EXECUTION_GATE_BLOCK"

    if "structure" in text or "breakout" in text or st in {"setup_candidate_15m", "signal_confirmed_15m_close", "entry_eval"}:
        return "NO_STRUCTURE_BREAK"

    if st == "entered":
        return "EXECUTION_GATE_BLOCK"

    return None

