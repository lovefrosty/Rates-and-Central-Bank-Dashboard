"""JSON helpers with NaN/Inf sanitation."""
from __future__ import annotations

from pathlib import Path
import json
import math
from typing import Any


def _sanitize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        val = float(value)
        return val if math.isfinite(val) else None
    try:
        if hasattr(value, "__float__"):
            val = float(value)
            return val if math.isfinite(val) else None
    except (TypeError, ValueError):
        pass
    return value


def sanitize_data(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: sanitize_data(val) for key, val in value.items()}
    if isinstance(value, list):
        return [sanitize_data(item) for item in value]
    return _sanitize_scalar(value)


def write_json(path: Path | str, data: Any) -> Any:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    sanitized = sanitize_data(data)
    target.write_text(
        json.dumps(sanitized, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return sanitized
