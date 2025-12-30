"""CPI level analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths
from Signals.json_utils import write_json


def _get_entry(raw_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    container = raw_state.get("policy", {})
    if not isinstance(container, dict):
        return {}
    entry = container.get(key, {})
    return entry if isinstance(entry, dict) else {}


def _get_current(entry: Dict[str, Any]) -> Optional[float]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    value = meta.get("current", entry.get("value"))
    return None if value is None else float(value)


def _get_year_ago(entry: Dict[str, Any]) -> Optional[float]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    value = meta.get("year_ago")
    return None if value is None else float(value)


def _quality(status: Optional[str], current: Optional[float], year_ago: Optional[float]) -> str:
    if status == "FAILED" or current is None:
        return "FAILED"
    if year_ago is None:
        return "PARTIAL"
    return "OK"


def _yoy_pct(current: Optional[float], year_ago: Optional[float]) -> Optional[float]:
    if current is None or year_ago is None or year_ago == 0:
        return None
    return (current / year_ago - 1.0) * 100


def _inflation_proxy_meta(current: Optional[float], year_ago: Optional[float]) -> Dict[str, Any]:
    confidence = "MEDIUM" if current is not None and year_ago is not None else "LOW"
    return {
        "type": "CPI_YoY",
        "orientation": "backward-looking",
        "confidence": confidence,
        "fallback_used": False,
    }


def build_inflation_level(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    entry = _get_entry(raw_state, "cpi_level")
    current = _get_current(entry)
    year_ago = _get_year_ago(entry)
    status = entry.get("status") if isinstance(entry, dict) else None
    return {
        "cpi_level_current": current,
        "cpi_yoy_pct": _yoy_pct(current, year_ago),
        "inflation_proxy": _inflation_proxy_meta(current, year_ago),
        "data_quality": {
            "cpi_level": _quality(status, current, year_ago),
        },
    }


def write_daily_state(
    raw_state_path: Path | str = state_paths.RAW_STATE_PATH,
    daily_state_path: Path | str = state_paths.DAILY_STATE_PATH,
) -> Dict[str, Any]:
    raw_state = json.loads(Path(raw_state_path).read_text(encoding="utf-8"))
    daily_path = Path(daily_state_path)
    daily = {}
    if daily_path.exists():
        daily = json.loads(daily_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(daily, dict):
            daily = {}
    daily["inflation_level"] = build_inflation_level(raw_state)
    write_json(daily_path, daily)
    return daily
