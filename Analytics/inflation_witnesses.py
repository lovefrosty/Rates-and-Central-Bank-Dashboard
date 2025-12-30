"""Inflation witnesses analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths
from Signals.json_utils import write_json


def _get_entry(raw_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    container = raw_state.get("inflation_witnesses", {})
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


def _inflation_proxy_meta(proxy_type: str, current: Optional[float], year_ago: Optional[float]) -> Dict[str, Any]:
    confidence = "MEDIUM" if current is not None and year_ago is not None else "LOW"
    return {
        "type": proxy_type,
        "orientation": "backward-looking",
        "confidence": confidence,
        "fallback_used": False,
    }


def build_inflation_witnesses(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    headline_entry = _get_entry(raw_state, "cpi_headline")
    core_entry = _get_entry(raw_state, "cpi_core")

    headline_current = _get_current(headline_entry)
    core_current = _get_current(core_entry)
    headline_year_ago = _get_year_ago(headline_entry)
    core_year_ago = _get_year_ago(core_entry)

    return {
        "cpi_headline_index_current": headline_current,
        "cpi_core_index_current": core_current,
        "cpi_headline_yoy_pct": _yoy_pct(headline_current, headline_year_ago),
        "cpi_core_yoy_pct": _yoy_pct(core_current, core_year_ago),
        "inflation_proxy": {
            "cpi_headline_yoy": _inflation_proxy_meta("CPI_YoY", headline_current, headline_year_ago),
            "cpi_core_yoy": _inflation_proxy_meta("Core_CPI", core_current, core_year_ago),
        },
        "data_quality": {
            "cpi_headline": _quality(headline_entry.get("status"), headline_current, headline_year_ago),
            "cpi_core": _quality(core_entry.get("status"), core_current, core_year_ago),
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
    daily["inflation_witnesses"] = build_inflation_witnesses(raw_state)
    write_json(daily_path, daily)
    return daily
