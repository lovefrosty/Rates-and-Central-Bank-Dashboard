"""Labor market analytics from raw_state.json."""
# NOTE: Evidence-only block. No resolver consumes this data in V1.
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths
from Signals.json_utils import write_json


def _get_entry(raw_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    container = raw_state.get("labor_market", {})
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


def _snapshots(entry: Dict[str, Any]) -> Dict[str, Optional[float]]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    current = meta.get("current", entry.get("value"))
    last_week = meta.get("last_week")
    start_of_year = meta.get("start_of_year")
    change_1m = meta.get("1m_change")
    last_month = None
    if current is not None and change_1m is not None:
        last_month = current - change_1m
    return {
        "current": None if current is None else float(current),
        "last_week": None if last_week is None else float(last_week),
        "last_month": None if last_month is None else float(last_month),
        "start_of_year": None if start_of_year is None else float(start_of_year),
    }


def _quality(status: Optional[str], current: Optional[float], year_ago: Optional[float]) -> str:
    if status == "FAILED" or current is None:
        return "FAILED"
    if year_ago is None:
        return "PARTIAL"
    return "OK"


def _yoy_change(current: Optional[float], year_ago: Optional[float]) -> Optional[float]:
    if current is None or year_ago is None:
        return None
    return current - year_ago


def _yoy_pct(current: Optional[float], year_ago: Optional[float]) -> Optional[float]:
    if current is None or year_ago is None or year_ago == 0:
        return None
    return (current / year_ago - 1.0) * 100


def build_labor_market(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    unrate_entry = _get_entry(raw_state, "unrate")
    jolts_entry = _get_entry(raw_state, "jolts_openings")
    eci_entry = _get_entry(raw_state, "eci")

    unrate_current = _get_current(unrate_entry)
    jolts_current = _get_current(jolts_entry)
    eci_current = _get_current(eci_entry)

    unrate_year_ago = _get_year_ago(unrate_entry)
    jolts_year_ago = _get_year_ago(jolts_entry)
    eci_year_ago = _get_year_ago(eci_entry)
    unrate_snapshots = _snapshots(unrate_entry)
    jolts_snapshots = _snapshots(jolts_entry)
    eci_snapshots = _snapshots(eci_entry)

    return {
        "unrate_current": unrate_current,
        "unrate_yoy_change": _yoy_change(unrate_current, unrate_year_ago),
        "jolts_openings_current": jolts_current,
        "jolts_yoy_change": _yoy_change(jolts_current, jolts_year_ago),
        "eci_index_current": eci_current,
        "eci_yoy_pct": _yoy_pct(eci_current, eci_year_ago),
        "anchors": {
            "unrate": unrate_snapshots,
            "jolts_openings": jolts_snapshots,
            "eci": eci_snapshots,
        },
        "data_quality": {
            "unrate": _quality(unrate_entry.get("status"), unrate_current, unrate_year_ago),
            "jolts_openings": _quality(jolts_entry.get("status"), jolts_current, jolts_year_ago),
            "eci": _quality(eci_entry.get("status"), eci_current, eci_year_ago),
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
    daily["labor_market"] = build_labor_market(raw_state)
    write_json(daily_path, daily)
    return daily
