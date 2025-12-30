"""Yield curve panel analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, List, Tuple

from Signals import state_paths
from Signals.json_utils import write_json


TENOR_ORDER: List[Tuple[str, str]] = [
    ("3M", "y3m_nominal"),
    ("6M", "y6m_nominal"),
    ("1Y", "y1y_nominal"),
    ("2Y", "y2y_nominal"),
    ("3Y", "y3y_nominal"),
    ("5Y", "y5y_nominal"),
    ("7Y", "y7y_nominal"),
    ("10Y", "y10_nominal"),
    ("20Y", "y20y_nominal"),
    ("30Y", "y30y_nominal"),
]


def _snapshots(item: Dict[str, Any]) -> Dict[str, Any]:
    meta = item.get("meta", {}) if isinstance(item, dict) else {}
    current = meta.get("current", item.get("value"))
    last_week = meta.get("last_week")
    last_month = meta.get("last_month")
    last_6m = meta.get("last_6m")
    start_of_year = meta.get("start_of_year")
    change_1m = meta.get("1m_change")
    if last_month is None and current is not None and change_1m is not None:
        last_month = current - change_1m
    return {
        "current": current,
        "last_week": last_week,
        "last_month": last_month,
        "last_6m": last_6m,
        "start_of_year": start_of_year,
    }


def build_yield_curve_block(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    duration = raw_state.get("duration", {})
    tenors: list[str] = []
    lines = {"start_of_year": [], "last_6m": [], "last_month": [], "last_week": [], "current": []}
    changes_bps = {"change_1w": [], "change_1m": [], "change_6m": [], "change_ytd": []}
    rows = []
    for tenor, key in TENOR_ORDER:
        item = duration.get(key, {}) if isinstance(duration, dict) else {}
        snapshots = _snapshots(item)
        current = snapshots["current"]
        last_week = snapshots["last_week"]
        last_month = snapshots["last_month"]
        last_6m = snapshots["last_6m"]
        start_of_year = snapshots["start_of_year"]
        tenors.append(tenor)
        lines["start_of_year"].append(start_of_year)
        lines["last_6m"].append(last_6m)
        lines["last_month"].append(last_month)
        lines["last_week"].append(last_week)
        lines["current"].append(current)
        change_1w = None if current is None or last_week is None else (current - last_week) * 100
        change_1m = None if current is None or last_month is None else (current - last_month) * 100
        change_6m = None if current is None or last_6m is None else (current - last_6m) * 100
        change_ytd = None if current is None or start_of_year is None else (current - start_of_year) * 100
        changes_bps["change_1w"].append(change_1w)
        changes_bps["change_1m"].append(change_1m)
        changes_bps["change_6m"].append(change_6m)
        changes_bps["change_ytd"].append(change_ytd)
        rows.append(
            {
                "tenor": tenor,
                "start_of_year": start_of_year,
                "last_6m": last_6m,
                "last_month": last_month,
                "last_week": last_week,
                "current": current,
                "weekly_change_bps": change_1w,
                "change_1m_bps": change_1m,
                "change_6m_bps": change_6m,
                "change_ytd_bps": change_ytd,
            }
        )
    return {"tenors": tenors, "lines": lines, "table_rows": rows, "changes_bps": changes_bps}


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
    daily["yield_curve"] = build_yield_curve_block(raw_state)
    write_json(daily_path, daily)
    return daily
