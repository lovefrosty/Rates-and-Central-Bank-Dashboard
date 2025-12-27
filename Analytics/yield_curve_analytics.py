"""Yield curve panel analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, List, Tuple

from Signals import state_paths


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


def build_yield_curve_block(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    duration = raw_state.get("duration", {})
    tenors: list[str] = []
    lines = {"start_of_year": [], "last_week": [], "current": []}
    rows = []
    for tenor, key in TENOR_ORDER:
        item = duration.get(key, {}) if isinstance(duration, dict) else {}
        meta = item.get("meta", {}) if isinstance(item, dict) else {}
        current = meta.get("current", item.get("value"))
        last_week = meta.get("last_week")
        start_of_year = meta.get("start_of_year")
        tenors.append(tenor)
        lines["start_of_year"].append(start_of_year)
        lines["last_week"].append(last_week)
        lines["current"].append(current)
        weekly_change_bps = None
        if current is not None and last_week is not None:
            weekly_change_bps = (current - last_week) * 100
        rows.append(
            {
                "tenor": tenor,
                "start_of_year": start_of_year,
                "last_week": last_week,
                "current": current,
                "weekly_change_bps": weekly_change_bps,
            }
        )
    return {"tenors": tenors, "lines": lines, "table_rows": rows}


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
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
