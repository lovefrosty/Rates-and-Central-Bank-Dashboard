"""Yield curve panel builder from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, List, Tuple


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


def load_raw_state(path: Path | str = Path("signals/raw_state.json")) -> Dict[str, Any]:
    path = Path(path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def build_yield_curve_panel(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    duration = raw_state.get("duration", {})
    curve = {"tenors": [], "start_of_year": [], "last_week": [], "current": []}
    rows = []
    for tenor, key in TENOR_ORDER:
        item = duration.get(key, {}) if isinstance(duration, dict) else {}
        meta = item.get("meta", {}) if isinstance(item, dict) else {}
        current = meta.get("current", item.get("value"))
        last_week = meta.get("last_week")
        start_of_year = meta.get("start_of_year")
        curve["tenors"].append(tenor)
        curve["start_of_year"].append(start_of_year)
        curve["last_week"].append(last_week)
        curve["current"].append(current)
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
    return {"curve_lines": curve, "table_rows": rows}
