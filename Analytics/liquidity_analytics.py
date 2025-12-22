"""Liquidity analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional, Tuple


def _get_entry(raw_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    container = raw_state.get("liquidity", {})
    if not isinstance(container, dict):
        return {}
    entry = container.get(key, {})
    return entry if isinstance(entry, dict) else {}


def _extract_values(entry: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    status = entry.get("status") if isinstance(entry, dict) else None
    if status == "FAILED":
        return None, None, None, status
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    current = meta.get("current")
    last_week = meta.get("last_week")
    start_of_year = meta.get("start_of_year")
    return current, last_week, start_of_year, status


def _quality(current: Optional[float], last_week: Optional[float], start_of_year: Optional[float], status: Optional[str]) -> str:
    if status == "FAILED" or (current is None and last_week is None and start_of_year is None):
        return "FAILED"
    if current is not None and last_week is not None and start_of_year is not None:
        return "OK"
    return "PARTIAL"


def _changes(current: Optional[float], last_week: Optional[float], start_of_year: Optional[float]) -> Dict[str, Optional[float]]:
    change_1w = None if current is None or last_week is None else current - last_week
    change_ytd = None if current is None or start_of_year is None else current - start_of_year
    return {"change_1w": change_1w, "change_ytd": change_ytd}


def build_liquidity_analytics(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    rrp_entry = _get_entry(raw_state, "rrp_level")
    tga_entry = _get_entry(raw_state, "tga_level")

    rrp_current, rrp_last_week, rrp_start, rrp_status = _extract_values(rrp_entry)
    tga_current, tga_last_week, tga_start, tga_status = _extract_values(tga_entry)

    rrp_changes = _changes(rrp_current, rrp_last_week, rrp_start)
    tga_changes = _changes(tga_current, tga_last_week, tga_start)

    return {
        "rrp": {
            "level": rrp_current,
            "change_1w": rrp_changes["change_1w"],
            "change_ytd": rrp_changes["change_ytd"],
        },
        "tga": {
            "level": tga_current,
            "change_1w": tga_changes["change_1w"],
            "change_ytd": tga_changes["change_ytd"],
        },
        "data_quality": {
            "rrp": _quality(rrp_current, rrp_last_week, rrp_start, rrp_status),
            "tga": _quality(tga_current, tga_last_week, tga_start, tga_status),
        },
    }


def write_daily_state(
    raw_state_path: Path | str = Path("signals/raw_state.json"),
    daily_state_path: Path | str = Path("signals/daily_state.json"),
) -> Dict[str, Any]:
    raw_state = json.loads(Path(raw_state_path).read_text(encoding="utf-8"))
    daily_path = Path(daily_state_path)
    daily = {}
    if daily_path.exists():
        daily = json.loads(daily_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(daily, dict):
            daily = {}
    daily["liquidity_analytics"] = build_liquidity_analytics(raw_state)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
