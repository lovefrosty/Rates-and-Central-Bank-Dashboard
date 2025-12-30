"""Liquidity analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional, Tuple

from Signals import state_paths
from Signals.json_utils import write_json


def _get_entry(raw_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    container = raw_state.get("liquidity", {})
    if not isinstance(container, dict):
        return {}
    entry = container.get(key, {})
    return entry if isinstance(entry, dict) else {}


def _extract_values(
    entry: Dict[str, Any],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[str]]:
    status = entry.get("status") if isinstance(entry, dict) else None
    if status == "FAILED":
        return None, None, None, None, None, status
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    current = meta.get("current")
    last_week = meta.get("last_week")
    last_month = meta.get("last_month")
    last_6m = meta.get("last_6m")
    start_of_year = meta.get("start_of_year")
    change_1m = meta.get("1m_change")
    if last_month is None and current is not None and change_1m is not None:
        last_month = current - change_1m
    return current, last_week, last_month, last_6m, start_of_year, status


def _quality(
    current: Optional[float],
    last_week: Optional[float],
    last_month: Optional[float],
    last_6m: Optional[float],
    start_of_year: Optional[float],
    status: Optional[str],
) -> str:
    if status == "FAILED" or (
        current is None
        and last_week is None
        and last_month is None
        and last_6m is None
        and start_of_year is None
    ):
        return "FAILED"
    if (
        current is not None
        and last_week is not None
        and last_month is not None
        and last_6m is not None
        and start_of_year is not None
    ):
        return "OK"
    return "PARTIAL"


def _changes(
    current: Optional[float],
    last_week: Optional[float],
    last_month: Optional[float],
    last_6m: Optional[float],
    start_of_year: Optional[float],
) -> Dict[str, Optional[float]]:
    change_1w = None if current is None or last_week is None else current - last_week
    change_1m = None if current is None or last_month is None else current - last_month
    change_6m = None if current is None or last_6m is None else current - last_6m
    change_ytd = None if current is None or start_of_year is None else current - start_of_year
    return {"change_1w": change_1w, "change_1m": change_1m, "change_6m": change_6m, "change_ytd": change_ytd}


def build_liquidity_analytics(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    rrp_entry = _get_entry(raw_state, "rrp_level")
    tga_entry = _get_entry(raw_state, "tga_level")
    walcl_entry = _get_entry(raw_state, "walcl")

    rrp_current, rrp_last_week, rrp_last_month, rrp_last_6m, rrp_start, rrp_status = _extract_values(rrp_entry)
    tga_current, tga_last_week, tga_last_month, tga_last_6m, tga_start, tga_status = _extract_values(tga_entry)
    walcl_current, walcl_last_week, walcl_last_month, walcl_last_6m, walcl_start, walcl_status = _extract_values(walcl_entry)

    rrp_changes = _changes(rrp_current, rrp_last_week, rrp_last_month, rrp_last_6m, rrp_start)
    tga_changes = _changes(tga_current, tga_last_week, tga_last_month, tga_last_6m, tga_start)
    walcl_changes = _changes(walcl_current, walcl_last_week, walcl_last_month, walcl_last_6m, walcl_start)

    return {
        "rrp": {
            "level": rrp_current,
            "change_1w": rrp_changes["change_1w"],
            "change_1m": rrp_changes["change_1m"],
            "change_6m": rrp_changes["change_6m"],
            "change_ytd": rrp_changes["change_ytd"],
            "anchors": {
                "current": rrp_current,
                "last_week": rrp_last_week,
                "last_month": rrp_last_month,
                "last_6m": rrp_last_6m,
                "start_of_year": rrp_start,
            },
        },
        "tga": {
            "level": tga_current,
            "change_1w": tga_changes["change_1w"],
            "change_1m": tga_changes["change_1m"],
            "change_6m": tga_changes["change_6m"],
            "change_ytd": tga_changes["change_ytd"],
            "anchors": {
                "current": tga_current,
                "last_week": tga_last_week,
                "last_month": tga_last_month,
                "last_6m": tga_last_6m,
                "start_of_year": tga_start,
            },
        },
        "walcl": {
            "level": walcl_current,
            "change_1w": walcl_changes["change_1w"],
            "change_1m": walcl_changes["change_1m"],
            "change_6m": walcl_changes["change_6m"],
            "change_ytd": walcl_changes["change_ytd"],
            "anchors": {
                "current": walcl_current,
                "last_week": walcl_last_week,
                "last_month": walcl_last_month,
                "last_6m": walcl_last_6m,
                "start_of_year": walcl_start,
            },
        },
        "data_quality": {
            "rrp": _quality(rrp_current, rrp_last_week, rrp_last_month, rrp_last_6m, rrp_start, rrp_status),
            "tga": _quality(tga_current, tga_last_week, tga_last_month, tga_last_6m, tga_start, tga_status),
            "walcl": _quality(walcl_current, walcl_last_week, walcl_last_month, walcl_last_6m, walcl_start, walcl_status),
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
    daily["liquidity_analytics"] = build_liquidity_analytics(raw_state)
    write_json(daily_path, daily)
    return daily
