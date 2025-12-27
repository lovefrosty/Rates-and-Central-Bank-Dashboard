"""Credit transmission analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional, Tuple

from Signals import state_paths


def _get_entry(raw_state: Dict[str, Any], section: str, key: str) -> Dict[str, Any]:
    container = raw_state.get(section, {})
    if not isinstance(container, dict):
        return {}
    entry = container.get(key, {})
    return entry if isinstance(entry, dict) else {}


def _extract_values(entry: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    status = entry.get("status") if isinstance(entry, dict) else None
    if status == "FAILED":
        return None, None, status
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    current = meta.get("current")
    last_week = meta.get("last_week")
    return current, last_week, status


def _quality(current: Optional[float], last_week: Optional[float], status: Optional[str]) -> str:
    if status == "FAILED" or (current is None and last_week is None):
        return "FAILED"
    if current is not None and last_week is not None:
        return "OK"
    return "PARTIAL"


def _weekly_change_bps(current: Optional[float], last_week: Optional[float]) -> Optional[float]:
    if current is None or last_week is None:
        return None
    return (current - last_week) * 100


def build_credit_transmission(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    ig_entry = _get_entry(raw_state, "credit_spreads", "ig_oas")
    hy_entry = _get_entry(raw_state, "credit_spreads", "hy_oas")
    y10_entry = _get_entry(raw_state, "duration", "y10_nominal")
    dxy_entry = _get_entry(raw_state, "global_policy", "dxy")

    ig_current, ig_last_week, ig_status = _extract_values(ig_entry)
    hy_current, hy_last_week, hy_status = _extract_values(hy_entry)
    y10_current, y10_last_week, y10_status = _extract_values(y10_entry)

    dxy_meta = dxy_entry.get("meta", {}) if isinstance(dxy_entry, dict) else {}
    dxy_current = dxy_meta.get("current", dxy_entry.get("value"))
    dxy_status = dxy_entry.get("status") if isinstance(dxy_entry, dict) else None

    return {
        "ig_oas_current": ig_current,
        "ig_oas_weekly_change_bps": _weekly_change_bps(ig_current, ig_last_week),
        "hy_oas_current": hy_current,
        "hy_oas_weekly_change_bps": _weekly_change_bps(hy_current, hy_last_week),
        "hy_minus_ig_bps": None if ig_current is None or hy_current is None else (hy_current - ig_current) * 100,
        "treasury_10y_current": y10_current,
        "treasury_10y_weekly_change_bps": _weekly_change_bps(y10_current, y10_last_week),
        "dxy": {
            "current": dxy_current,
            "changes_pct": {
                "1d": dxy_meta.get("1d_change_pct"),
                "5d": dxy_meta.get("5d_change_pct"),
                "1m": dxy_meta.get("1m_change_pct"),
                "6m": dxy_meta.get("6m_change_pct"),
            },
            "candle_1y": {
                "open": dxy_meta.get("year_open"),
                "high": dxy_meta.get("year_high"),
                "low": dxy_meta.get("year_low"),
                "close": dxy_current,
            },
            "data_quality": _quality(dxy_current, dxy_meta.get("last_week"), dxy_status),
        },
        "data_quality": {
            "ig_oas": _quality(ig_current, ig_last_week, ig_status),
            "hy_oas": _quality(hy_current, hy_last_week, hy_status),
            "treasury_10y": _quality(y10_current, y10_last_week, y10_status),
        },
    }


def write_daily_state(
    raw_state_path: Path | str = state_paths.RAW_STATE_PATH,
    daily_state_path: Path | str = state_paths.DAILY_STATE_PATH,
) -> Dict[str, Any]:
    raw_state = json.loads(Path(raw_state_path).read_text(encoding="utf-8"))
    daily_path = Path(daily_state_path)
    daily: Dict[str, Any] = {}
    if daily_path.exists():
        daily = json.loads(daily_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(daily, dict):
            daily = {}
    daily["credit_transmission"] = build_credit_transmission(raw_state)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
