"""Inflation & real rates analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths


def _get_entry(raw_state: Dict[str, Any], section: str, key: str) -> Dict[str, Any]:
    container = raw_state.get(section, {})
    if not isinstance(container, dict):
        return {}
    entry = container.get(key, {})
    return entry if isinstance(entry, dict) else {}


def _get_current(entry: Dict[str, Any]) -> Optional[float]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    value = meta.get("current", entry.get("value"))
    return None if value is None else float(value)


def _get_status(entry: Dict[str, Any]) -> Optional[str]:
    return entry.get("status") if isinstance(entry, dict) else None


def _get_change_1m(entry: Dict[str, Any]) -> Optional[float]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    change = meta.get("1m_change")
    return None if change is None else float(change)


def _get_cpi_yoy(raw_state: Dict[str, Any]) -> Optional[float]:
    policy = raw_state.get("policy", {})
    if not isinstance(policy, dict):
        return None
    entry = policy.get("cpi_level", {})
    if not isinstance(entry, dict):
        return None
    meta = entry.get("meta", {})
    current = meta.get("current", entry.get("value"))
    year_ago = meta.get("year_ago")
    if current is None or year_ago in (None, 0):
        return None
    return (float(current) / float(year_ago) - 1.0) * 100


def _resolve_breakeven(
    nominal_value: Optional[float],
    real_value: Optional[float],
    breakeven_entry: Dict[str, Any],
    nominal_status: Optional[str],
    real_status: Optional[str],
) -> tuple[Optional[float], Optional[str]]:
    if breakeven_entry:
        return _get_current(breakeven_entry), _get_status(breakeven_entry)
    if nominal_value is None or real_value is None:
        return None, "FAILED"
    if nominal_status == "FAILED" or real_status == "FAILED":
        return None, "FAILED"
    if nominal_status == "OK" and real_status == "OK":
        return nominal_value - real_value, "OK"
    return None, "FAILED"


def _driver_read(nominal_change: Optional[float], real_change: Optional[float]) -> Optional[str]:
    if nominal_change is None or real_change is None:
        return "Mixed drivers / insufficient data"
    if real_change > 0 and nominal_change == real_change:
        return "Real-rate repricing (growth / policy)"
    if nominal_change > 0 and real_change > 0 and nominal_change > real_change:
        return "Inflation repricing"
    if nominal_change < 0 and real_change < 0:
        return "Recession / easing"
    return "Mixed drivers / insufficient data"


def build_inflation_real_rates(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    nominal_entry = _get_entry(raw_state, "duration", "y10_nominal")
    real_entry = _get_entry(raw_state, "duration", "y10_real")
    breakeven_entry = _get_entry(raw_state, "duration", "breakeven_10y")
    if not breakeven_entry:
        breakeven_entry = _get_entry(raw_state, "duration", "y10_breakeven")

    nominal_value = _get_current(nominal_entry)
    real_value = _get_current(real_entry)
    nominal_status = _get_status(nominal_entry)
    real_status = _get_status(real_entry)
    nominal_change = _get_change_1m(nominal_entry)
    real_change = _get_change_1m(real_entry)
    breakeven_change = None
    if nominal_change is not None and real_change is not None:
        breakeven_change = nominal_change - real_change

    breakeven_value, breakeven_status = _resolve_breakeven(
        nominal_value,
        real_value,
        breakeven_entry,
        nominal_status,
        real_status,
    )
    cpi_yoy_pct = _get_cpi_yoy(raw_state)

    return {
        "nominal_10y": nominal_value,
        "real_10y": real_value,
        "breakeven_10y": breakeven_value,
        "breakeven_10y_change": breakeven_change,
        "cpi_yoy_pct": cpi_yoy_pct,
        "real_rate_spread": breakeven_value,
        "driver_read": _driver_read(
            nominal_change,
            real_change,
        ),
        "data_quality": {
            "nominal": nominal_status,
            "real": real_status,
            "breakeven": breakeven_status,
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
    daily["inflation_real_rates"] = build_inflation_real_rates(raw_state)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
