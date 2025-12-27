"""Volatility analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths


def _get_entry(raw_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    container = raw_state.get("volatility", {})
    if not isinstance(container, dict):
        return {}
    entry = container.get(key, {})
    return entry if isinstance(entry, dict) else {}


def _get_value(entry: Dict[str, Any]) -> Optional[float]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    value = meta.get("current", entry.get("value"))
    return None if value is None else float(value)


def _get_status(entry: Dict[str, Any]) -> Optional[str]:
    return entry.get("status") if isinstance(entry, dict) else None


def _get_5d_roc(entry: Dict[str, Any]) -> Optional[float]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    roc = meta.get("5d_roc")
    return None if roc is None else float(roc)


def _get_change_pct(entry: Dict[str, Any], field: str) -> Optional[float]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    value = meta.get(field)
    return None if value is None else float(value)


def _build_changes(entry: Dict[str, Any]) -> Dict[str, Optional[float]]:
    return {
        "1d_pct": _get_change_pct(entry, "1d_change_pct"),
        "5d_pct": _get_change_pct(entry, "5d_change_pct"),
        "1m_pct": _get_change_pct(entry, "1m_change_pct"),
        "6m_pct": _get_change_pct(entry, "6m_change_pct"),
    }


def _stress_origin_read(move_roc: Optional[float], vix_roc: Optional[float]) -> str:
    if move_roc is None or vix_roc is None:
        return "Low or indeterminate stress"
    if move_roc > 0 and vix_roc <= 0:
        return "Rates-led volatility"
    if move_roc > 0 and vix_roc > 0:
        return "Cross-asset stress"
    if vix_roc > 0 and move_roc <= 0:
        return "Equity-led volatility"
    return "Low or indeterminate stress"


def build_volatility_block(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    vix_entry = _get_entry(raw_state, "vix")
    move_entry = _get_entry(raw_state, "move")
    gvz_entry = _get_entry(raw_state, "gvz")
    ovx_entry = _get_entry(raw_state, "ovx")
    vix_value = _get_value(vix_entry)
    move_value = _get_value(move_entry)
    gvz_value = _get_value(gvz_entry)
    ovx_value = _get_value(ovx_entry)
    vix_roc = _get_5d_roc(vix_entry)
    move_roc = _get_5d_roc(move_entry)
    vix_move_ratio = None
    if vix_value is not None and move_value not in (None, 0):
        vix_move_ratio = vix_value / move_value
    move_vix_ratio = None
    if move_value is not None and vix_value not in (None, 0):
        move_vix_ratio = move_value / vix_value
    gvz_vix_ratio = None
    if gvz_value is not None and vix_value not in (None, 0):
        gvz_vix_ratio = gvz_value / vix_value
    ovx_vix_ratio = None
    if ovx_value is not None and vix_value not in (None, 0):
        ovx_vix_ratio = ovx_value / vix_value

    return {
        "vix": vix_value,
        "move": move_value,
        "gvz": gvz_value,
        "ovx": ovx_value,
        "vix_move_ratio": vix_move_ratio,
        "move_vix_ratio": move_vix_ratio,
        "gvz_vix_ratio": gvz_vix_ratio,
        "ovx_vix_ratio": ovx_vix_ratio,
        "vix_5d_roc": vix_roc,
        "move_5d_roc": move_roc,
        "stress_origin_read": _stress_origin_read(move_roc, vix_roc),
        "changes_pct": {
            "vix": _build_changes(vix_entry),
            "move": _build_changes(move_entry),
            "gvz": _build_changes(gvz_entry),
            "ovx": _build_changes(ovx_entry),
        },
        "data_quality": {
            "vix": _get_status(vix_entry),
            "move": _get_status(move_entry),
            "gvz": _get_status(gvz_entry),
            "ovx": _get_status(ovx_entry),
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
    daily["volatility"] = build_volatility_block(raw_state)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
