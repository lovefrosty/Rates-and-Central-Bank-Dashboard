"""Volatility analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional


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
    vix_value = _get_value(vix_entry)
    move_value = _get_value(move_entry)
    vix_roc = _get_5d_roc(vix_entry)
    move_roc = _get_5d_roc(move_entry)

    return {
        "vix": vix_value,
        "move": move_value,
        "vix_5d_roc": vix_roc,
        "move_5d_roc": move_roc,
        "stress_origin_read": _stress_origin_read(move_roc, vix_roc),
        "data_quality": {
            "vix": _get_status(vix_entry),
            "move": _get_status(move_entry),
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
    daily["volatility"] = build_volatility_block(raw_state)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
