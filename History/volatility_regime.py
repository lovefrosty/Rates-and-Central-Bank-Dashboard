"""Volatility regime labels derived from history_state.json."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from Signals import state_paths
from Signals.json_utils import write_json


THRESHOLDS = (-0.5, 0.5, 1.5)
BOUNDARY_TOLERANCE = 0.1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _latest_value(block: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    dates = block.get("dates", []) if isinstance(block, dict) else []
    values = block.get("values", []) if isinstance(block, dict) else []
    if not isinstance(dates, list) or not isinstance(values, list):
        return None, None
    for dt, val in zip(reversed(dates), reversed(values)):
        if val is None:
            continue
        try:
            return float(val), dt
        except (TypeError, ValueError):
            continue
    return None, None


def _boundary_detail(value: Optional[float]) -> Optional[Dict[str, float]]:
    if value is None:
        return None
    for threshold in THRESHOLDS:
        if abs(value - threshold) <= BOUNDARY_TOLERANCE:
            return {"threshold": float(threshold), "observed": float(value)}
    return None


def _classify_regime(value: Optional[float]) -> Tuple[str, bool, Optional[Dict[str, float]]]:
    if value is None:
        return "UNAVAILABLE", False, None
    detail = _boundary_detail(value)
    if detail:
        return "TRANSITION", True, detail
    if value < THRESHOLDS[0]:
        return "Calm", False, None
    if value <= THRESHOLDS[1]:
        return "Normal", False, None
    if value <= THRESHOLDS[2]:
        return "Elevated", False, None
    return "Stress", False, None


def _joint_regime(
    vix_value: Optional[float],
    move_value: Optional[float],
    vix_boundary: bool,
    move_boundary: bool,
) -> str:
    if vix_value is None or move_value is None:
        return "UNAVAILABLE"
    if vix_boundary or move_boundary:
        return "TRANSITION"
    vix_stress = vix_value > THRESHOLDS[2]
    move_stress = move_value > THRESHOLDS[2]
    if vix_stress and move_stress:
        return "Systemic stress"
    if vix_stress and not move_stress:
        return "Equity-led stress"
    if move_stress and not vix_stress:
        return "Rates-led stress"
    if vix_value <= THRESHOLDS[1] and move_value <= THRESHOLDS[1]:
        return "Broad calm"
    return "Mixed"


def build_volatility_regime(history_state: Dict[str, Any]) -> Dict[str, Any]:
    transforms = history_state.get("transforms", {}) if isinstance(history_state, dict) else {}
    vix_block = transforms.get("vix", {}).get("zscore_3y", {}) if isinstance(transforms, dict) else {}
    move_block = transforms.get("move", {}).get("zscore_3y", {}) if isinstance(transforms, dict) else {}

    vix_value, vix_date = _latest_value(vix_block)
    move_value, move_date = _latest_value(move_block)

    vix_label, vix_boundary, vix_detail = _classify_regime(vix_value)
    move_label, move_boundary, move_detail = _classify_regime(move_value)
    joint_label = _joint_regime(vix_value, move_value, vix_boundary, move_boundary)

    boundary_case = vix_boundary or move_boundary
    boundary_detail: Dict[str, Any] = {}
    if vix_detail:
        boundary_detail["equity"] = vix_detail
    if move_detail:
        boundary_detail["rates"] = move_detail

    return {
        "equity": vix_label,
        "rates": move_label,
        "joint": joint_label,
        "inputs_used": ["VIX_z_3Y", "MOVE_z_3Y"],
        "window_used": "3Y",
        "computed_at": _now_iso(),
        "boundary_case": boundary_case,
        "boundary_detail": boundary_detail,
        "data_quality": {
            "equity": "OK" if vix_value is not None else "FAILED",
            "rates": "OK" if move_value is not None else "FAILED",
        },
        "zscore_3y": {"vix": vix_value, "move": move_value},
        "as_of": {"vix": vix_date, "move": move_date},
    }


def write_daily_state(
    history_state_path: Path | str = state_paths.HISTORY_STATE_PATH,
    daily_state_path: Path | str = state_paths.DAILY_STATE_PATH,
) -> Dict[str, Any]:
    history_path = Path(history_state_path)
    history_state: Dict[str, Any] = {}
    if history_path.exists():
        history_state = json.loads(history_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(history_state, dict):
            history_state = {}
    daily_path = Path(daily_state_path)
    daily: Dict[str, Any] = {}
    if daily_path.exists():
        daily = json.loads(daily_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(daily, dict):
            daily = {}
    daily["volatility_regime"] = build_volatility_regime(history_state)
    write_json(daily_path, daily)
    return daily


if __name__ == "__main__":
    write_daily_state()
