"""FX volatility diagnostics derived from history_state.json."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from Signals import state_paths
from Signals.json_utils import write_json


FX_VOL_SERIES = {
    "dxy": "DXY",
    "eurusd": "EURUSD",
    "gbpusd": "GBPUSD",
    "usdjpy": "USDJPY",
    "usdcad": "USDCAD",
    "audusd": "AUDUSD",
    "usdchf": "USDCHF",
    "usdcnh": "USDCNH",
}
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


def build_fx_volatility(history_state: Dict[str, Any]) -> Dict[str, Any]:
    transforms = history_state.get("transforms", {}) if isinstance(history_state, dict) else {}
    entries = []
    boundary_flags = []
    for key, label in FX_VOL_SERIES.items():
        series_block = transforms.get(key, {}) if isinstance(transforms, dict) else {}
        vol_block = series_block.get("realized_vol_20d_pct", {})
        z_block = series_block.get("realized_vol_20d_zscore_3y", {})
        vol_value, vol_date = _latest_value(vol_block)
        z_value, z_date = _latest_value(z_block)
        regime, boundary_case, boundary_detail = _classify_regime(z_value)
        boundary_flags.append(boundary_case)
        if vol_value is None and z_value is None:
            quality = "FAILED"
        elif vol_value is not None and z_value is not None:
            quality = "OK"
        else:
            quality = "PARTIAL"
        entries.append(
            {
                "pair": label,
                "realized_vol_20d_pct": vol_value,
                "zscore_3y": z_value,
                "regime": regime,
                "data_quality": quality,
                "boundary_case": boundary_case,
                "boundary_detail": boundary_detail,
                "as_of": {"vol": vol_date, "zscore": z_date},
            }
        )

    qualities = {entry["data_quality"] for entry in entries}
    if qualities == {"OK"}:
        overall = "OK"
    elif qualities == {"FAILED"}:
        overall = "FAILED"
    else:
        overall = "PARTIAL"

    return {
        "vol_type": "realized_20d",
        "window_used": "3Y",
        "computed_at": _now_iso(),
        "inputs_used": ["realized_vol_20d_pct", "realized_vol_20d_zscore_3y"],
        "data_quality": overall,
        "entries": entries,
        "boundary_case": any(boundary_flags),
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
    daily["fx_volatility"] = build_fx_volatility(history_state)
    write_json(daily_path, daily)
    return daily


if __name__ == "__main__":
    write_daily_state()
