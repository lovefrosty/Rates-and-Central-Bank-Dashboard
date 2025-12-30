"""Resolve yield curve regime from signals/daily_state.json.

Rules (nominal yields, in percentage points):
- Compute slopes:
  - s3m_2y = y2y - y3m
  - s2y_10y = y10y - y2y
  - s3m_10y = y10y - y3m
- Relative moves:
  - belly_vs_front = s3m_2y
  - long_vs_belly = s2y_10y
  - long_vs_front = s3m_10y
  - curvature = s2y_10y - s3m_2y
- Regime classification:
  - INVERTED: s3m_10y <= -0.10
  - FLAT: abs(s3m_10y) <= 0.10
  - STEEP: s3m_10y >= 0.75 and s2y_10y >= 0.25
  - NORMAL: otherwise
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict

from Signals import state_paths
from Signals.json_utils import write_json


@dataclass(frozen=True)
class YieldInputs:
    y3m: float
    y2y: float
    y10y: float


def _get_block(daily_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    block = daily_state.get(key, {})
    return block if isinstance(block, dict) else {}


def _current_curve_map(curve_block: Dict[str, Any]) -> Dict[str, float]:
    tenors = curve_block.get("tenors")
    lines = curve_block.get("lines")
    if not isinstance(tenors, list) or not isinstance(lines, dict):
        raise ValueError("yield_curve.tenors or yield_curve.lines missing")
    current = lines.get("current")
    if not isinstance(current, list):
        raise ValueError("yield_curve.lines.current missing")
    if len(tenors) != len(current):
        raise ValueError("yield_curve tenors/current length mismatch")
    mapping: Dict[str, float] = {}
    for tenor, value in zip(tenors, current):
        if value is None:
            continue
        if not isinstance(value, (int, float)):
            continue
        mapping[tenor] = float(value)
    return mapping


def _require_tenor_value(mapping: Dict[str, float], tenor: str) -> float:
    if tenor not in mapping:
        raise ValueError(f"missing current yield for {tenor}")
    return mapping[tenor]


def _extract_yields(curve_block: Dict[str, Any]) -> YieldInputs:
    current_map = _current_curve_map(curve_block)
    y3m = _require_tenor_value(current_map, "3M")
    y2y = _require_tenor_value(current_map, "2Y")
    y10y = _require_tenor_value(current_map, "10Y")
    return YieldInputs(y3m=y3m, y2y=y2y, y10y=y10y)


def _compute_slopes(yields: YieldInputs) -> Dict[str, float]:
    s3m_2y = yields.y2y - yields.y3m
    s2y_10y = yields.y10y - yields.y2y
    s3m_10y = yields.y10y - yields.y3m
    return {"s3m_2y": s3m_2y, "s2y_10y": s2y_10y, "s3m_10y": s3m_10y}


def _compute_relative_moves(slopes: Dict[str, float]) -> Dict[str, float]:
    s3m_2y = slopes["s3m_2y"]
    s2y_10y = slopes["s2y_10y"]
    s3m_10y = slopes["s3m_10y"]
    return {
        "belly_vs_front": s3m_2y,
        "long_vs_belly": s2y_10y,
        "long_vs_front": s3m_10y,
        "curvature": s2y_10y - s3m_2y,
    }


def _determine_regime(slopes: Dict[str, float]) -> str:
    s3m_10y = slopes["s3m_10y"]
    s2y_10y = slopes["s2y_10y"]
    if s3m_10y <= -0.10:
        return "INVERTED"
    if abs(s3m_10y) <= 0.10:
        return "FLAT"
    if s3m_10y >= 0.75 and s2y_10y >= 0.25:
        return "STEEP"
    return "NORMAL"


def build_yield_curve_state(curve_block: Dict[str, Any]) -> Dict[str, object]:
    inputs = _extract_yields(curve_block)
    slopes = _compute_slopes(inputs)
    relative_moves = _compute_relative_moves(slopes)
    regime = _determine_regime(slopes)
    return {
        "inputs": {"y3m": inputs.y3m, "y2y": inputs.y2y, "y10y": inputs.y10y},
        "slopes": slopes,
        "relative_moves": relative_moves,
        "regime": regime,
    }


def resolve_yield_curve(
    daily_state_path: Path | str = state_paths.DAILY_STATE_PATH,
    raw_state_path: Path | str = state_paths.RAW_STATE_PATH,
) -> Dict[str, object]:
    daily_state_path = Path(daily_state_path)
    daily_state = json.loads(daily_state_path.read_text(encoding="utf-8"))

    curve_block = _get_block(daily_state, "yield_curve")
    resolved = build_yield_curve_state(curve_block)
    merged = dict(curve_block)
    merged.update(resolved)
    daily_state["yield_curve"] = merged

    daily_state_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(daily_state_path, daily_state)
    return daily_state


def main() -> None:
    resolve_yield_curve()


if __name__ == "__main__":
    main()
