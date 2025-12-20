"""Resolve yield curve regime from signals/raw_state.json.

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
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Dict, Tuple


_OK_STATUSES = {"OK", "FALLBACK"}


@dataclass(frozen=True)
class YieldInputs:
    y3m: float
    y2y: float
    y10y: float


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_ingestion_value(raw: Dict, section: str, key: str) -> float:
    section_obj = raw.get(section)
    if not isinstance(section_obj, dict):
        raise ValueError(f"missing section: {section}")
    item = section_obj.get(key)
    if not isinstance(item, dict):
        raise ValueError(f"missing ingestion: {section}.{key}")
    status = item.get("status")
    if status not in _OK_STATUSES:
        raise ValueError(f"invalid status for {section}.{key}: {status}")
    value = item.get("value")
    if not isinstance(value, (int, float)):
        raise ValueError(f"invalid value for {section}.{key}: {value}")
    return float(value)


def _extract_yields(raw: Dict) -> YieldInputs:
    y3m = _require_ingestion_value(raw, "duration", "y3m_nominal")
    y2y = _require_ingestion_value(raw, "duration", "y2y_nominal")
    y10y = _require_ingestion_value(raw, "duration", "y10_nominal")
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


def build_yield_curve_state(raw: Dict) -> Dict[str, object]:
    inputs = _extract_yields(raw)
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
    raw_state_path: Path | str = Path("signals/raw_state.json"),
    daily_state_path: Path | str = Path("Signals/daily_state.json"),
) -> Dict[str, object]:
    raw_state_path = Path(raw_state_path)
    daily_state_path = Path(daily_state_path)
    raw = json.loads(raw_state_path.read_text(encoding="utf-8"))
    resolved = build_yield_curve_state(raw)

    daily_state = {}
    if daily_state_path.exists():
        daily_state = json.loads(daily_state_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(daily_state, dict):
            daily_state = {}
    daily_state["meta"] = {"generated_at": _now_iso()}
    daily_state["yield_curve"] = resolved

    daily_state_path.parent.mkdir(parents=True, exist_ok=True)
    daily_state_path.write_text(
        json.dumps(daily_state, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return daily_state


def main() -> None:
    resolve_yield_curve()


if __name__ == "__main__":
    main()
