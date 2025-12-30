"""FX panel analytics from raw_state.json."""
# NOTE: Evidence-only block. No resolver consumes this data in V1.
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths
from Signals.json_utils import write_json


FX_ORDER = [
    ("USDJPY", "usdjpy"),
    ("EURUSD", "eurusd"),
    ("GBPUSD", "gbpusd"),
    ("USDCAD", "usdcad"),
]


def _get_entry(raw_state: Dict[str, Any], section: str, key: str) -> Dict[str, Any]:
    container = raw_state.get(section, {})
    if not isinstance(container, dict):
        return {}
    entry = container.get(key, {})
    return entry if isinstance(entry, dict) else {}


def _anchors_from_meta(entry: Dict[str, Any]) -> Dict[str, Optional[float]]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    current = meta.get("current", entry.get("value"))
    last_week = meta.get("last_week")
    start_of_year = meta.get("start_of_year")
    change_1m_pct = meta.get("1m_change_pct")
    last_month = None
    if current is not None and change_1m_pct not in (None, -100):
        last_month = current / (1 + (change_1m_pct / 100))
    return {
        "current": None if current is None else float(current),
        "last_week": None if last_week is None else float(last_week),
        "last_month": None if last_month is None else float(last_month),
        "start_of_year": None if start_of_year is None else float(start_of_year),
    }


def _changes_pct(entry: Dict[str, Any]) -> Dict[str, Optional[float]]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    return {
        "1d": meta.get("1d_change_pct"),
        "5d": meta.get("5d_change_pct"),
        "1m": meta.get("1m_change_pct"),
        "6m": meta.get("6m_change_pct"),
    }


def _quality(entry: Dict[str, Any], anchors: Dict[str, Optional[float]]) -> str:
    status = entry.get("status") if isinstance(entry, dict) else None
    if status == "FAILED":
        return "FAILED"
    if all(value is not None for value in anchors.values()):
        return "OK"
    if any(value is not None for value in anchors.values()):
        return "PARTIAL"
    return "FAILED"


def _resolve_dxy(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    dxy_entry = _get_entry(raw_state, "global_policy", "dxy")
    dxy_anchors = _anchors_from_meta(dxy_entry)
    dxy_quality = _quality(dxy_entry, dxy_anchors)
    if dxy_quality == "FAILED":
        usd_entry = _get_entry(raw_state, "global_policy", "usd_index")
        usd_anchors = _anchors_from_meta(usd_entry)
        usd_quality = _quality(usd_entry, usd_anchors)
        return {
            "label": "USD Broad Index",
            "series_id": usd_entry.get("meta", {}).get("series_id"),
            "anchors": usd_anchors,
            "changes_pct": _changes_pct(usd_entry),
            "data_quality": usd_quality,
            "source": usd_entry.get("source"),
        }
    return {
        "label": "DXY",
        "series_id": dxy_entry.get("meta", {}).get("series_id"),
        "anchors": dxy_anchors,
        "changes_pct": _changes_pct(dxy_entry),
        "data_quality": dxy_quality,
        "source": dxy_entry.get("source"),
    }


def build_fx_panel(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    pairs = []
    for label, key in FX_ORDER:
        entry = _get_entry(raw_state, "fx", key)
        anchors = _anchors_from_meta(entry)
        pairs.append(
            {
                "pair": label,
                "series_id": entry.get("meta", {}).get("series_id"),
                "anchors": anchors,
                "data_quality": _quality(entry, anchors),
            }
        )

    dxy = _resolve_dxy(raw_state)

    return {
        "dxy": dxy,
        "pairs": pairs,
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
    daily["fx"] = build_fx_panel(raw_state)
    write_json(daily_path, daily)
    return daily
