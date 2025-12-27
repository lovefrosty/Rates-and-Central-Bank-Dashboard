"""Global policy alignment analytics from raw_state.json."""
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


def _extract_current(entry: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    status = entry.get("status") if isinstance(entry, dict) else None
    if status == "FAILED":
        return None, None, status
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    current = meta.get("current", entry.get("value"))
    last_week = meta.get("last_week")
    return current, last_week, status


def _quality(current: Optional[float], last_week: Optional[float], status: Optional[str]) -> str:
    if status == "FAILED" or (current is None and last_week is None):
        return "FAILED"
    if current is not None and last_week is not None:
        return "OK"
    return "PARTIAL"


def _weekly_change(current: Optional[float], last_week: Optional[float]) -> Optional[float]:
    if current is None or last_week is None:
        return None
    return current - last_week


def _boj_quality(entry: Dict[str, Any]) -> Tuple[Optional[str], str]:
    status = entry.get("status") if isinstance(entry, dict) else None
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    stance = meta.get("stance")
    if status == "FAILED":
        return None, "FAILED"
    if stance is None:
        return None, "PARTIAL"
    return stance, "OK"


def build_global_policy_alignment(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    ecb_entry = _get_entry(raw_state, "global_policy", "ecb_deposit_rate")
    usd_entry = _get_entry(raw_state, "global_policy", "usd_index")
    boj_entry = _get_entry(raw_state, "global_policy", "boj_stance")
    effr_entry = _get_entry(raw_state, "policy", "effr")

    ecb_current, ecb_last_week, ecb_status = _extract_current(ecb_entry)
    usd_current, usd_last_week, usd_status = _extract_current(usd_entry)
    effr_current, _, _ = _extract_current(effr_entry)
    boj_stance, boj_quality = _boj_quality(boj_entry)

    rate_diff = None
    if ecb_current is not None and effr_current is not None:
        rate_diff = effr_current - ecb_current

    return {
        "ecb_rate_current": ecb_current,
        "usd_index_current": usd_current,
        "usd_index_weekly_change": _weekly_change(usd_current, usd_last_week),
        "boj_stance": boj_stance,
        "rate_diff": rate_diff,
        "data_quality": {
            "ecb_rate": _quality(ecb_current, ecb_last_week, ecb_status),
            "usd_index": _quality(usd_current, usd_last_week, usd_status),
            "boj_stance": boj_quality,
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
    daily["global_policy_alignment"] = build_global_policy_alignment(raw_state)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
