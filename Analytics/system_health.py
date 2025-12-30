"""System health analytics from raw_state.json."""
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from Signals import state_paths
from Signals.json_utils import write_json


BLOCKS = {
    "Rates": ("duration",),
    "Policy Futures": ("policy_futures",),
    "Volatility": ("volatility",),
    "Liquidity": ("liquidity",),
    "Labor": ("labor_market",),
    "Credit": ("credit_spreads",),
    "Global Policy": ("global_policy",),
    "FX": ("fx",),
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _iter_ingestion_items(section: Any) -> Iterable[Dict[str, Any]]:
    if not isinstance(section, dict):
        return []
    if "zq" in section and isinstance(section.get("zq"), dict):
        return list(section.get("zq", {}).values())
    return list(section.values())


def _block_status(section: Any) -> Tuple[str, int, int]:
    items = list(_iter_ingestion_items(section))
    if not items:
        return "FAILED", 0, 0
    total = len(items)
    failed = 0
    for item in items:
        if not isinstance(item, dict) or item.get("status") == "FAILED":
            failed += 1
    if failed == 0:
        return "OK", failed, total
    if failed == total:
        return "FAILED", failed, total
    return "PARTIAL", failed, total


def _age_seconds(generated_at: str | None) -> int | None:
    parsed = _parse_iso(generated_at)
    if parsed is None:
        return None
    delta = _now_utc() - parsed
    return int(delta.total_seconds())


def _age_human(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    minutes = seconds // 60
    hours = minutes // 60
    days = hours // 24
    if days:
        return f"{days}d {hours % 24}h"
    if hours:
        return f"{hours}h {minutes % 60}m"
    if minutes:
        return f"{minutes}m"
    return f"{seconds}s"


def build_system_health(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    meta = raw_state.get("meta", {}) if isinstance(raw_state, dict) else {}
    generated_at = meta.get("generated_at")
    blocks: Dict[str, Any] = {}
    failed_total = 0
    series_total = 0
    for label, (section_key,) in BLOCKS.items():
        section = raw_state.get(section_key, {})
        status, failed, total = _block_status(section)
        blocks[label] = {
            "status": status,
            "failed": failed,
            "total": total,
        }
        failed_total += failed
        series_total += total

    failed_list: list[str] = []
    for label, (section_key,) in BLOCKS.items():
        section = raw_state.get(section_key, {})
        for item in _iter_ingestion_items(section):
            if isinstance(item, dict) and item.get("status") == "FAILED":
                series_id = None
                meta = item.get("meta")
                if isinstance(meta, dict):
                    series_id = meta.get("series_id") or meta.get("ticker")
                failed_list.append(series_id or label)

    age_seconds = _age_seconds(generated_at)

    return {
        "generated_at": generated_at,
        "age_seconds": age_seconds,
        "age_human": _age_human(age_seconds),
        "failed_series": failed_total,
        "total_series": series_total,
        "blocks": blocks,
        "history_state_available": state_paths.HISTORY_STATE_PATH.exists(),
        "failed_series_list": failed_list,
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
    daily["system_health"] = build_system_health(raw_state)
    write_json(daily_path, daily)
    return daily
