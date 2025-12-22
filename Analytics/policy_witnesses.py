"""Policy witnesses analytics from raw_state.json."""
from pathlib import Path
import json
from typing import Any, Dict


def _get_current(entry: Dict[str, Any]) -> Any:
    if not isinstance(entry, dict):
        return None
    meta = entry.get("meta", {})
    return meta.get("current", entry.get("value"))


def build_policy_witnesses(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    policy = raw_state.get("policy", {})
    witnesses = raw_state.get("policy_witnesses", {})
    sofr_entry = witnesses.get("sofr", {}) if isinstance(witnesses, dict) else {}
    effr_entry = policy.get("effr", {}) if isinstance(policy, dict) else {}

    sofr_current = _get_current(sofr_entry)
    effr_current = _get_current(effr_entry)
    spread_bps = None
    if sofr_current is not None and effr_current is not None:
        spread_bps = (effr_current - sofr_current) * 100

    data_quality = {
        "sofr_status": sofr_entry.get("status"),
        "effr_status": effr_entry.get("status"),
        "sofr_missing": sofr_current is None,
        "effr_missing": effr_current is None,
    }
    return {
        "sofr_current": sofr_current,
        "effr_sofr_spread_bps": spread_bps,
        "data_quality": data_quality,
    }


def write_daily_state(
    raw_state_path: Path | str = Path("signals/raw_state.json"),
    daily_state_path: Path | str = Path("signals/daily_state.json"),
) -> Dict[str, Any]:
    raw_state = json.loads(Path(raw_state_path).read_text(encoding="utf-8"))
    daily_path = Path(daily_state_path)
    daily = {"policy_witnesses": build_policy_witnesses(raw_state)}
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
