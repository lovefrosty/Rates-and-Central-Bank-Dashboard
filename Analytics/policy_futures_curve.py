"""Policy futures curve analytics from raw_state.json."""
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any, Dict, List, Optional

from Signals import state_paths


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _implied_rate(price: Optional[float]) -> Optional[float]:
    if price is None:
        return None
    return 100.0 - float(price)


def build_policy_futures_curve(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    futures = raw_state.get("policy_futures", {})
    zq = futures.get("zq", {}) if isinstance(futures, dict) else {}
    contracts: List[Dict[str, Any]] = []
    tenors: List[str] = []
    start_of_year = []
    last_week = []
    current = []
    missing_count = 0
    any_failed = False

    for ticker, entry in zq.items():
        if not isinstance(entry, dict):
            continue
        meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
        start_price = meta.get("start_of_year")
        last_week_price = meta.get("last_week")
        current_price = meta.get("current", entry.get("value"))

        start_rate = _implied_rate(start_price)
        last_week_rate = _implied_rate(last_week_price)
        current_rate = _implied_rate(current_price)
        weekly_change_bps = None
        if current_rate is not None and last_week_rate is not None:
            weekly_change_bps = (current_rate - last_week_rate) * 100

        if entry.get("status") == "FAILED":
            any_failed = True
        if current_rate is None:
            missing_count += 1

        contracts.append(
            {
                "ticker": ticker,
                "start_of_year_price": start_price,
                "last_week_price": last_week_price,
                "current_price": current_price,
                "start_of_year_rate_pct": start_rate,
                "last_week_rate_pct": last_week_rate,
                "current_rate_pct": current_rate,
                "weekly_change_bps": weekly_change_bps,
            }
        )

        tenors.append(ticker)
        start_of_year.append(start_rate)
        last_week.append(last_week_rate)
        current.append(current_rate)

    return {
        "as_of": _now_iso(),
        "contracts": contracts,
        "curve_lines": {
            "tenors": tenors,
            "start_of_year": start_of_year,
            "last_week": last_week,
            "current": current,
        },
        "data_quality": {
            "any_failed": any_failed,
            "missing_count": missing_count,
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
    daily["policy_futures_curve"] = build_policy_futures_curve(raw_state)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.write_text(json.dumps(daily, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily
