"""Policy futures curve analytics from raw_state.json."""
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any, Dict, List, Optional

from Signals import state_paths
from Signals.json_utils import write_json


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _implied_rate(price: Optional[float]) -> Optional[float]:
    if price is None:
        return None
    return 100.0 - float(price)


def _month_label(ticker: str) -> Optional[str]:
    if not ticker.startswith("ZQ") or len(ticker) < 5:
        return None
    code = ticker[2]
    year = ticker[3:5]
    month_map = {
        "F": "Jan",
        "G": "Feb",
        "H": "Mar",
        "J": "Apr",
        "K": "May",
        "M": "Jun",
        "N": "Jul",
        "Q": "Aug",
        "U": "Sep",
        "V": "Oct",
        "X": "Nov",
        "Z": "Dec",
    }
    month = month_map.get(code)
    if month is None:
        return None
    return f"{month} {year}"


def _current_policy_rate(raw_state: Dict[str, Any]) -> Optional[float]:
    policy = raw_state.get("policy", {})
    if not isinstance(policy, dict):
        return None
    entry = policy.get("effr", {})
    if not isinstance(entry, dict):
        return None
    meta = entry.get("meta", {})
    value = meta.get("current", entry.get("value"))
    return None if value is None else float(value)


def build_policy_futures_curve(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    futures = raw_state.get("policy_futures", {})
    zq = futures.get("zq", {}) if isinstance(futures, dict) else {}
    contracts: List[Dict[str, Any]] = []
    tenors: List[str] = []
    labels: List[Optional[str]] = []
    start_of_year = []
    last_month = []
    last_week = []
    current = []
    change_vs_now = []
    missing_count = 0
    any_failed = False
    spot_rate = _current_policy_rate(raw_state)

    for ticker, entry in zq.items():
        if not isinstance(entry, dict):
            continue
        meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
        start_price = meta.get("start_of_year")
        last_week_price = meta.get("last_week")
        current_price = meta.get("current", entry.get("value"))
        change_1m = meta.get("1m_change")
        last_month_price = None
        if current_price is not None and change_1m is not None:
            last_month_price = current_price - change_1m

        start_rate = _implied_rate(start_price)
        last_month_rate = _implied_rate(last_month_price)
        last_week_rate = _implied_rate(last_week_price)
        current_rate = _implied_rate(current_price)
        weekly_change_bps = None
        if current_rate is not None and last_week_rate is not None:
            weekly_change_bps = (current_rate - last_week_rate) * 100
        change_vs_now_bps = None
        if current_rate is not None and spot_rate is not None:
            change_vs_now_bps = (current_rate - spot_rate) * 100

        if entry.get("status") == "FAILED":
            any_failed = True
        if current_rate is None:
            missing_count += 1

        contracts.append(
            {
                "ticker": ticker,
                "label": _month_label(ticker),
                "status": entry.get("status"),
                "start_of_year_price": start_price,
                "last_month_price": last_month_price,
                "last_week_price": last_week_price,
                "current_price": current_price,
                "start_of_year_rate_pct": start_rate,
                "last_month_rate_pct": last_month_rate,
                "last_week_rate_pct": last_week_rate,
                "current_rate_pct": current_rate,
                "weekly_change_bps": weekly_change_bps,
                "change_vs_now_bps": change_vs_now_bps,
            }
        )

        tenors.append(ticker)
        labels.append(_month_label(ticker))
        start_of_year.append(start_rate)
        last_month.append(last_month_rate)
        last_week.append(last_week_rate)
        current.append(current_rate)
        change_vs_now.append(change_vs_now_bps)

    return {
        "as_of": _now_iso(),
        "contracts": contracts,
        "curve_lines": {
            "tenors": tenors,
            "labels": labels,
            "start_of_year": start_of_year,
            "last_month": last_month,
            "last_week": last_week,
            "current": current,
            "change_vs_now_bps": change_vs_now,
            "spot_rate": spot_rate,
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
    write_json(daily_path, daily)
    return daily
