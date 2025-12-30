"""Policy futures curve analytics from raw_state.json."""
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any, Dict, List, Optional

from Signals import state_paths
from Signals.json_utils import write_json


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def build_policy_futures_curve(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    futures = raw_state.get("policy_futures", {})
    zq = futures.get("zq", {}) if isinstance(futures, dict) else {}
    contracts: List[Dict[str, Any]] = []
    tenors: List[str] = []
    labels: List[Optional[str]] = []
    start_of_year = []
    last_6m = []
    last_month = []
    last_week = []
    current = []
    missing_count = 0
    any_failed = False

    for ticker, entry in zq.items():
        if not isinstance(entry, dict):
            continue
        meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
        start_price = meta.get("start_of_year")
        last_6m_price = meta.get("last_6m")
        last_week_price = meta.get("last_week")
        current_price = meta.get("current", entry.get("value"))
        last_month_price = meta.get("last_month")

        if entry.get("status") == "FAILED":
            any_failed = True
        if current_price is None:
            missing_count += 1

        contracts.append(
            {
                "ticker": ticker,
                "label": _month_label(ticker),
                "status": entry.get("status"),
                "start_of_year_price": start_price,
                "last_6m_price": last_6m_price,
                "last_month_price": last_month_price,
                "last_week_price": last_week_price,
                "current_price": current_price,
            }
        )

        tenors.append(ticker)
        labels.append(_month_label(ticker))
        start_of_year.append(start_price)
        last_6m.append(last_6m_price)
        last_month.append(last_month_price)
        last_week.append(last_week_price)
        current.append(current_price)

    return {
        "as_of": _now_iso(),
        "contracts": contracts,
        "curve_lines": {
            "tenors": tenors,
            "labels": labels,
            "start_of_year": start_of_year,
            "last_6m": last_6m,
            "last_month": last_month,
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
    write_json(daily_path, daily)
    return daily
