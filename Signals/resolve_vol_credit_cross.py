"""Resolve cross-market stress origin using daily_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional, Tuple

from Signals import state_paths


def _get_block(daily_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    block = daily_state.get(key, {})
    return block if isinstance(block, dict) else {}


def _get_number(block: Dict[str, Any], key: str) -> Optional[float]:
    value = block.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_label(vix_roc: Optional[float], hy_change_bps: Optional[float]) -> Tuple[str, str]:
    if vix_roc is None or hy_change_bps is None:
        return "UNAVAILABLE", "Inputs are incomplete, so cross-market stress cannot be evaluated."
    if vix_roc <= 0 and hy_change_bps <= 0:
        return "NO_STRESS", "Neither equity volatility nor credit spreads are rising materially."
    if vix_roc > 0 and hy_change_bps <= 0:
        return "MARKET_LED", "Equity volatility is rising while credit spreads are not widening."
    if hy_change_bps > 0 and vix_roc <= 0:
        return "CREDIT_LED", "Credit spreads are widening while equity volatility is not rising."
    return "UNAVAILABLE", "Both equity volatility and credit spreads are rising, so the lead is unclear."


def resolve_vol_credit_cross(daily_state_path: Path | str = state_paths.DAILY_STATE_PATH) -> Dict[str, Any]:
    path = Path(daily_state_path)
    daily_state = json.loads(path.read_text(encoding="utf-8"))

    volatility = _get_block(daily_state, "volatility")
    credit = _get_block(daily_state, "credit_transmission")

    vix_roc = _get_number(volatility, "vix_5d_roc")
    hy_change_bps = _get_number(credit, "hy_oas_weekly_change_bps")

    label, explanation = _resolve_label(vix_roc, hy_change_bps)

    daily_state["vol_credit_cross"] = {
        "label": label,
        "explanation": explanation,
    }
    path.write_text(json.dumps(daily_state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily_state
