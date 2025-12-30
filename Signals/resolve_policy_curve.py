"""Policy curve resolver: 6–12 month expectation from daily_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths
from Signals.json_utils import write_json


def _get_block(daily_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    block = daily_state.get(key, {})
    return block if isinstance(block, dict) else {}


def _get_value(block: Dict[str, Any], key: str) -> Optional[float]:
    value = block.get(key)
    return None if value is None else float(value)


def _get_text(block: Dict[str, Any], key: str) -> Optional[str]:
    value = block.get(key)
    return value if isinstance(value, str) else None


def _get_breakeven_change(inflation: Dict[str, Any]) -> Optional[float]:
    for key in ("breakeven_10y_change", "breakeven_10y_1m_change"):
        if key in inflation:
            value = inflation.get(key)
            return None if value is None else float(value)
    return None


def _base_direction(breakeven_change: Optional[float]) -> str:
    if breakeven_change is None:
        return "Hold"
    if breakeven_change > 0:
        return "Tightening"
    if breakeven_change < 0:
        return "Easing"
    return "Hold"


def _get_policy_proxy(daily_state: Dict[str, Any]) -> Optional[float]:
    for block_key, value_key in (
        ("yield_expectations", "policy_pricing_proxy"),
        ("yield_expectations", "forward_policy_proxy"),
        ("yield_curve", "policy_pricing_proxy"),
        ("yield_curve", "forward_policy_proxy"),
    ):
        block = _get_block(daily_state, block_key)
        if value_key in block:
            value = block.get(value_key)
            return None if value is None else float(value)
    return None


def _inputs_used(breakeven_change: Optional[float], proxy: Optional[float], stress: Optional[str]) -> Dict[str, bool]:
    return {
        "inflation_expectations": breakeven_change is not None,
        "policy_pricing": proxy is not None,
        "volatility": stress is not None,
    }


def _explanation(
    direction: str,
    breakeven_change: Optional[float],
    proxy: Optional[float],
    sofr: Optional[float],
    stress: Optional[str],
    missing_any: bool,
) -> str:
    parts = []
    if breakeven_change is None:
        parts.append("Inflation expectations trend is unavailable, so the bias defaults to hold.")
    elif breakeven_change > 0:
        parts.append("Inflation expectations are rising, pointing toward tightening.")
    elif breakeven_change < 0:
        parts.append("Inflation expectations are declining, pointing toward easing.")
    else:
        parts.append("Inflation expectations are stable, pointing toward hold.")

    if proxy is not None and sofr is not None:
        if proxy < sofr:
            parts.append("Forward policy pricing sits below current SOFR, reinforcing easing bias.")
        elif proxy > sofr:
            parts.append("Forward policy pricing sits above current SOFR, reinforcing tightening bias.")
        else:
            parts.append("Forward policy pricing is aligned with SOFR.")
    else:
        parts.append("Forward policy pricing is unavailable.")

    if stress is not None:
        if stress == "Rates-led volatility":
            parts.append("Rates-led volatility reinforces tightening bias.")
        elif stress == "Equity-led volatility":
            parts.append("Equity-led volatility reinforces easing bias.")
        else:
            parts.append("Volatility signals do not alter the bias.")

    if missing_any:
        parts.append("Some inputs are missing, so the read is less complete.")
    return " ".join(parts)


def resolve_policy_curve(daily_state_path: Path | str = state_paths.DAILY_STATE_PATH) -> Dict[str, Any]:
    path = Path(daily_state_path)
    daily_state = json.loads(path.read_text(encoding="utf-8"))

    inflation = _get_block(daily_state, "inflation_real_rates")
    witnesses = _get_block(daily_state, "policy_witnesses")
    volatility = _get_block(daily_state, "volatility")

    breakeven_change = _get_breakeven_change(inflation)
    proxy = _get_policy_proxy(daily_state)
    sofr = _get_value(witnesses, "sofr_current")
    stress = _get_text(volatility, "stress_origin_read")

    direction = _base_direction(breakeven_change)
    inputs_used = _inputs_used(breakeven_change, proxy, stress)
    missing_any = not all(inputs_used.values())

    policy_curve = {
        "expected_direction": direction,
        "horizon": "6–12 months",
        "explanation": _explanation(direction, breakeven_change, proxy, sofr, stress, missing_any),
        "inputs_used": inputs_used,
    }

    daily_state["policy_curve"] = policy_curve
    write_json(path, daily_state)
    return daily_state
