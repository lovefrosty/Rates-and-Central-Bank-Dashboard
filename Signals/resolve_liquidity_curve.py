"""Liquidity curve resolver: weeks–months liquidity direction from daily_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths


def _get_block(daily_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    block = daily_state.get(key, {})
    return block if isinstance(block, dict) else {}


def _get_value(block: Dict[str, Any], key: str) -> Optional[float]:
    value = block.get(key)
    return None if value is None else float(value)


def _get_liquidity_metric(daily_state: Dict[str, Any], series: str, key: str) -> Optional[float]:
    analytics = _get_block(daily_state, "liquidity_analytics")
    series_block = analytics.get(series, {}) if isinstance(analytics, dict) else {}
    if not isinstance(series_block, dict):
        return None
    return _get_value(series_block, key)


def _expected_liquidity(rrp_change: Optional[float]) -> str:
    if rrp_change is None:
        return "Neutral"
    if rrp_change < 0:
        return "Injecting"
    if rrp_change > 0:
        return "Draining"
    return "Neutral"


def _inputs_used(
    rrp_change: Optional[float],
    rrp_level: Optional[float],
    tga_change: Optional[float],
    tga_level: Optional[float],
) -> Dict[str, bool]:
    return {
        "rrp": rrp_change is not None or rrp_level is not None,
        "tga": tga_change is not None or tga_level is not None,
    }


def _explanation(
    expected: str,
    rrp_change: Optional[float],
    rrp_level: Optional[float],
    tga_change: Optional[float],
    tga_level: Optional[float],
    missing_any: bool,
) -> str:
    parts = []
    if rrp_change is None:
        if rrp_level is not None:
            parts.append(
                f"RRP balances are {rrp_level:.1f}, but the weekly change is unavailable, so the liquidity read defaults to neutral."
            )
        else:
            parts.append("RRP change data is unavailable, so the liquidity read defaults to neutral.")
    else:
        if expected == "Injecting":
            parts.append("Liquidity is injecting as balances leave the Fed's reverse repo facility.")
        elif expected == "Draining":
            parts.append("Liquidity is draining as balances move into the Fed's reverse repo facility.")
        else:
            parts.append("Reverse repo balances are flat, leaving liquidity conditions neutral.")

    if tga_change is not None:
        if tga_change < 0:
            parts.append("Treasury cash drawdowns reinforce the injecting signal.")
        elif tga_change > 0:
            parts.append("Rising Treasury cash balances reinforce the draining signal.")
        else:
            parts.append("Treasury cash balances are flat and do not reinforce the signal.")
    elif tga_level is not None:
        parts.append("Treasury cash level is available, but the weekly change is unavailable.")

    if missing_any:
        parts.append("Some liquidity inputs are missing, so the read is less complete.")

    parts.append("This is a liquidity transmission read, separate from policy stance.")
    return " ".join(parts)


def resolve_liquidity_curve(daily_state_path: Path | str = state_paths.DAILY_STATE_PATH) -> Dict[str, Any]:
    path = Path(daily_state_path)
    daily_state = json.loads(path.read_text(encoding="utf-8"))

    rrp_level = _get_liquidity_metric(daily_state, "rrp", "level")
    rrp_change = _get_liquidity_metric(daily_state, "rrp", "change_1w")
    tga_level = _get_liquidity_metric(daily_state, "tga", "level")
    tga_change = _get_liquidity_metric(daily_state, "tga", "change_1w")

    expected = _expected_liquidity(rrp_change)
    inputs_used = _inputs_used(rrp_change, rrp_level, tga_change, tga_level)
    missing_any = any(value is None for value in (rrp_level, rrp_change, tga_level, tga_change))

    liquidity_curve = {
        "expected_liquidity": expected,
        "horizon": "weeks–months",
        "explanation": _explanation(expected, rrp_change, rrp_level, tga_change, tga_level, missing_any),
        "inputs_used": inputs_used,
    }

    daily_state["liquidity_curve"] = liquidity_curve
    path.write_text(json.dumps(daily_state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily_state
