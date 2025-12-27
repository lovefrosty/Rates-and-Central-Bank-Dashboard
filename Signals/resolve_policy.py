"""Policy resolver: spot-only stance from daily_state.json."""
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


def _get_text(block: Dict[str, Any], key: str) -> Optional[str]:
    value = block.get(key)
    return value if isinstance(value, str) else None


def _base_stance(real_10y: Optional[float]) -> str:
    if real_10y is None:
        return "Neutral"
    if real_10y >= 1.0:
        return "Restrictive"
    if real_10y <= 0.0:
        return "Accommodative"
    return "Neutral"


def _apply_funding_tilt(stance: str, real_10y: Optional[float], spread_bps: Optional[float]) -> str:
    if real_10y is None:
        return stance
    if real_10y <= 0.0 or real_10y >= 1.0:
        return stance
    if spread_bps is None:
        return stance
    if spread_bps > 10:
        return "Restrictive"
    if spread_bps < -5:
        return "Accommodative"
    return stance


def _inputs_used(real_10y: Optional[float], sofr: Optional[float], spread_bps: Optional[float], stress: Optional[str]) -> Dict[str, bool]:
    return {
        "real_10y": real_10y is not None,
        "sofr": sofr is not None,
        "effr_sofr_spread": spread_bps is not None,
        "volatility": stress is not None,
    }


def _explanation(
    stance: str,
    real_10y: Optional[float],
    sofr: Optional[float],
    spread_bps: Optional[float],
    stress: Optional[str],
    missing_any: bool,
) -> str:
    parts = []
    if real_10y is not None:
        parts.append(f"Real 10Y rates anchor a {stance.lower()} stance ({real_10y:.2f}).")
    else:
        parts.append("Real 10Y rates are unavailable, limiting the anchor.")

    if spread_bps is not None:
        if spread_bps > 10:
            parts.append(f"Funding stress shows in EFFR-SOFR at {spread_bps:.1f} bps.")
        elif spread_bps < -5:
            parts.append(f"Funding looks loose with EFFR-SOFR at {spread_bps:.1f} bps.")
        else:
            parts.append(f"Funding spread is modest at {spread_bps:.1f} bps.")
    elif sofr is not None:
        parts.append(f"SOFR is {sofr:.2f}, but the funding spread is unavailable.")
    else:
        parts.append("Funding inputs are incomplete.")

    if stress is not None:
        parts.append(f"Volatility signal: {stress}.")

    if missing_any:
        parts.append("Some inputs are missing, so the read is less complete.")
    return " ".join(parts)


def resolve_policy(daily_state_path: Path | str = state_paths.DAILY_STATE_PATH) -> Dict[str, Any]:
    path = Path(daily_state_path)
    daily_state = json.loads(path.read_text(encoding="utf-8"))

    policy_witnesses = _get_block(daily_state, "policy_witnesses")
    inflation = _get_block(daily_state, "inflation_real_rates")
    volatility = _get_block(daily_state, "volatility")

    real_10y = _get_value(inflation, "real_10y")
    sofr = _get_value(policy_witnesses, "sofr_current")
    spread_bps = _get_value(policy_witnesses, "effr_sofr_spread_bps")
    stress = _get_text(volatility, "stress_origin_read")

    stance = _base_stance(real_10y)
    stance = _apply_funding_tilt(stance, real_10y, spread_bps)
    inputs_used = _inputs_used(real_10y, sofr, spread_bps, stress)
    missing_any = not all(inputs_used.values())

    policy_block = {
        "spot_stance": stance,
        "explanation": _explanation(stance, real_10y, sofr, spread_bps, stress, missing_any),
        "inputs_used": inputs_used,
    }

    daily_state["policy"] = policy_block
    path.write_text(json.dumps(daily_state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return daily_state
