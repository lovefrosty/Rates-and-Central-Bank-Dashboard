"""Disagreement detector: compare resolver outputs from daily_state.json."""
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths
from Signals.json_utils import write_json


def _get_block(daily_state: Dict[str, Any], key: str) -> Dict[str, Any]:
    block = daily_state.get(key, {})
    return block if isinstance(block, dict) else {}


def _get_text(block: Dict[str, Any], key: str) -> Optional[str]:
    value = block.get(key)
    return value if isinstance(value, str) else None


def _policy_vs_expectations(spot: Optional[str], expected: Optional[str]) -> Dict[str, Any]:
    if spot is None or expected is None:
        return {
            "flag": False,
            "explanation": "Inputs are incomplete, so policy-versus-expectations disagreement cannot be evaluated.",
        }
    flag = (spot == "Restrictive" and expected == "Easing") or (
        spot == "Accommodative" and expected == "Tightening"
    )
    if flag:
        explanation = "Spot policy is {} while expectations imply {}, indicating a divergence.".format(
            spot.lower(), expected.lower()
        )
    else:
        explanation = "Spot policy and expectations are broadly aligned."
    return {"flag": flag, "explanation": explanation}


def _policy_vs_liquidity(spot: Optional[str], liquidity: Optional[str]) -> Dict[str, Any]:
    if spot is None or liquidity is None:
        return {
            "flag": False,
            "explanation": "Inputs are incomplete, so policy-versus-liquidity disagreement cannot be evaluated.",
        }
    flag = (spot == "Restrictive" and liquidity == "Injecting") or (
        spot == "Accommodative" and liquidity == "Draining"
    )
    if flag:
        explanation = "Spot policy is {} while liquidity is {}, indicating a divergence.".format(
            spot.lower(), liquidity.lower()
        )
    else:
        explanation = "Spot policy and liquidity conditions are broadly aligned."
    return {"flag": flag, "explanation": explanation}


def _expectations_vs_liquidity(expected: Optional[str], liquidity: Optional[str]) -> Dict[str, Any]:
    if expected is None or liquidity is None:
        return {
            "flag": False,
            "explanation": "Inputs are incomplete, so expectations-versus-liquidity disagreement cannot be evaluated.",
        }
    flag = (expected == "Easing" and liquidity == "Draining") or (
        expected == "Tightening" and liquidity == "Injecting"
    )
    if flag:
        explanation = "Expectations imply {} while liquidity is {}, indicating a divergence.".format(
            expected.lower(), liquidity.lower()
        )
    else:
        explanation = "Expectations and liquidity conditions are broadly aligned."
    return {"flag": flag, "explanation": explanation}


def resolve_disagreements(daily_state_path: Path | str = state_paths.DAILY_STATE_PATH) -> Dict[str, Any]:
    path = Path(daily_state_path)
    daily_state = json.loads(path.read_text(encoding="utf-8"))

    policy = _get_block(daily_state, "policy")
    policy_curve = _get_block(daily_state, "policy_curve")
    liquidity_curve = _get_block(daily_state, "liquidity_curve")

    spot = _get_text(policy, "spot_stance")
    expected = _get_text(policy_curve, "expected_direction")
    liquidity = _get_text(liquidity_curve, "expected_liquidity")

    disagreements = {
        "policy_vs_expectations": _policy_vs_expectations(spot, expected),
        "policy_vs_liquidity": _policy_vs_liquidity(spot, liquidity),
        "expectations_vs_liquidity": _expectations_vs_liquidity(expected, liquidity),
    }

    daily_state["disagreements"] = disagreements
    write_json(path, daily_state)
    return daily_state
