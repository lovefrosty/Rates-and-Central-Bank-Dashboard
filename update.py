"""Orchestrator: collect ingestion objects and write signals/raw_state.json

Mechanical behavior only: call fetchers, build raw_state.json, add timestamps and
data_health summary. Must not interpret values.
"""
from datetime import datetime, timezone
import json
from typing import Dict
import os

from Data import fetch_policy, fetch_policy_witnesses, fetch_yields, fetch_vol, fetch_liquidity
from Signals.validate import validate_raw_state


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_call(fn):
    try:
        return fn()
    except Exception as e:
        # Return explicit failed ingestion object
        return {
            "value": None,
            "status": "FAILED",
            "source": None,
            "fetched_at": _now_iso(),
            "error": str(e),
            "meta": {},
        }


def compute_data_health(category: Dict[str, Dict]) -> str:
    statuses = [v.get("status") for v in category.values()]
    if all(s == "OK" for s in statuses):
        return "OK"
    if all(s == "FAILED" for s in statuses):
        return "FAILED"
    return "PARTIAL"


def build_raw_state() -> Dict:
    policy = {
        "effr": _safe_call(fetch_policy.fetch_effr),
        "cpi_yoy": _safe_call(fetch_policy.fetch_cpi_yoy),
    }

    duration = {
        "y3m_nominal": _safe_call(fetch_yields.fetch_y3m_nominal),
        "y6m_nominal": _safe_call(fetch_yields.fetch_y6m_nominal),
        "y1y_nominal": _safe_call(fetch_yields.fetch_y1y_nominal),
        "y2y_nominal": _safe_call(fetch_yields.fetch_y2y_nominal),
        "y3y_nominal": _safe_call(fetch_yields.fetch_y3y_nominal),
        "y5y_nominal": _safe_call(fetch_yields.fetch_y5y_nominal),
        "y7y_nominal": _safe_call(fetch_yields.fetch_y7y_nominal),
        "y10_nominal": _safe_call(fetch_yields.fetch_y10_nominal),
        "y10_real": _safe_call(fetch_yields.fetch_y10_real),
        "y20y_nominal": _safe_call(fetch_yields.fetch_y20y_nominal),
        "y30y_nominal": _safe_call(fetch_yields.fetch_y30y_nominal),
    }

    volatility = {
        "vix": _safe_call(fetch_vol.fetch_vix),
        "move": _safe_call(fetch_vol.fetch_move),
    }

    liquidity = {
        "rrp": _safe_call(fetch_liquidity.fetch_rrp),
        "rrp_level": _safe_call(fetch_liquidity.fetch_rrp_level),
        "tga_level": _safe_call(fetch_liquidity.fetch_tga_level),
        "walcl": _safe_call(fetch_liquidity.fetch_walcl),
    }

    policy_witnesses = {
        # Parallel addition: policy_witnesses
        "sofr": _safe_call(fetch_policy_witnesses.fetch_sofr),
    }

    raw = {
        "meta": {
            "generated_at": _now_iso(),
            "data_health": {
                "policy": compute_data_health(policy),
                "duration": compute_data_health(duration),
                "volatility": compute_data_health(volatility),
                "liquidity": compute_data_health(liquidity),
            },
        },
        "policy": policy,
        "policy_witnesses": policy_witnesses,
        "duration": duration,
        "volatility": volatility,
        "liquidity": liquidity,
    }

    # Validate structure before writing
    validate_raw_state(raw)
    return raw


def write_raw_state(path: str = "signals/raw_state.json") -> None:
    raw = build_raw_state()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(raw, f, indent=2, sort_keys=True)
    from Analytics.policy_witnesses import write_daily_state as write_policy_witnesses
    from Analytics.inflation_real_rates import write_daily_state as write_inflation_real_rates
    from Analytics.volatility_analytics import write_daily_state as write_volatility
    from Analytics.liquidity_analytics import write_daily_state as write_liquidity_analytics
    from Signals.resolve_policy import resolve_policy as resolve_policy_spot
    from Signals.resolve_policy_curve import resolve_policy_curve
    from Signals.resolve_liquidity_curve import resolve_liquidity_curve
    from Signals.resolve_disagreements import resolve_disagreements
    from Signals.resolve_liquidity_curve import resolve_liquidity_curve
    from data.fetch_policy_curve import fetch_policy_curve
    write_policy_witnesses()
    write_inflation_real_rates()
    write_volatility()
    write_liquidity_analytics()
    resolve_policy_spot()
    resolve_policy_curve()
    resolve_liquidity_curve()
    resolve_disagreements()
    resolve_liquidity_curve()


if __name__ == "__main__":
    write_raw_state()
