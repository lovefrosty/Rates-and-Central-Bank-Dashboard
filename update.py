"""Orchestrator: collect ingestion objects and write signals/raw_state.json

Mechanical behavior only: call fetchers, build raw_state.json, add timestamps and
data_health summary. Must not interpret values.
"""
from datetime import datetime, timezone
import json
from typing import Dict, List
import os
from pathlib import Path

from Data import (
    fetch_credit_spreads,
    fetch_fx,
    fetch_global_policy,
    fetch_inflation,
    fetch_inflation_witnesses,
    fetch_labor_market,
    fetch_liquidity,
    fetch_policy,
    fetch_policy_rates,
    fetch_policy_witnesses,
    fetch_policy_futures,
    fetch_policy_curve,
    fetch_vol,
    fetch_yields,
)
from Signals import state_paths
from Signals.json_utils import write_json
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


def _load_zq_contracts(path: Path | str = Path("config/zq_contracts.json")) -> List[str]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, str) and item.strip()]


def build_raw_state() -> Dict:
    policy = {
        "effr": _safe_call(fetch_policy.fetch_effr),
        "cpi_level": _safe_call(fetch_inflation.fetch_cpi_level),
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
        "gvz": _safe_call(fetch_vol.fetch_gvz),
        "ovx": _safe_call(fetch_vol.fetch_ovx),
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

    zq_contracts = _load_zq_contracts()
    policy_futures = {
        # Parallel addition: policy_futures
        "zq": {
            ticker: _safe_call(lambda t=ticker: fetch_policy_futures.fetch_zq_contract(t))
            for ticker in zq_contracts
        }
    }

    inflation_witnesses = {
        # Parallel addition: inflation_witnesses
        "cpi_headline": _safe_call(fetch_inflation_witnesses.fetch_cpi_headline),
        "cpi_core": _safe_call(fetch_inflation_witnesses.fetch_cpi_core),
    }

    labor_market = {
        # Parallel addition: labor_market
        "unrate": _safe_call(fetch_labor_market.fetch_unrate),
        "jolts_openings": _safe_call(fetch_labor_market.fetch_jolts_openings),
        "eci": _safe_call(fetch_labor_market.fetch_eci_index),
    }

    credit_spreads = {
        # Parallel addition: credit_spreads
        "ig_oas": _safe_call(fetch_credit_spreads.fetch_ig_oas),
        "hy_oas": _safe_call(fetch_credit_spreads.fetch_hy_oas),
    }

    global_policy = {
        # Parallel addition: global_policy
        "ecb_deposit_rate": _safe_call(fetch_global_policy.fetch_ecb_deposit_rate),
        "usd_index": _safe_call(fetch_global_policy.fetch_usd_index),
        "dxy": _safe_call(fetch_global_policy.fetch_dxy),
        "boj_stance": _safe_call(fetch_global_policy.fetch_boj_stance_manual),
    }

    policy_rates = {
        # Parallel addition: policy_rates
        "eur": _safe_call(fetch_policy_rates.fetch_policy_rate_eur),
        "gbp": _safe_call(fetch_policy_rates.fetch_policy_rate_gbp),
        "jpy": _safe_call(fetch_policy_rates.fetch_policy_rate_jpy),
        "chf": _safe_call(fetch_policy_rates.fetch_policy_rate_chf),
        "aud": _safe_call(fetch_policy_rates.fetch_policy_rate_aud),
        "nzd": _safe_call(fetch_policy_rates.fetch_policy_rate_nzd),
        "cad": _safe_call(fetch_policy_rates.fetch_policy_rate_cad),
        "cnh": _safe_call(fetch_policy_rates.fetch_policy_rate_cnh),
    }

    fx = {
        # Parallel addition: fx
        "usdjpy": _safe_call(fetch_fx.fetch_usdjpy),
        "eurusd": _safe_call(fetch_fx.fetch_eurusd),
        "gbpusd": _safe_call(fetch_fx.fetch_gbpusd),
        "usdcad": _safe_call(fetch_fx.fetch_usdcad),
        "audusd": _safe_call(fetch_fx.fetch_audusd),
        "nzdusd": _safe_call(fetch_fx.fetch_nzdusd),
        "usdnok": _safe_call(fetch_fx.fetch_usdnok),
        "usdmxn": _safe_call(fetch_fx.fetch_usdmxn),
        "usdzar": _safe_call(fetch_fx.fetch_usdzar),
        "usdchf": _safe_call(fetch_fx.fetch_usdchf),
        "usdcnh": _safe_call(fetch_fx.fetch_usdcnh),
    }

    policy_curve = {
        "curve": _safe_call(fetch_policy_curve.fetch_policy_curve),
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
        "policy_futures": policy_futures,
        "policy_witnesses": policy_witnesses,
        "inflation_witnesses": inflation_witnesses,
        "labor_market": labor_market,
        "credit_spreads": credit_spreads,
        "global_policy": global_policy,
        "policy_rates": policy_rates,
        "fx": fx,
        "policy_curve": policy_curve,  # ← APPEND HERE
        "duration": duration,
        "volatility": volatility,
        "liquidity": liquidity,
    }

    # Validate structure before writing
    validate_raw_state(raw)
    return raw


def write_raw_state(path: str | os.PathLike = state_paths.RAW_STATE_PATH) -> None:
    raw = build_raw_state()
    path = os.fspath(path)
    write_json(path, raw)
    from Analytics.policy_witnesses import write_daily_state as write_policy_witnesses
    from Analytics.inflation_real_rates import write_daily_state as write_inflation_real_rates
    from Analytics.volatility_analytics import write_daily_state as write_volatility
    from Analytics.liquidity_analytics import write_daily_state as write_liquidity_analytics
    from Analytics.yield_curve_analytics import write_daily_state as write_yield_curve
    from Analytics.inflation_level import write_daily_state as write_inflation_level
    from Analytics.inflation_witnesses import write_daily_state as write_inflation_witnesses
    from Analytics.labor_market import write_daily_state as write_labor_market
    from Analytics.credit_transmission import write_daily_state as write_credit_transmission
    from Analytics.global_policy_alignment import write_daily_state as write_global_policy_alignment
    from Analytics.fx_panel import write_daily_state as write_fx_panel
    from Analytics.system_health import write_daily_state as write_system_health
    from Analytics.policy_futures_curve import write_daily_state as write_policy_futures_curve
    from History.volatility_regime import write_daily_state as write_volatility_regime
    from History.fx_volatility import write_daily_state as write_fx_volatility
    from Signals.resolve_policy import resolve_policy as resolve_policy_spot
    from Signals.resolve_policy_curve import resolve_policy_curve
    from Signals.resolve_liquidity_curve import resolve_liquidity_curve
    from Signals.resolve_disagreements import resolve_disagreements
    from Signals.resolve_vol_credit_cross import resolve_vol_credit_cross
    write_policy_witnesses()
    write_inflation_real_rates()
    write_volatility()
    write_liquidity_analytics()
    write_yield_curve()
    write_inflation_level()
    write_inflation_witnesses()
    write_labor_market()
    write_credit_transmission()
    write_global_policy_alignment()
    write_fx_panel()
    write_system_health()
    write_policy_futures_curve()
    write_volatility_regime()
    write_fx_volatility()
    resolve_policy_spot()
    resolve_policy_curve()
    resolve_liquidity_curve()
    resolve_disagreements()
    resolve_vol_credit_cross()


if __name__ == "__main__":
    write_raw_state()
