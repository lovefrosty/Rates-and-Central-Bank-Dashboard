import json
from datetime import datetime

import pytest

import update
from Data import (
    fetch_credit_spreads,
    fetch_global_policy,
    fetch_inflation,
    fetch_inflation_witnesses,
    fetch_labor_market,
    fetch_liquidity,
    fetch_policy,
    fetch_policy_rates,
    fetch_policy_witnesses,
    fetch_vol,
    fetch_yields,
)
from update import build_raw_state, write_raw_state


@pytest.fixture(autouse=True)
def _patch_fred_fetchers(monkeypatch):
    def _ok(series_id):
        return 1.0, {
            "series_id": series_id,
            "start_of_year": 0.9,
            "last_week": 0.95,
            "current": 1.0,
            "as_of_start_of_year": "2024-01-02",
            "as_of_last_week": "2024-12-20",
            "as_of_current": "2024-12-27",
        }, "OK", "openbb:fred"

    for module in (
        fetch_policy,
        fetch_inflation,
        fetch_policy_witnesses,
        fetch_yields,
        fetch_liquidity,
        fetch_vol,
        fetch_credit_spreads,
        fetch_labor_market,
        fetch_global_policy,
        fetch_inflation_witnesses,
        fetch_policy_rates,
    ):
        if hasattr(module, "_fetch_fred_series"):
            monkeypatch.setattr(module, "_fetch_fred_series", _ok)


def test_write_raw_state(tmp_path):
    out = tmp_path / "raw_state.json"
    write_raw_state(str(out))
    assert out.exists()
    j = json.loads(out.read_text())
    # top-level keys
    for k in [
        "meta",
        "policy",
        "policy_futures",
        "policy_witnesses",
        "inflation_witnesses",
        "labor_market",
        "credit_spreads",
        "global_policy",
        "policy_rates",
        "fx",
        "policy_curve",
        "duration",
        "volatility",
        "liquidity",
    ]:
        assert k in j
    # data_health mapping exists
    assert "data_health" in j["meta"]
    duration_keys = {
        "y3m_nominal",
        "y6m_nominal",
        "y1y_nominal",
        "y2y_nominal",
        "y3y_nominal",
        "y5y_nominal",
        "y7y_nominal",
        "y10_nominal",
        "y10_real",
        "y20y_nominal",
        "y30y_nominal",
    }
    assert duration_keys.issubset(set(j["duration"].keys()))
    liquidity_keys = {"rrp", "rrp_level", "tga_level", "walcl"}
    assert liquidity_keys.issubset(set(j["liquidity"].keys()))
    credit_spreads_keys = {"ig_oas", "hy_oas"}
    assert credit_spreads_keys.issubset(set(j["credit_spreads"].keys()))
    global_policy_keys = {"ecb_deposit_rate", "usd_index", "dxy", "boj_stance"}
    assert global_policy_keys.issubset(set(j["global_policy"].keys()))
    policy_rates_keys = {"eur", "gbp", "jpy", "chf", "aud", "nzd", "cad", "cnh"}
    assert policy_rates_keys.issubset(set(j["policy_rates"].keys()))
    fx_keys = {
        "usdjpy",
        "eurusd",
        "gbpusd",
        "usdcad",
        "audusd",
        "nzdusd",
        "usdnok",
        "usdmxn",
        "usdzar",
        "usdchf",
        "usdcnh",
    }
    assert fx_keys.issubset(set(j["fx"].keys()))
    volatility_keys = {"vix", "move", "gvz", "ovx"}
    assert volatility_keys.issubset(set(j["volatility"].keys()))


def test_data_health_rules(monkeypatch):
    # Create a scenario where all policy subfields are FAILED -> policy FAILED
    def failed():
        return {"value": None, "status": "FAILED", "source": None, "fetched_at": "now", "error": "err", "meta": {}}

    monkeypatch.setattr("Data.fetch_policy.fetch_effr", failed)
    monkeypatch.setattr("Data.fetch_inflation.fetch_cpi_level", failed)
    raw = build_raw_state()
    assert raw["meta"]["data_health"]["policy"] == "FAILED"


def test_write_raw_state_handles_failures(tmp_path, monkeypatch):
    def _boom():
        raise RuntimeError("fail")

    monkeypatch.setattr(update.fetch_vol, "fetch_vix", _boom)
    path = tmp_path / "raw_state.json"
    write_raw_state(str(path))
    assert path.exists()

    data = json.loads(path.read_text())
    assert data["volatility"]["vix"]["status"] == "FAILED"


def test_write_raw_state_includes_generated_at(tmp_path):
    path = tmp_path / "raw_state.json"
    write_raw_state(str(path))

    data = json.loads(path.read_text())
    generated_at = data["meta"]["generated_at"]
    assert isinstance(generated_at, str)
    datetime.fromisoformat(generated_at.replace("Z", "+00:00"))


def test_output_is_pretty_and_stable(tmp_path):
    path = tmp_path / "raw_state.json"
    write_raw_state(str(path))

    content = path.read_text()
    data = json.loads(content)
    expected = json.dumps(data, indent=2, sort_keys=True)
    assert content.rstrip("\n") == expected
