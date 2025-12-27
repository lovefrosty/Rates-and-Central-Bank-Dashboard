import json
from pathlib import Path

import pytest

from Analytics.policy_witnesses import build_policy_witnesses
from Data import (
    fetch_credit_spreads,
    fetch_global_policy,
    fetch_inflation,
    fetch_inflation_witnesses,
    fetch_labor_market,
    fetch_liquidity,
    fetch_policy,
    fetch_policy_witnesses,
    fetch_vol,
    fetch_yields,
)
from update import write_raw_state


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
    ):
        if hasattr(module, "_fetch_fred_series"):
            monkeypatch.setattr(module, "_fetch_fred_series", _ok)


def test_fetch_sofr_schema(monkeypatch):
    def _ok(series_id):
        return 5.0, {
            "series_id": series_id,
            "start_of_year": 4.8,
            "last_week": 4.9,
            "current": 5.0,
            "as_of_start_of_year": "2024-01-02",
            "as_of_last_week": "2024-12-20",
            "as_of_current": "2024-12-27",
        }, "OK", "openbb"

    monkeypatch.setattr(fetch_policy_witnesses, "_fetch_fred_series", _ok)
    res = fetch_policy_witnesses.fetch_sofr()
    assert res["status"] == "OK"
    for key in ["value", "status", "source", "fetched_at", "error", "meta"]:
        assert key in res


def test_raw_state_includes_policy_witnesses(tmp_path, monkeypatch):
    def _failed():
        return {
            "value": None,
            "status": "FAILED",
            "source": None,
            "fetched_at": "now",
            "error": "fail",
            "meta": {},
        }

    monkeypatch.setattr("Data.fetch_policy_witnesses.fetch_sofr", _failed)
    out = tmp_path / "raw_state.json"
    write_raw_state(str(out))
    data = out.read_text()
    assert "\"policy_witnesses\"" in data
    assert "\"sofr\"" in data


def test_effr_sofr_spread_bps():
    raw_state = {
        "policy": {"effr": {"value": 5.0, "meta": {"current": 5.0}, "status": "OK"}},
        "policy_witnesses": {"sofr": {"value": 4.5, "meta": {"current": 4.5}, "status": "OK"}},
    }
    out = build_policy_witnesses(raw_state)
    assert out["effr_sofr_spread_bps"] == (5.0 - 4.5) * 100


def test_daily_state_written_from_update():
    daily_path = Path("signals/daily_state.json")
    if daily_path.exists():
        daily_path.unlink()
    write_raw_state("signals/raw_state.json")
    assert daily_path.exists()
    data = json.loads(daily_path.read_text())
    assert "policy_witnesses" in data
