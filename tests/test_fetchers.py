from datetime import datetime

import pytest

from Data import (
    fetch_credit_spreads,
    fetch_fx,
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
from Data.utils.snapshot_selection import select_snapshots


REQUIRED_KEYS = {"value", "status", "source", "fetched_at", "error", "meta"}


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


def assert_ingestion_shape(obj):
    assert isinstance(obj, dict)
    assert REQUIRED_KEYS.issubset(set(obj.keys()))
    assert obj["status"] in {"OK", "FAILED"}


def test_policy_fetchers_shape():
    assert_ingestion_shape(fetch_policy.fetch_effr())
    assert_ingestion_shape(fetch_inflation.fetch_cpi_level())
    assert_ingestion_shape(fetch_policy_witnesses.fetch_sofr())


def test_inflation_witness_fetchers_shape():
    assert_ingestion_shape(fetch_inflation_witnesses.fetch_cpi_headline())
    assert_ingestion_shape(fetch_inflation_witnesses.fetch_cpi_core())


def test_yields_fetchers_shape():
    assert_ingestion_shape(fetch_yields.fetch_y3m_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y6m_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y1y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y2y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y3y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y5y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y7y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y10y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y10_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y20y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y30y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y10_real())


def test_vol_fetchers_shape():
    assert_ingestion_shape(fetch_vol.fetch_vix())
    assert_ingestion_shape(fetch_vol.fetch_move())
    assert_ingestion_shape(fetch_vol.fetch_gvz())
    assert_ingestion_shape(fetch_vol.fetch_ovx())


def test_liquidity_fetchers_shape():
    assert_ingestion_shape(fetch_liquidity.fetch_rrp())
    assert_ingestion_shape(fetch_liquidity.fetch_rrp_level())
    assert_ingestion_shape(fetch_liquidity.fetch_tga_level())
    assert_ingestion_shape(fetch_liquidity.fetch_walcl())


def test_labor_fetchers_shape():
    assert_ingestion_shape(fetch_labor_market.fetch_unrate())
    assert_ingestion_shape(fetch_labor_market.fetch_jolts_openings())
    assert_ingestion_shape(fetch_labor_market.fetch_eci_index())


def test_credit_spreads_fetchers_shape():
    assert_ingestion_shape(fetch_credit_spreads.fetch_ig_oas())
    assert_ingestion_shape(fetch_credit_spreads.fetch_hy_oas())


def test_global_policy_fetchers_shape():
    assert_ingestion_shape(fetch_global_policy.fetch_ecb_deposit_rate())
    assert_ingestion_shape(fetch_global_policy.fetch_usd_index())
    assert_ingestion_shape(fetch_global_policy.fetch_dxy())
    assert_ingestion_shape(fetch_global_policy.fetch_boj_stance_manual())


def test_fx_fetchers_shape():
    assert_ingestion_shape(fetch_fx.fetch_usdjpy())
    assert_ingestion_shape(fetch_fx.fetch_eurusd())
    assert_ingestion_shape(fetch_fx.fetch_gbpusd())
    assert_ingestion_shape(fetch_fx.fetch_usdcad())


def test_policy_fetcher_failure_returns_failed(monkeypatch):
    def _boom(series_id):
        raise RuntimeError("no data")

    monkeypatch.setattr(fetch_policy, "_fetch_fred_series", _boom)
    res = fetch_policy.fetch_effr()
    assert_ingestion_shape(res)
    assert res["status"] == "FAILED"


def test_yield_fetcher_failure_returns_failed(monkeypatch):
    def _boom(series_id):
        raise RuntimeError("no data")

    monkeypatch.setattr(fetch_yields, "_fetch_fred_series", _boom)
    res = fetch_yields.fetch_y3m_nominal()
    assert_ingestion_shape(res)
    assert res["status"] == "FAILED"
    assert res["value"] is None


def test_policy_witness_failure_returns_failed(monkeypatch):
    def _boom(series_id):
        raise RuntimeError("no data")

    monkeypatch.setattr(fetch_policy_witnesses, "_fetch_fred_series", _boom)
    res = fetch_policy_witnesses.fetch_sofr()
    assert_ingestion_shape(res)
    assert res["status"] == "FAILED"
    assert res["value"] is None


def test_inflation_fetcher_failure_returns_failed(monkeypatch):
    def _boom(series_id):
        raise RuntimeError("no data")

    monkeypatch.setattr(fetch_inflation, "_fetch_fred_series", _boom)
    res = fetch_inflation.fetch_cpi_level()
    assert_ingestion_shape(res)
    assert res["status"] == "FAILED"
    assert res["value"] is None


def test_liquidity_fetcher_failure_returns_failed(monkeypatch):
    def _boom(series_id):
        raise RuntimeError("no data")

    monkeypatch.setattr(fetch_liquidity, "_fetch_fred_series", _boom)
    res = fetch_liquidity.fetch_rrp_level()
    assert_ingestion_shape(res)
    assert res["status"] == "FAILED"
    assert res["value"] is None


def test_liquidity_snapshots_use_calendar_year():
    points = [
        (datetime(2024, 12, 31), 1.0),
        (datetime(2025, 1, 3), 2.0),
        (datetime(2025, 1, 4), 3.0),
        (datetime(2025, 1, 5), 4.0),
        (datetime(2025, 1, 6), 5.0),
        (datetime(2025, 1, 7), 6.0),
    ]
    snapshots = select_snapshots(points, current_year=2025)
    assert snapshots["start_of_year"][0].year == 2024
    assert snapshots["start_of_year"][1] == 1.0


def test_liquidity_snapshots_use_trading_day_offset():
    points = [(datetime(2025, 1, day), float(day)) for day in range(1, 8)]
    snapshots = select_snapshots(points, current_year=2025)
    assert snapshots["last_week"] == points[0]
