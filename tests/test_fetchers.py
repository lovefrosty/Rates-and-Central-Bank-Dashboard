from datetime import datetime

import pytest

from Data import fetch_policy, fetch_policy_witnesses, fetch_yields, fetch_vol, fetch_liquidity
from Data.utils.snapshot_selection import select_snapshots


REQUIRED_KEYS = {"value", "status", "source", "fetched_at", "error", "meta"}


def assert_ingestion_shape(obj):
    assert isinstance(obj, dict)
    assert REQUIRED_KEYS.issubset(set(obj.keys()))
    assert obj["status"] in {"OK", "FALLBACK", "FAILED"}


def test_policy_fetchers_shape():
    assert_ingestion_shape(fetch_policy.fetch_effr())
    assert_ingestion_shape(fetch_policy.fetch_cpi_yoy())
    assert_ingestion_shape(fetch_policy_witnesses.fetch_sofr())


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


def test_liquidity_fetchers_shape():
    assert_ingestion_shape(fetch_liquidity.fetch_rrp())
    assert_ingestion_shape(fetch_liquidity.fetch_rrp_level())
    assert_ingestion_shape(fetch_liquidity.fetch_tga_level())
    assert_ingestion_shape(fetch_liquidity.fetch_walcl())


def test_primary_failure_uses_fallback(monkeypatch):
    # Force primary to throw for effr, fallback should be used and status==FALLBACK
    def _boom():
        raise RuntimeError("primary down")

    monkeypatch.setattr(fetch_policy, "_try_primary", _boom)
    res = fetch_policy.fetch_effr()
    assert_ingestion_shape(res)
    assert res["status"] in {"FALLBACK", "FAILED"}


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
    assert snapshots["start_of_year"][0].year == 2025
    assert snapshots["start_of_year"][1] == 2.0


def test_liquidity_snapshots_use_trading_day_offset():
    points = [(datetime(2025, 1, day), float(day)) for day in range(1, 8)]
    snapshots = select_snapshots(points, current_year=2025)
    assert snapshots["last_week"] == points[-6]
