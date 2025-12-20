import pytest

from Data import fetch_policy, fetch_yields, fetch_vol, fetch_liquidity


REQUIRED_KEYS = {"value", "status", "source", "fetched_at", "error", "meta"}


def assert_ingestion_shape(obj):
    assert isinstance(obj, dict)
    assert REQUIRED_KEYS.issubset(set(obj.keys()))
    assert obj["status"] in {"OK", "FALLBACK", "FAILED"}


def test_policy_fetchers_shape():
    assert_ingestion_shape(fetch_policy.fetch_effr())
    assert_ingestion_shape(fetch_policy.fetch_cpi_yoy())


def test_yields_fetchers_shape():
    assert_ingestion_shape(fetch_yields.fetch_y3m_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y2y_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y10_nominal())
    assert_ingestion_shape(fetch_yields.fetch_y10_real())


def test_vol_fetchers_shape():
    assert_ingestion_shape(fetch_vol.fetch_vix())
    assert_ingestion_shape(fetch_vol.fetch_move())


def test_liquidity_fetchers_shape():
    assert_ingestion_shape(fetch_liquidity.fetch_rrp())
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
    def _boom(field, provider):
        raise RuntimeError("no data")

    monkeypatch.setattr(fetch_yields, "_fetch_latest_treasury_rate", _boom)
    res = fetch_yields.fetch_y3m_nominal()
    assert_ingestion_shape(res)
    assert res["status"] == "FAILED"
    assert res["value"] is None
