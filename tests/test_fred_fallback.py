import pandas as pd

from Data import fetch_policy


def _df(value: float = 5.0) -> pd.DataFrame:
    return pd.DataFrame([{"date": "2024-12-27", "value": value}])


def test_openbb_primary_ok(monkeypatch):
    def _openbb(series_id, start_date=None, end_date=None):
        return _df()

    def _fred(series_id, start_date=None, end_date=None, api_key=None):
        raise AssertionError("FRED fallback should not be called")

    monkeypatch.setattr(fetch_policy, "_try_openbb_fred", _openbb)
    monkeypatch.setattr(fetch_policy, "_try_fred_http", _fred)

    out = fetch_policy.fetch_effr()
    assert out["status"] == "OK"
    assert out["source"] == "openbb:fred"


def test_fred_fallback_used(monkeypatch):
    def _openbb(series_id, start_date=None, end_date=None):
        raise RuntimeError("OpenBB down")

    def _fred(series_id, start_date=None, end_date=None, api_key=None):
        return _df(4.9)

    monkeypatch.setattr(fetch_policy, "_try_openbb_fred", _openbb)
    monkeypatch.setattr(fetch_policy, "_try_fred_http", _fred)

    out = fetch_policy.fetch_effr()
    assert out["status"] == "OK"
    assert out["source"] == "fred_http"


def test_both_providers_fail(monkeypatch):
    def _openbb(series_id, start_date=None, end_date=None):
        raise RuntimeError("OpenBB down")

    def _fred(series_id, start_date=None, end_date=None, api_key=None):
        raise RuntimeError("FRED down")

    monkeypatch.setattr(fetch_policy, "_try_openbb_fred", _openbb)
    monkeypatch.setattr(fetch_policy, "_try_fred_http", _fred)

    out = fetch_policy.fetch_effr()
    assert out["status"] == "FAILED"
    assert out["value"] is None
