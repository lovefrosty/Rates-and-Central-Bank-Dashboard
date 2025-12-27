import pandas as pd

from Data import fetch_policy_futures


def test_fetch_zq_contract_schema(monkeypatch):
    def _history(ticker, period="1y", start_date=None, end_date=None):
        dates = pd.date_range("2024-01-01", periods=6, freq="D")
        closes = [99.5, 99.4, 99.3, 99.2, 99.1, 99.0]
        return pd.DataFrame({"date": dates, "close": closes})

    monkeypatch.setattr("Data.yfinance_provider.fetch_price_history", _history)
    out = fetch_policy_futures.fetch_zq_contract("ZQZ25.CBT")
    for key in ["value", "status", "source", "fetched_at", "error", "meta"]:
        assert key in out
    assert out["status"] == "OK"
    assert out["source"] == "yfinance"
    assert out["meta"]["ticker"] == "ZQZ25.CBT"
    assert out["meta"]["current"] == 99.0
