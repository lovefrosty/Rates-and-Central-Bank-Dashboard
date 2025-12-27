import pandas as pd

from Data import fetch_vol


def test_move_fetch_uses_yfinance(monkeypatch):
    def _history(ticker, period="1y", start_date=None, end_date=None):
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-12-27")],
                "close": [100.0, 101.0],
            }
        )

    monkeypatch.setattr("Data.yfinance_provider.fetch_price_history", _history)
    out = fetch_vol.fetch_move()
    assert out["status"] == "OK"
    assert out["source"] == "yfinance"
