import json
from datetime import datetime
from math import inf, nan

from History import history_state


def test_history_state_sanitizes_nan_inf(tmp_path, monkeypatch):
    def _fred(series_id, years=5):
        records = [
            (datetime(2024, 1, 1), nan),
            (datetime(2024, 1, 2), 100.0),
        ]
        return records, "fred_http", "OK"

    def _yf(ticker, years=5):
        records = [
            (datetime(2024, 1, 1), inf),
            (datetime(2024, 1, 2), 99.0),
        ]
        return records, "yfinance", "OK"

    monkeypatch.setattr(history_state, "_fetch_fred_history", _fred)
    monkeypatch.setattr(history_state, "_fetch_yfinance_history", _yf)

    out_path = tmp_path / "history_state.json"
    history_state.write_history_state(path=out_path)
    content = out_path.read_text(encoding="utf-8")
    assert "NaN" not in content
    assert "Infinity" not in content
    data = json.loads(content)
    assert "series" in data
    assert "transforms" in data
    assert "meta" in data
    series_entry = data["series"].get("vix", {})
    assert "dates" in series_entry
    assert "values" in series_entry
