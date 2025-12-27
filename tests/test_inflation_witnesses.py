import json

import pytest

from Analytics.inflation_witnesses import build_inflation_witnesses, write_daily_state
from Data import fetch_inflation_witnesses
from update import build_raw_state


def test_fetcher_schema_invariance(monkeypatch):
    def _ok(series_id):
        return 100.0, {
            "series_id": series_id,
            "start_of_year": 95.0,
            "last_week": 99.0,
            "current": 100.0,
            "as_of_start_of_year": "2024-01-02",
            "as_of_last_week": "2024-12-20",
            "as_of_current": "2024-12-27",
            "year_ago": 90.0,
            "as_of_year_ago": "2023-12-27",
        }, "OK", "openbb"

    monkeypatch.setattr(fetch_inflation_witnesses, "_fetch_fred_series", _ok)
    out = fetch_inflation_witnesses.fetch_cpi_headline()
    for key in ["value", "status", "source", "fetched_at", "error", "meta"]:
        assert key in out


def test_raw_state_includes_inflation_witnesses(monkeypatch):
    def _failed():
        return {
            "value": None,
            "status": "FAILED",
            "source": None,
            "fetched_at": "now",
            "error": "fail",
            "meta": {},
        }

    monkeypatch.setattr("Data.fetch_inflation_witnesses.fetch_cpi_headline", _failed)
    monkeypatch.setattr("Data.fetch_inflation_witnesses.fetch_cpi_core", _failed)
    raw = build_raw_state()
    assert "inflation_witnesses" in raw
    assert "cpi_headline" in raw["inflation_witnesses"]
    assert "cpi_core" in raw["inflation_witnesses"]


def test_yoy_calculation():
    raw = {
        "inflation_witnesses": {
            "cpi_headline": {"value": 105.0, "status": "OK", "meta": {"current": 105.0, "year_ago": 100.0}},
            "cpi_core": {"value": 110.0, "status": "OK", "meta": {"current": 110.0, "year_ago": 100.0}},
        }
    }
    out = build_inflation_witnesses(raw)
    assert out["cpi_headline_yoy_pct"] == pytest.approx(5.0)
    assert out["cpi_core_yoy_pct"] == pytest.approx(10.0)


def test_writer_preserves_other_blocks(tmp_path):
    raw = {
        "inflation_witnesses": {
            "cpi_headline": {"value": 105.0, "status": "OK", "meta": {"current": 105.0, "year_ago": 100.0}},
            "cpi_core": {"value": 110.0, "status": "OK", "meta": {"current": 110.0, "year_ago": 100.0}},
        }
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "inflation_witnesses" in data
    assert data["policy"]["spot_stance"] == "Neutral"
