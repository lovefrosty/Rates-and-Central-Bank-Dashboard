import json

import pytest

from Analytics.labor_market import build_labor_market, write_daily_state
from Data import fetch_labor_market
from update import build_raw_state


def test_fetcher_schema_invariance(monkeypatch):
    def _ok(series_id):
        return 100.0, {
            "series_id": series_id,
            "start_of_year": 95.0,
            "last_week": None,
            "current": 100.0,
            "as_of_start_of_year": "2024-01-02",
            "as_of_last_week": None,
            "as_of_current": "2024-12-27",
            "year_ago": 90.0,
            "as_of_year_ago": "2023-12-27",
        }, "OK", "openbb"

    monkeypatch.setattr(fetch_labor_market, "_fetch_fred_series", _ok)
    out = fetch_labor_market.fetch_unrate()
    for key in ["value", "status", "source", "fetched_at", "error", "meta"]:
        assert key in out


def test_raw_state_includes_labor_market(monkeypatch):
    def _failed():
        return {
            "value": None,
            "status": "FAILED",
            "source": None,
            "fetched_at": "now",
            "error": "fail",
            "meta": {},
        }

    monkeypatch.setattr("Data.fetch_labor_market.fetch_unrate", _failed)
    monkeypatch.setattr("Data.fetch_labor_market.fetch_jolts_openings", _failed)
    monkeypatch.setattr("Data.fetch_labor_market.fetch_eci_index", _failed)
    raw = build_raw_state()
    assert "labor_market" in raw
    assert "unrate" in raw["labor_market"]
    assert "jolts_openings" in raw["labor_market"]
    assert "eci" in raw["labor_market"]


def test_yoy_calculations():
    raw = {
        "labor_market": {
            "unrate": {"value": 4.0, "status": "OK", "meta": {"current": 4.0, "year_ago": 3.5}},
            "jolts_openings": {"value": 8.0, "status": "OK", "meta": {"current": 8.0, "year_ago": 10.0}},
            "eci": {"value": 110.0, "status": "OK", "meta": {"current": 110.0, "year_ago": 100.0}},
        }
    }
    out = build_labor_market(raw)
    assert out["unrate_yoy_change"] == pytest.approx(0.5)
    assert out["jolts_yoy_change"] == pytest.approx(-2.0)
    assert out["eci_yoy_pct"] == pytest.approx(10.0)


def test_writer_preserves_other_blocks(tmp_path):
    raw = {
        "labor_market": {
            "unrate": {"value": 4.0, "status": "OK", "meta": {"current": 4.0, "year_ago": 3.5}},
            "jolts_openings": {"value": 8.0, "status": "OK", "meta": {"current": 8.0, "year_ago": 10.0}},
            "eci": {"value": 110.0, "status": "OK", "meta": {"current": 110.0, "year_ago": 100.0}},
        }
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "labor_market" in data
    assert data["policy"]["spot_stance"] == "Neutral"
