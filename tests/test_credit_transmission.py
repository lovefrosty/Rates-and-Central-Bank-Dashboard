import json

import pytest

from Analytics.credit_transmission import build_credit_transmission, write_daily_state
from Data import fetch_credit_spreads
from update import build_raw_state


def test_fetcher_schema_invariance(monkeypatch):
    def _ok(series_id):
        return 1.5, {
            "series_id": series_id,
            "start_of_year": 1.2,
            "last_week": 1.4,
            "current": 1.5,
            "as_of_start_of_year": "2024-01-02",
            "as_of_last_week": "2024-12-20",
            "as_of_current": "2024-12-27",
        }, "OK", "openbb"

    monkeypatch.setattr(fetch_credit_spreads, "_fetch_fred_series", _ok)
    out = fetch_credit_spreads.fetch_ig_oas()
    for key in ["value", "status", "source", "fetched_at", "error", "meta"]:
        assert key in out


def test_raw_state_includes_credit_spreads(monkeypatch):
    def _failed():
        return {
            "value": None,
            "status": "FAILED",
            "source": None,
            "fetched_at": "now",
            "error": "fail",
            "meta": {},
        }

    monkeypatch.setattr("Data.fetch_credit_spreads.fetch_ig_oas", _failed)
    monkeypatch.setattr("Data.fetch_credit_spreads.fetch_hy_oas", _failed)
    raw = build_raw_state()
    assert "credit_spreads" in raw
    assert "ig_oas" in raw["credit_spreads"]
    assert "hy_oas" in raw["credit_spreads"]


def test_weekly_change_math():
    raw = {
        "credit_spreads": {
            "ig_oas": {"status": "OK", "meta": {"current": 1.5, "last_week": 1.4}},
            "hy_oas": {"status": "OK", "meta": {"current": 4.0, "last_week": 3.8}},
        },
        "duration": {"y10_nominal": {"status": "OK", "meta": {"current": 4.1, "last_week": 4.0}}},
        "global_policy": {
            "dxy": {
                "status": "OK",
                "meta": {
                    "current": 102.0,
                    "last_week": 101.0,
                    "1d_change_pct": 0.5,
                    "5d_change_pct": 1.0,
                    "1m_change_pct": 2.0,
                    "6m_change_pct": 3.0,
                    "year_open": 100.0,
                    "year_high": 105.0,
                    "year_low": 95.0,
                },
            }
        },
    }
    out = build_credit_transmission(raw)
    assert out["ig_oas_weekly_change_bps"] == pytest.approx(10.0)
    assert out["hy_oas_weekly_change_bps"] == pytest.approx(20.0)
    assert out["hy_minus_ig_bps"] == pytest.approx(250.0)
    assert out["treasury_10y_weekly_change_bps"] == pytest.approx(10.0)
    assert out["dxy"]["current"] == 102.0
    assert out["dxy"]["candle_1y"]["high"] == 105.0


def test_writer_preserves_other_blocks(tmp_path):
    raw = {
        "credit_spreads": {
            "ig_oas": {"status": "OK", "meta": {"current": 1.5, "last_week": 1.4}},
            "hy_oas": {"status": "OK", "meta": {"current": 4.0, "last_week": 3.8}},
        }
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "credit_transmission" in data
    assert data["policy"]["spot_stance"] == "Neutral"
