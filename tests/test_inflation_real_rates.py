import json

import pytest

from Analytics.inflation_real_rates import build_inflation_real_rates, write_daily_state


def _entry(value, status="OK", change_1m=None):
    meta = {}
    if change_1m is not None:
        meta["1m_change"] = change_1m
    return {
        "value": value,
        "status": status,
        "meta": meta,
    }


def test_writer_creates_block(tmp_path):
    raw = {
        "duration": {
            "y10_nominal": _entry(4.0),
            "y10_real": _entry(2.0),
        }
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "inflation_real_rates" in data


def test_breakeven_computed_when_missing():
    raw = {
        "duration": {
            "y10_nominal": _entry(4.0, status="OK"),
            "y10_real": _entry(1.5, status="OK"),
        }
    }
    out = build_inflation_real_rates(raw)
    assert out["breakeven_10y"] == 2.5
    assert out["data_quality"]["breakeven"] == "OK"


def test_missing_inputs_propagate_none():
    raw = {
        "duration": {
            "y10_nominal": _entry(None, status="FAILED"),
            "y10_real": _entry(1.0, status="OK"),
        }
    }
    out = build_inflation_real_rates(raw)
    assert out["nominal_10y"] is None
    assert out["breakeven_10y"] is None
    assert out["data_quality"]["nominal"] == "FAILED"


def test_driver_read_mapping():
    raw = {
        "duration": {
            "y10_nominal": _entry(4.0, change_1m=0.1),
            "y10_real": _entry(2.0, change_1m=0.05),
        }
    }
    out = build_inflation_real_rates(raw)
    assert out["driver_read"] == "Inflation repricing"


def test_cpi_yoy_derived():
    raw = {
        "policy": {"cpi_level": {"value": 110.0, "status": "OK", "meta": {"current": 110.0, "year_ago": 100.0}}},
        "duration": {
            "y10_nominal": _entry(4.0, status="OK"),
            "y10_real": _entry(2.0, status="OK"),
        },
    }
    out = build_inflation_real_rates(raw)
    assert out["cpi_yoy_pct"] == pytest.approx(10.0)
    assert out["real_rate_spread"] == 2.0
