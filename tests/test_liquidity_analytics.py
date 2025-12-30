import json

import pytest

from Analytics.liquidity_analytics import build_liquidity_analytics, write_daily_state


def _entry(current=None, last_week=None, last_month=None, last_6m=None, start_of_year=None, change_1m=None, status="OK"):
    meta = {
        "current": current,
        "last_week": last_week,
        "last_month": last_month,
        "last_6m": last_6m,
        "start_of_year": start_of_year,
        "1m_change": change_1m,
    }
    return {
        "value": current,
        "status": status,
        "meta": meta,
    }


def test_delta_math():
    raw_state = {
        "liquidity": {
            "rrp_level": _entry(current=2.0, last_week=1.5, last_month=1.8, last_6m=1.2, start_of_year=1.0),
            "tga_level": _entry(current=3.0, last_week=2.0, last_month=2.9, last_6m=2.4, start_of_year=2.5),
            "walcl": _entry(current=7.0, last_week=6.5, last_month=6.7, last_6m=6.1, start_of_year=6.0),
        }
    }
    out = build_liquidity_analytics(raw_state)
    assert out["rrp"]["change_1w"] == 0.5
    assert out["rrp"]["change_1m"] == pytest.approx(0.2)
    assert out["rrp"]["change_6m"] == pytest.approx(0.8)
    assert out["rrp"]["change_ytd"] == 1.0
    assert out["tga"]["change_1w"] == 1.0
    assert out["tga"]["change_1m"] == pytest.approx(0.1)
    assert out["tga"]["change_6m"] == pytest.approx(0.6)
    assert out["tga"]["change_ytd"] == 0.5
    assert out["walcl"]["change_1m"] == pytest.approx(0.3)
    assert out["walcl"]["change_6m"] == pytest.approx(0.9)
    assert out["data_quality"]["rrp"] == "OK"
    assert out["data_quality"]["tga"] == "OK"


def test_missing_snapshots():
    raw_state = {
        "liquidity": {
            "rrp_level": _entry(current=2.0, last_week=None, last_month=None, last_6m=None, start_of_year=1.0),
        }
    }
    out = build_liquidity_analytics(raw_state)
    assert out["rrp"]["change_1w"] is None
    assert out["rrp"]["change_ytd"] == 1.0
    assert out["data_quality"]["rrp"] == "PARTIAL"


def test_failed_propagation():
    raw_state = {
        "liquidity": {
            "rrp_level": _entry(status="FAILED"),
            "tga_level": _entry(status="FAILED"),
            "walcl": _entry(status="FAILED"),
        }
    }
    out = build_liquidity_analytics(raw_state)
    assert out["rrp"]["level"] is None
    assert out["tga"]["level"] is None
    assert out["data_quality"]["rrp"] == "FAILED"
    assert out["data_quality"]["tga"] == "FAILED"
    assert out["data_quality"]["walcl"] == "FAILED"


def test_writer_preserves_other_blocks(tmp_path):
    raw_state = {
        "liquidity": {
            "rrp_level": _entry(current=2.0, last_week=1.5, last_month=1.8, last_6m=1.2, start_of_year=1.0),
            "tga_level": _entry(current=3.0, last_week=2.0, last_month=2.9, last_6m=2.4, start_of_year=2.5),
        }
    }
    daily_state = {"policy": {"spot_stance": "Restrictive"}}
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw_state))
    daily_path.write_text(json.dumps(daily_state))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    out = json.loads(daily_path.read_text())
    assert out["policy"]["spot_stance"] == "Restrictive"
    assert "liquidity_analytics" in out
