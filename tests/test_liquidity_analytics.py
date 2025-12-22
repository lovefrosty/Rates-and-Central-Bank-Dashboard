import json

from Analytics.liquidity_analytics import build_liquidity_analytics, write_daily_state


def _entry(current=None, last_week=None, start_of_year=None, status="OK"):
    meta = {
        "current": current,
        "last_week": last_week,
        "start_of_year": start_of_year,
    }
    return {
        "value": current,
        "status": status,
        "meta": meta,
    }


def test_delta_math():
    raw_state = {
        "liquidity": {
            "rrp_level": _entry(current=2.0, last_week=1.5, start_of_year=1.0),
            "tga_level": _entry(current=3.0, last_week=2.0, start_of_year=2.5),
        }
    }
    out = build_liquidity_analytics(raw_state)
    assert out["rrp"]["change_1w"] == 0.5
    assert out["rrp"]["change_ytd"] == 1.0
    assert out["tga"]["change_1w"] == 1.0
    assert out["tga"]["change_ytd"] == 0.5
    assert out["data_quality"]["rrp"] == "OK"
    assert out["data_quality"]["tga"] == "OK"


def test_missing_snapshots():
    raw_state = {
        "liquidity": {
            "rrp_level": _entry(current=2.0, last_week=None, start_of_year=1.0),
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
        }
    }
    out = build_liquidity_analytics(raw_state)
    assert out["rrp"]["level"] is None
    assert out["tga"]["level"] is None
    assert out["data_quality"]["rrp"] == "FAILED"
    assert out["data_quality"]["tga"] == "FAILED"


def test_writer_preserves_other_blocks(tmp_path):
    raw_state = {
        "liquidity": {
            "rrp_level": _entry(current=2.0, last_week=1.5, start_of_year=1.0),
            "tga_level": _entry(current=3.0, last_week=2.0, start_of_year=2.5),
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
