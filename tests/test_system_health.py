import json

from Analytics.system_health import build_system_health, write_daily_state


def _entry(status="OK"):
    return {"status": status, "value": 1.0, "source": "test", "fetched_at": "now", "error": None, "meta": {}}


def test_build_system_health_counts():
    raw_state = {
        "meta": {"generated_at": "2024-01-01T00:00:00Z"},
        "duration": {"y10_nominal": _entry(), "y10_real": _entry("FAILED")},
        "policy_futures": {"zq": {"ZQZ25.CBT": _entry()}},
        "volatility": {"vix": _entry()},
        "liquidity": {"rrp_level": _entry("FAILED")},
        "labor_market": {"unrate": _entry()},
        "credit_spreads": {"ig_oas": _entry()},
        "global_policy": {"ecb_deposit_rate": _entry()},
    }
    out = build_system_health(raw_state)
    assert out["blocks"]["Rates"]["status"] == "PARTIAL"
    assert out["blocks"]["Liquidity"]["status"] == "FAILED"


def test_writer_preserves_other_blocks(tmp_path):
    raw_state = {"meta": {"generated_at": "2024-01-01T00:00:00Z"}}
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw_state))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "system_health" in data
    assert data["policy"]["spot_stance"] == "Neutral"
