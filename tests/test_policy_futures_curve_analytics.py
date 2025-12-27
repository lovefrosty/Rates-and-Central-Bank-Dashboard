import json

import pytest

from Analytics.policy_futures_curve import build_policy_futures_curve, write_daily_state


def _entry(current, start_of_year=None, last_week=None, status="OK"):
    return {
        "value": current,
        "status": status,
        "meta": {
            "current": current,
            "start_of_year": start_of_year,
            "last_week": last_week,
        },
    }


def test_implied_rate_math():
    raw_state = {
        "policy_futures": {
            "zq": {
                "ZQZ25.CBT": _entry(99.0, start_of_year=99.5, last_week=99.2),
                "ZQF26.CBT": _entry(98.5, start_of_year=99.0, last_week=98.7),
            }
        }
    }
    out = build_policy_futures_curve(raw_state)
    contract = out["contracts"][0]
    assert contract["current_rate_pct"] == 1.0
    assert contract["weekly_change_bps"] == pytest.approx((1.0 - 0.8) * 100)


def test_writer_merges_blocks(tmp_path):
    raw_state = {
        "policy_futures": {
            "zq": {"ZQZ25.CBT": _entry(99.0, start_of_year=99.5, last_week=99.2)}
        }
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw_state))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))

    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "policy_futures_curve" in data
    assert data["policy"]["spot_stance"] == "Neutral"
