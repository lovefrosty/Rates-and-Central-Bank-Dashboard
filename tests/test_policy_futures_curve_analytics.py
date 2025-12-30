import json

from Analytics.policy_futures_curve import build_policy_futures_curve, write_daily_state


def _entry(current, start_of_year=None, last_week=None, last_month=None, last_6m=None, status="OK"):
    return {
        "value": current,
        "status": status,
        "meta": {
            "current": current,
            "start_of_year": start_of_year,
            "last_week": last_week,
            "last_month": last_month,
            "last_6m": last_6m,
        },
    }


def test_anchor_values():
    raw_state = {
        "policy_futures": {
            "zq": {
                "ZQZ25.CBT": _entry(99.0, start_of_year=99.5, last_week=99.2, last_month=99.1, last_6m=98.7),
                "ZQF26.CBT": _entry(98.5, start_of_year=99.0, last_week=98.7, last_month=98.6, last_6m=98.2),
            }
        }
    }
    out = build_policy_futures_curve(raw_state)
    contract = out["contracts"][0]
    assert contract["current_price"] == 99.0
    assert contract["last_week_price"] == 99.2
    assert contract["last_month_price"] == 99.1
    assert contract["last_6m_price"] == 98.7
    assert out["curve_lines"]["current"][0] == 99.0


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
