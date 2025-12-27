import json

from Analytics.inflation_witnesses import write_daily_state as write_inflation_witnesses
from Analytics.policy_witnesses import write_daily_state as write_policy_witnesses
from Signals.resolve_policy import resolve_policy


def test_writers_merge_blocks(tmp_path):
    raw_state = {
        "policy": {"effr": {"value": 5.0, "status": "OK", "meta": {"current": 5.0}}},
        "policy_witnesses": {"sofr": {"value": 5.0, "status": "OK", "meta": {"current": 5.0}}},
        "inflation_witnesses": {
            "cpi_headline": {"value": 100.0, "status": "OK", "meta": {"current": 100.0, "year_ago": 95.0}},
            "cpi_core": {"value": 101.0, "status": "OK", "meta": {"current": 101.0, "year_ago": 96.0}},
        },
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw_state))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))

    write_policy_witnesses(raw_state_path=raw_path, daily_state_path=daily_path)
    write_inflation_witnesses(raw_state_path=raw_path, daily_state_path=daily_path)

    data = json.loads(daily_path.read_text())
    assert "policy_witnesses" in data
    assert "inflation_witnesses" in data
    assert data["policy"]["spot_stance"] == "Neutral"


def test_resolver_merges_block(tmp_path):
    daily_state = {
        "policy_curve": {"expected_direction": "Hold"},
        "policy_witnesses": {},
        "inflation_real_rates": {},
        "volatility": {},
    }
    daily_path = tmp_path / "daily_state.json"
    daily_path.write_text(json.dumps(daily_state))

    resolve_policy(daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "policy" in data
    assert data["policy_curve"]["expected_direction"] == "Hold"
