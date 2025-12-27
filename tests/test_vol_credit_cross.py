import json

from Signals.resolve_vol_credit_cross import resolve_vol_credit_cross


def _write_and_resolve(tmp_path, daily_state):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(daily_state))
    resolve_vol_credit_cross(daily_state_path=path)
    return json.loads(path.read_text())


def test_market_led_label(tmp_path):
    daily = {
        "volatility": {"vix_5d_roc": 1.0},
        "credit_transmission": {"hy_oas_weekly_change_bps": 0.0},
    }
    out = _write_and_resolve(tmp_path, daily)
    assert out["vol_credit_cross"]["label"] == "MARKET_LED"


def test_credit_led_label(tmp_path):
    daily = {
        "volatility": {"vix_5d_roc": 0.0},
        "credit_transmission": {"hy_oas_weekly_change_bps": 5.0},
    }
    out = _write_and_resolve(tmp_path, daily)
    assert out["vol_credit_cross"]["label"] == "CREDIT_LED"


def test_no_stress_label(tmp_path):
    daily = {
        "volatility": {"vix_5d_roc": -0.5},
        "credit_transmission": {"hy_oas_weekly_change_bps": -1.0},
    }
    out = _write_and_resolve(tmp_path, daily)
    assert out["vol_credit_cross"]["label"] == "NO_STRESS"


def test_unavailable_on_missing_inputs(tmp_path):
    daily = {"volatility": {}, "credit_transmission": {"hy_oas_weekly_change_bps": 2.0}}
    out = _write_and_resolve(tmp_path, daily)
    assert out["vol_credit_cross"]["label"] == "UNAVAILABLE"


def test_preserves_other_blocks(tmp_path):
    daily = {
        "volatility": {"vix_5d_roc": 1.0},
        "credit_transmission": {"hy_oas_weekly_change_bps": 0.0},
        "policy": {"spot_stance": "Neutral"},
    }
    out = _write_and_resolve(tmp_path, daily)
    assert out["policy"]["spot_stance"] == "Neutral"
