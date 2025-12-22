import json

from Signals.resolve_policy_curve import resolve_policy_curve


def _daily_state(breakeven_change=None, proxy=None, sofr=None, stress=None):
    data = {
        "policy_witnesses": {
            "sofr_current": sofr,
            "effr_sofr_spread_bps": None,
            "data_quality": {},
        },
        "inflation_real_rates": {
            "breakeven_10y": None,
            "breakeven_10y_1m_change": breakeven_change,
            "driver_read": None,
            "data_quality": {},
        },
        "volatility": {
            "stress_origin_read": stress,
            "vix": None,
            "move": None,
            "data_quality": {},
        },
    }
    if proxy is not None:
        data["yield_expectations"] = {"policy_pricing_proxy": proxy}
    return data


def test_resolver_creates_block(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(breakeven_change=-0.1, sofr=5.0, stress="Rates-led volatility")))
    resolve_policy_curve(path)
    data = json.loads(path.read_text())
    assert "policy_curve" in data
    assert data["policy_curve"]["expected_direction"] == "Easing"


def test_direction_from_breakeven_change(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(breakeven_change=0.2, sofr=5.0)))
    resolve_policy_curve(path)
    data = json.loads(path.read_text())
    assert data["policy_curve"]["expected_direction"] == "Tightening"


def test_pricing_reinforces_not_override(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(breakeven_change=0.0, proxy=4.0, sofr=5.0)))
    resolve_policy_curve(path)
    data = json.loads(path.read_text())
    assert data["policy_curve"]["expected_direction"] == "Hold"


def test_spot_policy_unchanged(tmp_path):
    data = _daily_state(breakeven_change=0.1, sofr=5.0)
    data["policy"] = {"spot_stance": "Restrictive", "explanation": "x", "inputs_used": {}}
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(data))
    resolve_policy_curve(path)
    out = json.loads(path.read_text())
    assert out["policy"]["spot_stance"] == "Restrictive"


def test_missing_inputs(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state()))
    resolve_policy_curve(path)
    data = json.loads(path.read_text())
    assert data["policy_curve"]["expected_direction"] == "Hold"
    assert "missing" in data["policy_curve"]["explanation"].lower()
