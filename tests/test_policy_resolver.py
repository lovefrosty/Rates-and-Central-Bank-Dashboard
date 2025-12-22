import json

from Signals.resolve_policy import resolve_policy


def _daily_state(real_10y=None, spread_bps=None, stress=None, sofr=None):
    data = {
        "policy_witnesses": {
            "sofr_current": sofr,
            "effr_sofr_spread_bps": spread_bps,
            "data_quality": {},
        },
        "inflation_real_rates": {
            "real_10y": real_10y,
            "breakeven_10y": None,
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
    return data


def test_resolver_writes_policy_block(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(real_10y=1.2, spread_bps=5, stress="Rates-led volatility")))
    resolve_policy(path)
    data = json.loads(path.read_text())
    assert "policy" in data
    assert data["policy"]["spot_stance"] == "Restrictive"


def test_restrictive_real_rates(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(real_10y=1.5, spread_bps=-10, stress="Equity-led volatility")))
    resolve_policy(path)
    data = json.loads(path.read_text())
    assert data["policy"]["spot_stance"] == "Restrictive"


def test_borderline_tilts_with_funding(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(real_10y=0.5, spread_bps=15, stress="Rates-led volatility")))
    resolve_policy(path)
    data = json.loads(path.read_text())
    assert data["policy"]["spot_stance"] == "Restrictive"


def test_missing_inputs_handled(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(real_10y=None, spread_bps=None, stress=None, sofr=None)))
    resolve_policy(path)
    data = json.loads(path.read_text())
    assert data["policy"]["spot_stance"] == "Neutral"
    assert "missing" in data["policy"]["explanation"].lower()
