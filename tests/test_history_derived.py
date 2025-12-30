import json

from History.fx_volatility import build_fx_volatility, write_daily_state as write_fx_volatility
from History.volatility_regime import build_volatility_regime, write_daily_state as write_vol_regime


def test_volatility_regime_classification():
    history_state = {
        "transforms": {
            "vix": {"zscore_3y": {"dates": ["2024-01-01"], "values": [1.7]}},
            "move": {"zscore_3y": {"dates": ["2024-01-01"], "values": [0.2]}},
        }
    }
    out = build_volatility_regime(history_state)
    assert out["equity"] == "Stress"
    assert out["rates"] == "Normal"
    assert out["joint"] == "Equity-led stress"
    assert "computed_at" in out


def test_fx_volatility_builds_entries():
    history_state = {
        "transforms": {
            "eurusd": {
                "realized_vol_20d_pct": {"dates": ["2024-01-01"], "values": [7.5]},
                "realized_vol_20d_zscore_3y": {"dates": ["2024-01-01"], "values": [0.3]},
            }
        }
    }
    out = build_fx_volatility(history_state)
    entries = {entry["pair"]: entry for entry in out["entries"]}
    assert entries["EURUSD"]["realized_vol_20d_pct"] == 7.5
    assert entries["EURUSD"]["zscore_3y"] == 0.3
    assert entries["EURUSD"]["regime"] == "Normal"


def test_history_writers_preserve_other_blocks(tmp_path):
    history_state = {
        "transforms": {
            "vix": {"zscore_3y": {"dates": ["2024-01-01"], "values": [0.2]}},
            "move": {"zscore_3y": {"dates": ["2024-01-01"], "values": [0.1]}},
        }
    }
    history_path = tmp_path / "history_state.json"
    daily_path = tmp_path / "daily_state.json"
    history_path.write_text(json.dumps(history_state))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))

    write_vol_regime(history_state_path=history_path, daily_state_path=daily_path)
    write_fx_volatility(history_state_path=history_path, daily_state_path=daily_path)

    data = json.loads(daily_path.read_text())
    assert "volatility_regime" in data
    assert "fx_volatility" in data
    assert data["policy"]["spot_stance"] == "Neutral"
