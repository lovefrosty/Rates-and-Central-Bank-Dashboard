import json

from Analytics.volatility_analytics import build_volatility_block, write_daily_state


def _entry(value, status="OK", roc=None):
    meta = {}
    if roc is not None:
        meta["5d_roc"] = roc
    return {
        "value": value,
        "status": status,
        "meta": meta,
    }


def test_writer_creates_block(tmp_path):
    raw = {"volatility": {"vix": _entry(18.0), "move": _entry(95.0)}}
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "volatility" in data


def test_values_propagate():
    raw = {"volatility": {"vix": _entry(18.0), "move": _entry(95.0)}}
    out = build_volatility_block(raw)
    assert out["vix"] == 18.0
    assert out["move"] == 95.0


def test_stress_origin_read_mapping():
    raw = {"volatility": {"vix": _entry(18.0, roc=0.0), "move": _entry(95.0, roc=0.1)}}
    out = build_volatility_block(raw)
    assert out["stress_origin_read"] == "Rates-led volatility"

    raw = {"volatility": {"vix": _entry(18.0, roc=0.1), "move": _entry(95.0, roc=0.2)}}
    out = build_volatility_block(raw)
    assert out["stress_origin_read"] == "Cross-asset stress"

    raw = {"volatility": {"vix": _entry(18.0, roc=0.1), "move": _entry(95.0, roc=0.0)}}
    out = build_volatility_block(raw)
    assert out["stress_origin_read"] == "Equity-led volatility"


def test_missing_inputs_handled():
    raw = {"volatility": {"vix": _entry(None, status="FAILED")}}
    out = build_volatility_block(raw)
    assert out["vix"] is None
    assert out["move"] is None
    assert out["data_quality"]["vix"] == "FAILED"
    assert out["data_quality"]["move"] is None
