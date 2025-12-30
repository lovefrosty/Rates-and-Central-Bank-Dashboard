import json

from Analytics.volatility_analytics import build_volatility_block, write_daily_state


def _entry(
    value,
    status="OK",
    roc=None,
    change_1d=None,
    change_5d=None,
    change_1m=None,
    change_6m=None,
    last_week=None,
    start_of_year=None,
):
    meta = {}
    if roc is not None:
        meta["5d_roc"] = roc
    if change_1d is not None:
        meta["1d_change_pct"] = change_1d
    if change_5d is not None:
        meta["5d_change_pct"] = change_5d
    if change_1m is not None:
        meta["1m_change_pct"] = change_1m
    if change_6m is not None:
        meta["6m_change_pct"] = change_6m
    if last_week is not None:
        meta["last_week"] = last_week
    if start_of_year is not None:
        meta["start_of_year"] = start_of_year
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
    raw = {
        "volatility": {
            "vix": _entry(
                18.0,
                change_1d=1.0,
                change_5d=2.0,
                change_1m=3.0,
                change_6m=4.0,
                last_week=17.5,
                start_of_year=20.0,
            ),
            "move": _entry(95.0, change_1d=-1.0, last_week=94.0, start_of_year=100.0),
            "gvz": _entry(14.0),
            "ovx": _entry(28.0),
        }
    }
    out = build_volatility_block(raw)
    assert out["vix"] == 18.0
    assert out["move"] == 95.0
    assert out["vix_move_ratio"] == 18.0 / 95.0
    assert out["move_vix_ratio"] == 95.0 / 18.0
    assert out["gvz_vix_ratio"] == 14.0 / 18.0
    assert out["ovx_vix_ratio"] == 28.0 / 18.0
    assert out["changes_pct"]["vix"]["1d_pct"] == 1.0
    assert out["changes_pct"]["vix"]["5d_pct"] == 2.0
    assert out["anchors"]["vix"]["last_week"] == 17.5
    assert out["anchors"]["vix"]["start_of_year"] == 20.0


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
    assert out["data_quality"]["gvz"] is None
