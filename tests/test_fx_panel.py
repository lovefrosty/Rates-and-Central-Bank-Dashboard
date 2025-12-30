import json

from Analytics.fx_panel import build_fx_panel, write_daily_state


def _entry(current, last_week=None, start_of_year=None, change_1m_pct=None, status="OK", series_id="X"):
    meta = {
        "current": current,
        "last_week": last_week,
        "start_of_year": start_of_year,
        "1m_change_pct": change_1m_pct,
        "series_id": series_id,
    }
    return {
        "value": current,
        "status": status,
        "meta": meta,
        "source": "yfinance",
    }


def test_fallback_to_usd_index():
    raw_state = {
        "global_policy": {
            "dxy": _entry(None, status="FAILED", series_id="DX-Y.NYB"),
            "usd_index": _entry(102.0, last_week=101.0, start_of_year=100.0, change_1m_pct=2.0, series_id="DTWEXBGS"),
        },
        "fx": {},
    }
    out = build_fx_panel(raw_state)
    dxy = out["dxy"]
    assert dxy["label"] == "USD Broad Index"
    assert dxy["anchors"]["current"] == 102.0


def test_pairs_anchor_values():
    raw_state = {
        "global_policy": {
            "dxy": _entry(104.0, last_week=103.0, start_of_year=100.0, change_1m_pct=1.0, series_id="DX-Y.NYB"),
        },
        "fx": {
            "usdjpy": _entry(150.0, last_week=149.0, start_of_year=140.0, change_1m_pct=2.0, series_id="JPY=X"),
            "eurusd": _entry(1.08, last_week=1.07, start_of_year=1.1, change_1m_pct=-1.0, series_id="EURUSD=X"),
            "gbpusd": _entry(1.25, last_week=1.24, start_of_year=1.3, change_1m_pct=-0.5, series_id="GBPUSD=X"),
            "usdcad": _entry(1.35, last_week=1.34, start_of_year=1.33, change_1m_pct=1.5, series_id="CAD=X"),
        },
    }
    out = build_fx_panel(raw_state)
    pairs = {row["pair"]: row for row in out["pairs"]}
    assert pairs["USDJPY"]["anchors"]["last_week"] == 149.0
    assert pairs["EURUSD"]["anchors"]["start_of_year"] == 1.1


def test_writer_preserves_other_blocks(tmp_path):
    raw_state = {
        "global_policy": {"dxy": _entry(104.0, series_id="DX-Y.NYB")},
        "fx": {},
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw_state))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "fx" in data
    assert data["policy"]["spot_stance"] == "Neutral"
