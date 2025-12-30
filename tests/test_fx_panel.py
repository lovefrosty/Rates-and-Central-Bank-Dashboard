import json

import pytest

from Analytics.fx_panel import build_fx_panel, write_daily_state


def _entry(
    current,
    last_week=None,
    last_month=None,
    last_6m=None,
    start_of_year=None,
    change_1m_pct=None,
    status="OK",
    series_id="X",
):
    meta = {
        "current": current,
        "last_week": last_week,
        "last_month": last_month,
        "last_6m": last_6m,
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


def _policy_entry(current, status="OK", series_id="X"):
    return {
        "value": current,
        "status": status,
        "meta": {"current": current, "series_id": series_id},
        "source": "openbb:fred",
    }


def test_fallback_to_usd_index():
    raw_state = {
        "global_policy": {
            "dxy": _entry(None, status="FAILED", series_id="DX-Y.NYB"),
            "usd_index": _entry(
                102.0,
                last_week=101.0,
                last_month=100.0,
                last_6m=98.0,
                start_of_year=100.0,
                change_1m_pct=2.0,
                series_id="DTWEXBGS",
            ),
        },
        "fx": {},
        "policy": {"effr": _policy_entry(5.25, series_id="EFFR")},
        "policy_rates": {"eur": _policy_entry(4.0, series_id="ECBDFR")},
    }
    out = build_fx_panel(raw_state)
    dxy = out["dxy"]
    assert dxy["label"] == "USD Broad Index"
    assert dxy["anchors"]["current"] == 102.0


def test_pairs_anchor_values():
    raw_state = {
        "global_policy": {
            "dxy": _entry(
                104.0,
                last_week=103.0,
                last_month=102.0,
                last_6m=99.0,
                start_of_year=100.0,
                change_1m_pct=1.0,
                series_id="DX-Y.NYB",
            ),
        },
        "fx": {
            "usdjpy": _entry(
                150.0,
                last_week=149.0,
                last_month=148.0,
                last_6m=145.0,
                start_of_year=140.0,
                change_1m_pct=2.0,
                series_id="JPY=X",
            ),
            "eurusd": _entry(
                1.08,
                last_week=1.07,
                last_month=1.09,
                last_6m=1.12,
                start_of_year=1.1,
                change_1m_pct=-1.0,
                series_id="EURUSD=X",
            ),
            "gbpusd": _entry(
                1.25,
                last_week=1.24,
                last_month=1.26,
                last_6m=1.28,
                start_of_year=1.3,
                change_1m_pct=-0.5,
                series_id="GBPUSD=X",
            ),
            "usdcad": _entry(
                1.35,
                last_week=1.34,
                last_month=1.33,
                last_6m=1.31,
                start_of_year=1.33,
                change_1m_pct=1.5,
                series_id="CAD=X",
            ),
        },
        "policy": {"effr": _policy_entry(5.25, series_id="EFFR")},
        "policy_rates": {
            "eur": _policy_entry(4.0, series_id="ECBDFR"),
            "gbp": _policy_entry(5.0, series_id="BOERATE"),
            "jpy": _policy_entry(0.1, series_id="BOJ"),
            "chf": _policy_entry(1.75, series_id="SNB"),
            "aud": _policy_entry(4.35, series_id="RBA"),
            "nzd": _policy_entry(5.5, series_id="RBNZ"),
            "cad": _policy_entry(5.0, series_id="BOC"),
            "cnh": _policy_entry(3.45, series_id="PBOC"),
        },
    }
    out = build_fx_panel(raw_state)
    pairs = {row["pair"]: row for row in out["pairs"]}
    assert pairs["USDJPY"]["anchors"]["last_week"] == 149.0
    assert pairs["EURUSD"]["anchors"]["start_of_year"] == 1.1

    rates = out["rate_differentials"]["rows"]
    eur_row = next(row for row in rates if row["currency"] == "EUR")
    assert eur_row["differential_bps"] == (4.0 - 5.25) * 100

    matrix = out["matrix_1m_pct"]
    assert matrix["anchor"] == "1M"
    assert "values_pct" in matrix
    eur_index = matrix["currencies"].index("EUR")
    usd_index = matrix["currencies"].index("USD")
    eur_usd_change = matrix["values_pct"][eur_index][usd_index]
    expected = (1.08 / 1.09 - 1) * 100
    assert eur_usd_change == pytest.approx(expected, rel=1e-6)

    baskets = out["risk_baskets"]
    assert "risk_on" in baskets
    assert "risk_off" in baskets
    assert "spread" in baskets


def test_writer_preserves_other_blocks(tmp_path):
    raw_state = {
        "global_policy": {"dxy": _entry(104.0, series_id="DX-Y.NYB")},
        "fx": {},
        "policy": {"effr": _policy_entry(5.25, series_id="EFFR")},
        "policy_rates": {"eur": _policy_entry(4.0, series_id="ECBDFR")},
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw_state))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "fx" in data
    assert data["policy"]["spot_stance"] == "Neutral"
