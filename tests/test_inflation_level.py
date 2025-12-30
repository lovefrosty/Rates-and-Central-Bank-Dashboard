import pytest

from Analytics.inflation_level import build_inflation_level


def _ingestion(current, year_ago, status="OK"):
    return {
        "status": status,
        "value": current,
        "source": "test",
        "fetched_at": "now",
        "error": None,
        "meta": {"current": current, "year_ago": year_ago},
    }


def test_cpi_yoy_computation():
    raw_state = {"policy": {"cpi_level": _ingestion(110.0, 100.0)}}
    out = build_inflation_level(raw_state)
    assert out["cpi_yoy_pct"] == pytest.approx(10.0)
    assert out["inflation_proxy"]["type"] == "CPI_YoY"
    assert out["inflation_proxy"]["orientation"] == "backward-looking"
    assert out["data_quality"]["cpi_level"] == "OK"


def test_cpi_missing_year_ago_partial():
    raw_state = {"policy": {"cpi_level": _ingestion(110.0, None)}}
    out = build_inflation_level(raw_state)
    assert out["cpi_yoy_pct"] is None
    assert out["inflation_proxy"]["confidence"] == "LOW"
    assert out["data_quality"]["cpi_level"] == "PARTIAL"
