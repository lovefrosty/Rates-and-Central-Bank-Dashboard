from Analytics.yield_curve_analytics import TENOR_ORDER, build_yield_curve_block


def _ingestion(current, last_week, start_of_year, change_1m=None):
    return {
        "status": "OK",
        "value": current,
        "source": "test",
        "fetched_at": "now",
        "error": None,
        "meta": {
            "current": current,
            "last_week": last_week,
            "start_of_year": start_of_year,
            "1m_change": change_1m,
        },
    }


def test_builds_order_and_weekly_change():
    duration = {}
    for _, key in TENOR_ORDER:
        duration[key] = _ingestion(current=2.0, last_week=1.5, start_of_year=1.0, change_1m=0.2)
    raw_state = {"duration": duration}

    panel = build_yield_curve_block(raw_state)
    tenors = [label for label, _ in TENOR_ORDER]
    assert panel["tenors"] == tenors
    assert len(panel["table_rows"]) == len(tenors)
    assert panel["table_rows"][0]["weekly_change_bps"] == (2.0 - 1.5) * 100
    assert panel["table_rows"][0]["last_month"] == 1.8


def test_missing_values_propagate():
    duration = {
        TENOR_ORDER[0][1]: _ingestion(current=None, last_week=1.5, start_of_year=1.0, change_1m=0.2),
    }
    raw_state = {"duration": duration}

    panel = build_yield_curve_block(raw_state)
    assert panel["table_rows"][0]["weekly_change_bps"] is None
