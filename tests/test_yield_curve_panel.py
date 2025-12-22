from Analytics.yield_curve_panel import TENOR_ORDER, build_yield_curve_panel


def _ingestion(current, last_week, start_of_year):
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
        },
    }


def test_panel_builds_order_and_weekly_change():
    duration = {}
    for _, key in TENOR_ORDER:
        duration[key] = _ingestion(current=2.0, last_week=1.5, start_of_year=1.0)
    raw_state = {"duration": duration}

    panel = build_yield_curve_panel(raw_state)
    tenors = [label for label, _ in TENOR_ORDER]
    assert panel["curve_lines"]["tenors"] == tenors
    assert len(panel["table_rows"]) == len(tenors)
    assert panel["table_rows"][0]["weekly_change_bps"] == (2.0 - 1.5) * 100
