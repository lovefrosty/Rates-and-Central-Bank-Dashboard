from UI import dashboard


def test_format_cell_handles_none_and_numeric():
    assert dashboard._format_cell(None) == "Unavailable"
    assert dashboard._format_cell(1.234, decimals=1) == "1.2"


def test_format_table_rows_preserves_values():
    rows = [
        {"tenor": "3M", "start_of_year": None, "last_week": 1.0, "current": 2.0, "weekly_change_bps": None},
        {"tenor": "6M", "start_of_year": 0.9, "last_week": 1.1, "current": 1.2, "weekly_change_bps": 10.0},
    ]
    formatted = dashboard._format_table_rows(rows)
    assert formatted[0]["Tenor"] == "3M"
    assert formatted[1]["Weekly Change (bps)"] == 10.0
