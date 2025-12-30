from UI import dashboard


def test_format_cell_handles_none_and_numeric():
    assert dashboard._format_cell(None) == "—"
    assert dashboard._format_cell(1.234, decimals=1) == "1.2"


def test_formatters_handle_none_and_numbers():
    percent = dashboard._percent_formatter(2)
    bps = dashboard._bps_formatter(1)
    assert percent(None) == "—"
    assert percent(1.234) == "1.23%"
    assert bps(None) == "—"
    assert bps(10.0) == "10.0 bps"
