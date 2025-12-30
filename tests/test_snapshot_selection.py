from datetime import datetime

from Data.utils.snapshot_selection import select_snapshots


def test_select_snapshots_start_of_year():
    points = [
        (datetime(2023, 12, 31), 1.0),
        (datetime(2024, 1, 2), 2.0),
        (datetime(2024, 2, 1), 3.0),
    ]
    snapshots = select_snapshots(points)
    assert snapshots["current"] == (datetime(2024, 2, 1), 3.0)
    assert snapshots["start_of_year"] == (datetime(2023, 12, 31), 1.0)


def test_select_snapshots_last_week_offset():
    points = [
        (datetime(2024, 1, 1), 1.0),
        (datetime(2024, 1, 2), 2.0),
        (datetime(2024, 1, 3), 3.0),
        (datetime(2024, 1, 4), 4.0),
        (datetime(2024, 1, 5), 5.0),
        (datetime(2024, 1, 8), 6.0),
    ]
    snapshots = select_snapshots(points)
    assert snapshots["last_week"] == (datetime(2024, 1, 1), 1.0)


def test_select_snapshots_insufficient_data():
    points = [
        (datetime(2024, 1, 1), 1.0),
        (datetime(2024, 1, 2), 2.0),
        (datetime(2024, 1, 3), 3.0),
    ]
    snapshots = select_snapshots(points)
    assert snapshots["last_week"] == (datetime(2024, 1, 1), 1.0)
    assert snapshots["current"] == (datetime(2024, 1, 3), 3.0)


def test_select_snapshots_empty():
    snapshots = select_snapshots([])
    assert snapshots["current"] is None
    assert snapshots["last_week"] is None
    assert snapshots["start_of_year"] is None
