"""Snapshot selection helper for ingestion meta."""
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple


Snapshot = Optional[Tuple[datetime, float]]


def select_snapshots(
    points: list[Tuple[datetime, float]],
    current_year: Optional[int] = None,
) -> Dict[str, Snapshot]:
    """Select start_of_year, last_week, and current from sorted observations."""
    if not points:
        return {"current": None, "last_week": None, "start_of_year": None}
    points = sorted(points, key=lambda pair: pair[0])
    current_date, current_value = points[-1]
    last_week = points[-6] if len(points) >= 6 else None
    if current_year is None:
        current_year = current_date.year
    start_of_year = None
    for dt, value in points:
        if dt.year == current_year:
            start_of_year = (dt, value)
            break
    return {
        "current": (current_date, current_value),
        "last_week": last_week,
        "start_of_year": start_of_year,
    }


def select_prior(
    points: list[Tuple[datetime, float]],
    current_date: datetime,
    days: int,
) -> Snapshot:
    """Select the latest observation at or before current_date - days."""
    target = current_date - timedelta(days=days)
    candidates = [pair for pair in points if pair[0] <= target]
    if not candidates:
        return None
    return max(candidates, key=lambda pair: pair[0])
