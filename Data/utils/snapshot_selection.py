"""Snapshot selection helper for ingestion meta."""
from datetime import datetime, timedelta
import math
from typing import Dict, Optional, Tuple


Snapshot = Optional[Tuple[datetime, float]]


def sanitize_float(value: object) -> Optional[float]:
    """Return a finite float or None for invalid values."""
    if value is None:
        return None
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    return val if math.isfinite(val) else None


def _clean_points(points: list[Tuple[datetime, float]]) -> list[Tuple[datetime, float]]:
    cleaned: list[Tuple[datetime, float]] = []
    for dt, value in points:
        val = sanitize_float(value)
        if val is None:
            continue
        cleaned.append((dt, val))
    return sorted(cleaned, key=lambda pair: pair[0])


def select_anchor(points: list[Tuple[datetime, float]], anchor_date: datetime) -> Snapshot:
    """Select the last observation on/before anchor_date, else first after."""
    cleaned = _clean_points(points)
    if not cleaned:
        return None
    before = [pair for pair in cleaned if pair[0] <= anchor_date]
    if before:
        return max(before, key=lambda pair: pair[0])
    after = [pair for pair in cleaned if pair[0] > anchor_date]
    if after:
        return min(after, key=lambda pair: pair[0])
    return None


def select_snapshots(
    points: list[Tuple[datetime, float]],
    current_year: Optional[int] = None,
) -> Dict[str, Snapshot]:
    """Select start_of_year, last_week, and current from sorted observations."""
    cleaned = _clean_points(points)
    if not cleaned:
        return {"current": None, "last_week": None, "start_of_year": None}
    current_date, _ = cleaned[-1]
    current = select_anchor(cleaned, current_date)
    last_week = select_anchor(cleaned, current_date - timedelta(days=7))
    if current_year is None:
        current_year = current_date.year
    start_anchor = datetime(current_year, 1, 1, tzinfo=current_date.tzinfo)
    start_of_year = select_anchor(cleaned, start_anchor)
    return {
        "current": current,
        "last_week": last_week,
        "start_of_year": start_of_year,
    }


def select_prior(
    points: list[Tuple[datetime, float]],
    current_date: datetime,
    days: int,
) -> Snapshot:
    """Select observation near current_date - days with anchor rules."""
    target = current_date - timedelta(days=days)
    return select_anchor(points, target)


def anchor_window_start(reference_date: datetime, padding_days: int = 10) -> datetime:
    """Compute earliest anchor date minus padding days."""
    anchors = [
        reference_date,
        reference_date - timedelta(days=7),
        reference_date - timedelta(days=30),
        datetime(reference_date.year, 1, 1, tzinfo=reference_date.tzinfo),
    ]
    earliest = min(anchors)
    return earliest - timedelta(days=padding_days)


def anchor_window_start_iso(reference_date: datetime, padding_days: int = 10) -> str:
    """ISO date for anchor window start."""
    return anchor_window_start(reference_date, padding_days=padding_days).date().isoformat()
