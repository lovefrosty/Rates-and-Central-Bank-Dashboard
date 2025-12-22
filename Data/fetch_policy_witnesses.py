"""Policy witness data fetchers."""
from datetime import date, datetime, timedelta, timezone
import os
from typing import Any, Dict, Iterable, Optional, Tuple

from Data.utils.snapshot_selection import select_snapshots

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ingestion_object(
    value: Any = None,
    status: str = "FAILED",
    source: Optional[str] = None,
    error: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    obj = {
        "value": value,
        "status": status,
        "source": source,
        "fetched_at": _now_iso(),
        "error": error,
        "meta": {},
    }
    if extra:
        obj["meta"].update(extra)
    return obj


def _normalize_date(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _extract_points(results: Iterable[Any]) -> list[Tuple[datetime, float]]:
    points: list[Tuple[datetime, float]] = []
    for item in results:
        if isinstance(item, dict):
            raw_date = item.get("date")
            raw_value = item.get("value")
        else:
            raw_date = getattr(item, "date", None)
            raw_value = getattr(item, "value", None)
        dt = _normalize_date(raw_date)
        if dt is None or raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        points.append((dt, value))
    return points


def _format_date(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.date().isoformat()


def _fetch_fred_series(series_id: str) -> Tuple[float, Dict[str, Any]]:
    if os.environ.get("PYTEST_CURRENT_TEST"):
        raise RuntimeError("OpenBB disabled in tests")
    try:
        from openbb import obb
    except ImportError as exc:
        raise RuntimeError("OpenBB not installed") from exc
    start_date = (datetime.now(timezone.utc) - timedelta(days=400)).date().isoformat()
    data = obb.economy.fred_series(symbol=series_id, start_date=start_date, provider="fred")
    results = getattr(data, "results", None)
    if results is None and isinstance(data, dict):
        results = data.get("results")
    if results is None:
        raise ValueError("missing results from OpenBB")
    points = _extract_points(results)
    snapshots = select_snapshots(points)
    current = snapshots["current"]
    if current is None:
        raise ValueError("no observations")
    current_date, current_value = current
    last_week = snapshots["last_week"]
    start_of_year = snapshots["start_of_year"]
    meta = {
        "series_id": series_id,
        "start_of_year": None if start_of_year is None else start_of_year[1],
        "last_week": None if last_week is None else last_week[1],
        "current": current_value,
        "as_of_start_of_year": _format_date(None if start_of_year is None else start_of_year[0]),
        "as_of_last_week": _format_date(None if last_week is None else last_week[0]),
        "as_of_current": _format_date(current_date),
    }
    return current_value, meta


def fetch_sofr() -> Dict[str, Any]:
    """Fetch SOFR level (latest observation)."""
    series_id = "SOFR"
    try:
        value, meta = _fetch_fred_series(series_id)
        return _ingestion_object(value=value, status="OK", source="openbb", extra=meta)
    except Exception as exc:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source=None,
            error=f"{series_id} fetch failed: {exc}",
            extra={"series_id": series_id},
        )
