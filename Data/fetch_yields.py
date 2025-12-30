"""Yield data fetchers."""
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from Data.utils.fred_provider import _try_fred_http, _try_openbb_fred
from Data.utils.snapshot_selection import anchor_window_start_iso, select_snapshots

_FRED_SERIES = {
    "y3m_nominal": "DGS3MO",
    "y6m_nominal": "DGS6MO",
    "y1y_nominal": "DGS1",
    "y2y_nominal": "DGS2",
    "y3y_nominal": "DGS3",
    "y5y_nominal": "DGS5",
    "y7y_nominal": "DGS7",
    "y10y_nominal": "DGS10",
    "y20y_nominal": "DGS20",
    "y30y_nominal": "DGS30",
    "y10_real": "DFII10",
}


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


def _fetch_fred_series(series_id: str) -> Tuple[float, Dict[str, Any], str, str]:
    start_date = anchor_window_start_iso(datetime.now(timezone.utc), padding_days=10)
    try:
        df = _try_openbb_fred(series_id, start_date=start_date)
        status = "OK"
        source = "openbb:fred"
    except Exception as openbb_exc:
        try:
            df = _try_fred_http(series_id, start_date=start_date)
            status = "OK"
            source = "fred_http"
        except Exception as fred_exc:
            raise RuntimeError(f"OpenBB failed: {openbb_exc}; FRED HTTP failed: {fred_exc}") from fred_exc

    points = _extract_points(df.to_dict("records"))
    snapshots = select_snapshots(points)
    current = snapshots["current"]
    if current is None:
        raise ValueError("no observations")
    current_date, current_value = current
    last_week = snapshots["last_week"]
    last_month = snapshots["last_month"]
    last_6m = snapshots["last_6m"]
    start_of_year = snapshots["start_of_year"]
    change_1m = None if last_month is None else current_value - last_month[1]
    roc_5d = None
    if last_week is not None and last_week[1] not in (None, 0):
        roc_5d = (current_value - last_week[1]) / last_week[1] * 100
    meta = {
        "provider": "fred",
        "series_id": series_id,
        "current": current_value,
        "last_week": None if last_week is None else last_week[1],
        "last_month": None if last_month is None else last_month[1],
        "last_6m": None if last_6m is None else last_6m[1],
        "start_of_year": None if start_of_year is None else start_of_year[1],
        "1m_change": change_1m,
        "5d_roc": roc_5d,
        "as_of_current": _format_date(current_date),
        "as_of_last_week": _format_date(None if last_week is None else last_week[0]),
        "as_of_last_month": _format_date(None if last_month is None else last_month[0]),
        "as_of_last_6m": _format_date(None if last_6m is None else last_6m[0]),
        "as_of_start_of_year": _format_date(None if start_of_year is None else start_of_year[0]),
    }
    return current_value, meta, status, source


def _fetch_nominal(series_id: str) -> Dict[str, Any]:
    try:
        value, meta, status, source = _fetch_fred_series(series_id)
        return _ingestion_object(value=value, status=status, source=source, extra=meta)
    except Exception as exc:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source=None,
            error=f"{series_id} fetch failed: {exc}",
            extra={"series_id": series_id, "provider": "fred"},
        )


def fetch_y3m_nominal() -> Dict[str, Any]:
    """Fetch 3-month nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y3m_nominal"])


def fetch_y6m_nominal() -> Dict[str, Any]:
    """Fetch 6-month nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y6m_nominal"])


def fetch_y1y_nominal() -> Dict[str, Any]:
    """Fetch 1-year nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y1y_nominal"])


def fetch_y2y_nominal() -> Dict[str, Any]:
    """Fetch 2-year nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y2y_nominal"])


def fetch_y3y_nominal() -> Dict[str, Any]:
    """Fetch 3-year nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y3y_nominal"])


def fetch_y5y_nominal() -> Dict[str, Any]:
    """Fetch 5-year nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y5y_nominal"])


def fetch_y7y_nominal() -> Dict[str, Any]:
    """Fetch 7-year nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y7y_nominal"])


def fetch_y10y_nominal() -> Dict[str, Any]:
    """Fetch 10-year nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y10y_nominal"])


def fetch_y20y_nominal() -> Dict[str, Any]:
    """Fetch 20-year nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y20y_nominal"])


def fetch_y30y_nominal() -> Dict[str, Any]:
    """Fetch 30-year nominal Treasury yield (latest observation)."""
    return _fetch_nominal(_FRED_SERIES["y30y_nominal"])


def fetch_y10_nominal() -> Dict[str, Any]:
    return fetch_y10y_nominal()


def fetch_y10_real() -> Dict[str, Any]:
    """Fetch 10-year real Treasury yield (FRED: DFII10)."""
    return _fetch_nominal(_FRED_SERIES["y10_real"])
