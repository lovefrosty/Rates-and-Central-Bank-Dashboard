"""Inflation witness data fetchers."""
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from Data.utils.fred_provider import _try_fred_http, _try_openbb_fred
from Data.utils.snapshot_selection import anchor_window_start_iso, select_prior, select_snapshots


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


def _select_year_ago(points: list[Tuple[datetime, float]], current_date: datetime) -> Optional[Tuple[datetime, float]]:
    target = current_date - timedelta(days=365)
    candidates = [pair for pair in points if pair[0] <= target]
    if not candidates:
        return None
    return max(candidates, key=lambda pair: pair[0])


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
    start_of_year = snapshots["start_of_year"]
    year_ago = _select_year_ago(points, current_date)
    month_ago = select_prior(points, current_date, days=30)
    change_1m = None if month_ago is None else current_value - month_ago[1]
    roc_5d = None
    if last_week is not None and last_week[1] not in (None, 0):
        roc_5d = (current_value - last_week[1]) / last_week[1] * 100
    meta = {
        "series_id": series_id,
        "start_of_year": None if start_of_year is None else start_of_year[1],
        "last_week": None if last_week is None else last_week[1],
        "current": current_value,
        "1m_change": change_1m,
        "5d_roc": roc_5d,
        "as_of_start_of_year": _format_date(None if start_of_year is None else start_of_year[0]),
        "as_of_last_week": _format_date(None if last_week is None else last_week[0]),
        "as_of_current": _format_date(current_date),
        "year_ago": None if year_ago is None else year_ago[1],
        "as_of_year_ago": _format_date(None if year_ago is None else year_ago[0]),
    }
    return current_value, meta, status, source


def _fetch_inflation(series_id: str) -> Dict[str, Any]:
    try:
        value, meta, status, source = _fetch_fred_series(series_id)
        return _ingestion_object(value=value, status=status, source=source, extra=meta)
    except Exception as exc:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source=None,
            error=f"{series_id} fetch failed: {exc}",
            extra={"series_id": series_id},
        )


def fetch_cpi_headline() -> Dict[str, Any]:
    """Fetch CPI headline index (FRED: CPIAUCSL)."""
    return _fetch_inflation("CPIAUCSL")


def fetch_cpi_core() -> Dict[str, Any]:
    """Fetch CPI core index (FRED: CPILFESL)."""
    return _fetch_inflation("CPILFESL")
