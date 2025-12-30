"""Policy futures (ZQ) data fetchers."""
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from Data.utils.snapshot_selection import select_snapshots
from Data import yfinance_provider


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
            raw_value = item.get("close")
        else:
            raw_date = getattr(item, "date", None)
            raw_value = getattr(item, "close", None)
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


def fetch_zq_contract(ticker: str) -> Dict[str, Any]:
    """Fetch a ZQ futures contract price snapshot via yfinance."""
    try:
        frame = yfinance_provider.fetch_price_history(ticker, period="1y")
        points = _extract_points(frame.to_dict("records"))
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
        meta = {
            "ticker": ticker,
            "start_of_year": None if start_of_year is None else start_of_year[1],
            "last_week": None if last_week is None else last_week[1],
            "last_month": None if last_month is None else last_month[1],
            "last_6m": None if last_6m is None else last_6m[1],
            "current": current_value,
            "1m_change": change_1m,
            "as_of_start_of_year": _format_date(None if start_of_year is None else start_of_year[0]),
            "as_of_last_week": _format_date(None if last_week is None else last_week[0]),
            "as_of_last_month": _format_date(None if last_month is None else last_month[0]),
            "as_of_last_6m": _format_date(None if last_6m is None else last_6m[0]),
            "as_of_current": _format_date(current_date),
        }
        return _ingestion_object(
            value=current_value,
            status="OK",
            source="yfinance",
            extra=meta,
        )
    except Exception as exc:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source="yfinance",
            error=str(exc),
            extra={"ticker": ticker},
        )
