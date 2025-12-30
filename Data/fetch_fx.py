"""FX data fetchers (yfinance)."""
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from Data.utils.snapshot_selection import select_prior, select_snapshots
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


def _pct_change(current: float, prior: Optional[Tuple[datetime, float]]) -> Optional[float]:
    if prior is None or prior[1] in (None, 0):
        return None
    return (current - prior[1]) / prior[1] * 100


def _fetch_fx_series(ticker: str) -> Dict[str, Any]:
    try:
        frame = yfinance_provider.fetch_price_history(ticker, period="1y")
        points = _extract_points(frame.to_dict("records"))
        snapshots = select_snapshots(points)
        current = snapshots["current"]
        if current is None:
            raise ValueError("no observations")
        current_date, current_value = current
        last_week = snapshots["last_week"]
        start_of_year = snapshots["start_of_year"]
        prior_1d = select_prior(points, current_date, days=1)
        prior_5d = select_prior(points, current_date, days=5)
        prior_1m = select_prior(points, current_date, days=30)
        prior_6m = select_prior(points, current_date, days=180)

        meta = {
            "series_id": ticker,
            "start_of_year": None if start_of_year is None else start_of_year[1],
            "last_week": None if last_week is None else last_week[1],
            "current": current_value,
            "1d_change_pct": _pct_change(current_value, prior_1d),
            "5d_change_pct": _pct_change(current_value, prior_5d),
            "1m_change_pct": _pct_change(current_value, prior_1m),
            "6m_change_pct": _pct_change(current_value, prior_6m),
            "as_of_start_of_year": _format_date(None if start_of_year is None else start_of_year[0]),
            "as_of_last_week": _format_date(None if last_week is None else last_week[0]),
            "as_of_current": _format_date(current_date),
        }
        return _ingestion_object(value=current_value, status="OK", source="yfinance", extra=meta)
    except Exception as exc:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source="yfinance",
            error=f"{ticker} fetch failed: {exc}",
            extra={"series_id": ticker},
        )


def fetch_usdjpy() -> Dict[str, Any]:
    """Fetch USDJPY (yfinance: JPY=X)."""
    return _fetch_fx_series("JPY=X")


def fetch_eurusd() -> Dict[str, Any]:
    """Fetch EURUSD (yfinance: EURUSD=X)."""
    return _fetch_fx_series("EURUSD=X")


def fetch_gbpusd() -> Dict[str, Any]:
    """Fetch GBPUSD (yfinance: GBPUSD=X)."""
    return _fetch_fx_series("GBPUSD=X")


def fetch_usdcad() -> Dict[str, Any]:
    """Fetch USDCAD (yfinance: CAD=X)."""
    return _fetch_fx_series("CAD=X")
