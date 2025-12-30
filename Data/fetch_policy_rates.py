"""Foreign policy rate fetchers for FX differentials."""
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from Data.utils.fred_provider import _try_fred_http, _try_openbb_fred
from Data.utils.snapshot_selection import anchor_window_start_iso, select_snapshots


POLICY_RATE_SERIES = {
    "eur": "ECBDFR",
    "gbp": None,
    "jpy": None,
    "chf": None,
    "aud": None,
    "nzd": None,
    "cad": None,
    "cnh": None,
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
    meta = {
        "series_id": series_id,
        "start_of_year": None if start_of_year is None else start_of_year[1],
        "last_week": None if last_week is None else last_week[1],
        "last_month": None if last_month is None else last_month[1],
        "last_6m": None if last_6m is None else last_6m[1],
        "current": current_value,
        "as_of_start_of_year": _format_date(None if start_of_year is None else start_of_year[0]),
        "as_of_last_week": _format_date(None if last_week is None else last_week[0]),
        "as_of_last_month": _format_date(None if last_month is None else last_month[0]),
        "as_of_last_6m": _format_date(None if last_6m is None else last_6m[0]),
        "as_of_current": _format_date(current_date),
    }
    return current_value, meta, status, source


def _fetch_policy_rate(series_id: Optional[str]) -> Dict[str, Any]:
    if not series_id:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source=None,
            error="policy rate series_id not configured",
            extra={"series_id": series_id},
        )
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


def fetch_policy_rate_eur() -> Dict[str, Any]:
    """Fetch EUR policy proxy (ECB deposit facility rate)."""
    return _fetch_policy_rate(POLICY_RATE_SERIES["eur"])


def fetch_policy_rate_gbp() -> Dict[str, Any]:
    """Fetch GBP policy proxy (series not configured)."""
    return _fetch_policy_rate(POLICY_RATE_SERIES["gbp"])


def fetch_policy_rate_jpy() -> Dict[str, Any]:
    """Fetch JPY policy proxy (series not configured)."""
    return _fetch_policy_rate(POLICY_RATE_SERIES["jpy"])


def fetch_policy_rate_chf() -> Dict[str, Any]:
    """Fetch CHF policy proxy (series not configured)."""
    return _fetch_policy_rate(POLICY_RATE_SERIES["chf"])


def fetch_policy_rate_aud() -> Dict[str, Any]:
    """Fetch AUD policy proxy (series not configured)."""
    return _fetch_policy_rate(POLICY_RATE_SERIES["aud"])


def fetch_policy_rate_nzd() -> Dict[str, Any]:
    """Fetch NZD policy proxy (series not configured)."""
    return _fetch_policy_rate(POLICY_RATE_SERIES["nzd"])


def fetch_policy_rate_cad() -> Dict[str, Any]:
    """Fetch CAD policy proxy (series not configured)."""
    return _fetch_policy_rate(POLICY_RATE_SERIES["cad"])


def fetch_policy_rate_cnh() -> Dict[str, Any]:
    """Fetch CNH policy proxy (series not configured)."""
    return _fetch_policy_rate(POLICY_RATE_SERIES["cnh"])
