"""Global policy witness data fetchers."""
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from Data.utils.fred_provider import _try_fred_http, _try_openbb_fred
from Data.utils.snapshot_selection import anchor_window_start_iso, select_prior, select_snapshots
from Data import yfinance_provider


MANUAL_BOJ_PATH = Path("config/boj_stance.json")


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


def _extract_price_points(results: Iterable[Any]) -> list[Tuple[datetime, float]]:
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
    }
    return current_value, meta, status, source


def _fetch_yfinance_series(ticker: str) -> Tuple[float, Dict[str, Any], str, str]:
    frame = yfinance_provider.fetch_price_history(ticker, period="1y")
    points = _extract_price_points(frame.to_dict("records"))
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

    def _pct_change(current_val: float, prior: Optional[Tuple[datetime, float]]) -> Optional[float]:
        if prior is None or prior[1] in (None, 0):
            return None
        return (current_val - prior[1]) / prior[1] * 100

    values = [value for _, value in points]
    year_high = max(values) if values else None
    year_low = min(values) if values else None

    meta = {
        "series_id": ticker,
        "start_of_year": None if start_of_year is None else start_of_year[1],
        "last_week": None if last_week is None else last_week[1],
        "current": current_value,
        "1d_change_pct": _pct_change(current_value, prior_1d),
        "5d_change_pct": _pct_change(current_value, prior_5d),
        "1m_change_pct": _pct_change(current_value, prior_1m),
        "6m_change_pct": _pct_change(current_value, prior_6m),
        "5d_roc": _pct_change(current_value, prior_5d),
        "year_open": None if start_of_year is None else start_of_year[1],
        "year_high": year_high,
        "year_low": year_low,
        "as_of_start_of_year": _format_date(None if start_of_year is None else start_of_year[0]),
        "as_of_last_week": _format_date(None if last_week is None else last_week[0]),
        "as_of_current": _format_date(current_date),
    }
    return current_value, meta, "OK", "yfinance"


def _fetch_global_series(series_id: str) -> Dict[str, Any]:
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


def fetch_ecb_deposit_rate() -> Dict[str, Any]:
    """Fetch ECB deposit facility rate (FRED: ECBDFR)."""
    return _fetch_global_series("ECBDFR")


def fetch_usd_index() -> Dict[str, Any]:
    """Fetch broad trade-weighted USD index (FRED: DTWEXBGS)."""
    return _fetch_global_series("DTWEXBGS")


def fetch_dxy() -> Dict[str, Any]:
    """Fetch DXY (yfinance: DX-Y.NYB)."""
    try:
        value, meta, status, source = _fetch_yfinance_series("DX-Y.NYB")
        return _ingestion_object(value=value, status=status, source=source, extra=meta)
    except Exception as exc:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source="yfinance",
            error=f"DX-Y.NYB fetch failed: {exc}",
            extra={"series_id": "DX-Y.NYB"},
        )


def fetch_boj_stance_manual() -> Dict[str, Any]:
    """Fetch BOJ stance from a manual config file, if present."""
    if not MANUAL_BOJ_PATH.exists():
        return _ingestion_object(
            value=None,
            status="FAILED",
            source=None,
            error="manual BOJ stance config missing",
            extra={"stance": "UNKNOWN"},
        )
    try:
        data = json.loads(MANUAL_BOJ_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source=None,
            error=f"manual BOJ stance config unreadable: {exc}",
            extra={"stance": "UNKNOWN"},
        )
    stance = data.get("stance") if isinstance(data, dict) else None
    allowed = {"YCC", "NIRP", "EXIT"}
    if stance not in allowed:
        return _ingestion_object(
            value=None,
            status="FAILED",
            source=None,
            error="manual BOJ stance invalid",
            extra={"stance": "UNKNOWN"},
        )
    return _ingestion_object(
        value=None,
        status="OK",
        source="MANUAL",
        error=None,
        extra={"stance": stance},
    )
