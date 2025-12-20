"""Yield data fetchers."""
from datetime import date, datetime, timezone
import os
from typing import Any, Dict, Iterable, Optional, Tuple


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ingestion_object(value: Any = None,
                      status: str = "FAILED",
                      source: Optional[str] = None,
                      error: Optional[str] = None,
                      extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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


def _latest_from_results(results: Iterable[Any], field: str) -> float:
    candidates: list[Tuple[Optional[datetime], float]] = []
    for item in results:
        if isinstance(item, dict):
            value = item.get(field)
            d = _normalize_date(item.get("date"))
        else:
            value = getattr(item, field, None)
            d = _normalize_date(getattr(item, "date", None))
        if value is None:
            continue
        try:
            value_float = float(value)
        except (TypeError, ValueError):
            continue
        candidates.append((d, value_float))
    if not candidates:
        raise ValueError(f"no values found for {field}")
    candidates.sort(key=lambda pair: pair[0] or datetime.min)
    return candidates[-1][1]


def _fetch_latest_treasury_rate(field: str, provider: Optional[str]) -> Tuple[float, Dict[str, Any]]:
    try:
        from openbb import obb
    except ImportError as exc:
        raise RuntimeError("OpenBB not installed") from exc
    if os.environ.get("PYTEST_CURRENT_TEST"):
        raise RuntimeError("OpenBB disabled in tests")
    data = obb.fixedincome.government.treasury_rates(provider=provider)
    results = getattr(data, "results", None)
    if results is None and isinstance(data, dict):
        results = data.get("results")
    if results is None:
        raise ValueError("missing results from OpenBB")
    value = _latest_from_results(results, field)
    provider_used = getattr(data, "provider", None) or provider
    meta = {"provider": provider_used, "field": field}
    return value, meta


def _try_primary(field: str) -> Dict[str, Any]:
    value, meta = _fetch_latest_treasury_rate(field, provider="federal_reserve")
    return {"value": value, "source": "openbb", "status": "OK", "meta": meta}


def _try_fallback(field: str) -> Dict[str, Any]:
    value, meta = _fetch_latest_treasury_rate(field, provider="fmp")
    return {"value": value, "source": "openbb", "status": "FALLBACK", "meta": meta}


def fetch_y3m_nominal() -> Dict[str, Any]:
    """Fetch 3-month nominal Treasury yield (latest observation)."""
    try:
        r = _try_primary("month_3")
        return _ingestion_object(value=r.get("value"), status="OK", source=r.get("source"), extra=r.get("meta"))
    except Exception:
        try:
            r = _try_fallback("month_3")
            return _ingestion_object(value=r.get("value"), status="FALLBACK", source=r.get("source"), extra=r.get("meta"))
        except Exception as e2:
            return _ingestion_object(value=None, status="FAILED", source="openbb", error=str(e2))


def fetch_y2y_nominal() -> Dict[str, Any]:
    """Fetch 2-year nominal Treasury yield (latest observation)."""
    try:
        r = _try_primary("year_2")
        return _ingestion_object(value=r.get("value"), status="OK", source=r.get("source"), extra=r.get("meta"))
    except Exception:
        try:
            r = _try_fallback("year_2")
            return _ingestion_object(value=r.get("value"), status="FALLBACK", source=r.get("source"), extra=r.get("meta"))
        except Exception as e2:
            return _ingestion_object(value=None, status="FAILED", source="openbb", error=str(e2))


def fetch_y10_nominal() -> Dict[str, Any]:
    try:
        r = _try_primary("year_10")
        return _ingestion_object(value=r.get("value"), status="OK", source=r.get("source"), extra=r.get("meta"))
    except Exception:
        try:
            r = _try_fallback("year_10")
            return _ingestion_object(value=r.get("value"), status="FALLBACK", source=r.get("source"), extra=r.get("meta"))
        except Exception as e2:
            return _ingestion_object(value=None, status="FAILED", source="openbb", error=str(e2))


def fetch_y10_real() -> Dict[str, Any]:
    try:
        r = _try_primary("year_10")
        return _ingestion_object(value=r.get("value"), status="OK", source=r.get("source"), extra=r.get("meta"))
    except Exception:
        try:
            r = _try_fallback("year_10")
            return _ingestion_object(value=r.get("value"), status="FALLBACK", source=r.get("source"), extra=r.get("meta"))
        except Exception as e2:
            return _ingestion_object(value=None, status="FAILED", source="openbb", error=str(e2))
