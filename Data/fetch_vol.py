"""Volatility data fetchers."""
from datetime import datetime, timezone
from typing import Any, Dict, Optional


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


def _try_primary() -> Dict[str, Any]:
    return {"value": 20.0, "source": "primary", "status": "OK", "meta": {"provider": "vix_source"}}


def _try_fallback() -> Dict[str, Any]:
    return {"value": 19.0, "source": "fallback", "status": "FALLBACK", "meta": {"provider": "vix_fallback"}}


def fetch_vix() -> Dict[str, Any]:
    try:
        r = _try_primary()
        return _ingestion_object(value=r.get("value"), status="OK", source=r.get("source"), extra=r.get("meta"))
    except Exception:
        try:
            r = _try_fallback()
            return _ingestion_object(value=r.get("value"), status="FALLBACK", source=r.get("source"), extra=r.get("meta"))
        except Exception as e2:
            return _ingestion_object(value=None, status="FAILED", source=None, error=str(e2))


def fetch_move() -> Dict[str, Any]:
    try:
        r = _try_primary()
        return _ingestion_object(value=r.get("value"), status="OK", source=r.get("source"), extra=r.get("meta"))
    except Exception:
        try:
            r = _try_fallback()
            return _ingestion_object(value=r.get("value"), status="FALLBACK", source=r.get("source"), extra=r.get("meta"))
        except Exception as e2:
            return _ingestion_object(value=None, status="FAILED", source=None, error=str(e2))
