"""Validation helpers for signals/raw_state.json.

Canonical validation module for the Signals package.
"""
from typing import Any, Dict


REQUIRED_TOP_LEVEL = {"meta", "policy", "policy_witnesses", "duration", "volatility", "liquidity"}


def validate_top_level(obj: Dict[str, Any]) -> None:
    missing = REQUIRED_TOP_LEVEL - set(obj.keys())
    if missing:
        raise AssertionError(f"Missing top-level keys: {missing}")


def validate_ingestion_object(obj: Dict[str, Any]) -> None:
    # All ingestion objects must contain these keys
    required = {"value", "status", "source", "fetched_at", "error", "meta"}
    missing = required - set(obj.keys())
    if missing:
        raise AssertionError(f"Missing ingestion object keys: {missing}")
    if obj["status"] not in {"OK", "FALLBACK", "FAILED"}:
        raise AssertionError(f"Invalid status: {obj['status']}")


def validate_raw_state(raw: Dict[str, Any]) -> None:
    validate_top_level(raw)
    # Check structure for each expected subkey presence
    for category in ("policy", "policy_witnesses", "duration", "volatility", "liquidity"):
        if category not in raw:
            raise AssertionError(f"Missing category: {category}")
        if not isinstance(raw[category], dict):
            raise AssertionError(f"Category {category} must be a dict")
        for k, v in raw[category].items():
            validate_ingestion_object(v)
