"""Schema helpers used by tests and validators.

This module provides a lightweight, discoverable description of the expected
sections and a validator that raises ValueError on violations (used by existing tests).
"""
from typing import Dict


EXPECTED_SECTION_KEYS = {
    "policy": ["effr", "cpi_yoy"],
    "duration": ["y3m_nominal", "y2y_nominal", "y10_nominal", "y10_real"],
    "volatility": ["vix", "move"],
    "liquidity": ["rrp", "walcl"],
}


def validate_raw_state(raw: Dict) -> None:
    # Top-level keys
    top = {"meta", *EXPECTED_SECTION_KEYS.keys()}
    missing = top - set(raw.keys())
    if missing:
        raise ValueError(f"Missing top-level keys: {missing}")
    extra = set(raw.keys()) - top
    if extra:
        raise ValueError(f"Unexpected top-level keys: {extra}")

    # Validate ingestion fields
    required = {"status", "value", "as_of", "source"}
    for section, keys in EXPECTED_SECTION_KEYS.items():
        section_obj = raw.get(section, {})
        for k in keys:
            item = section_obj.get(k)
            if item is None:
                raise ValueError(f"Missing key in section {section}: {k}")
            if not required.issubset(set(item.keys())):
                raise ValueError(f"Missing ingestion field in {section}.{k}")
EXPECTED_TOP_LEVEL_KEYS = {
    "meta",
    "policy",
    "duration",
    "volatility",
    "liquidity",
}

EXPECTED_SECTION_KEYS = {
    "policy": {"effr", "cpi_yoy"},
    "duration": {"y3m_nominal", "y2y_nominal", "y10_nominal", "y10_real"},
    "volatility": {"vix", "move"},
    "liquidity": {"rrp", "walcl"},
}

INGESTION_REQUIRED_FIELDS = {"status", "value", "as_of", "source"}


def _ensure_dict(name: str, value: object) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a dict")
    return value


def validate_ingestion_object(obj: object) -> None:
    data = _ensure_dict("ingestion object", obj)
    missing = INGESTION_REQUIRED_FIELDS - set(data.keys())
    if missing:
        raise ValueError(f"missing ingestion fields: {sorted(missing)}")


def validate_raw_state(raw_state: object) -> None:
    data = _ensure_dict("raw_state", raw_state)
    unexpected = set(data.keys()) - EXPECTED_TOP_LEVEL_KEYS
    if unexpected:
        raise ValueError(f"unexpected top-level keys: {sorted(unexpected)}")
    missing = EXPECTED_TOP_LEVEL_KEYS - set(data.keys())
    if missing:
        raise ValueError(f"missing top-level keys: {sorted(missing)}")

    meta = _ensure_dict("meta", data["meta"])
    meta_keys = set(meta.keys())
    if meta_keys != {"generated_at"}:
        raise ValueError("meta must contain only generated_at")

    for section, keys in EXPECTED_SECTION_KEYS.items():
        section_data = _ensure_dict(section, data[section])
        section_keys = set(section_data.keys())
        if section_keys != keys:
            raise ValueError(f"{section} keys must be {sorted(keys)}")
        for key in keys:
            validate_ingestion_object(section_data[key])
