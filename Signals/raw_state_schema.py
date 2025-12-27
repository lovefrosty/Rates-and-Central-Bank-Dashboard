"""Schema helpers used by tests and validators."""
from typing import Dict, Set


EXPECTED_TOP_LEVEL_KEYS: Set[str] = {
    "meta",
    "policy",
    "policy_futures",
    "policy_witnesses",
    "inflation_witnesses",
    "labor_market",
    "credit_spreads",
    "global_policy",
    "policy_curve",
    "duration",
    "volatility",
    "liquidity",
}

EXPECTED_SECTION_KEYS = {
    "policy": {"effr", "cpi_level"},
    "policy_futures": {"zq"},
    "policy_witnesses": {"sofr"},
    "inflation_witnesses": {"cpi_headline", "cpi_core"},
    "labor_market": {"unrate", "jolts_openings", "eci"},
    "credit_spreads": {"ig_oas", "hy_oas"},
    "global_policy": {"ecb_deposit_rate", "usd_index", "dxy", "boj_stance"},
    "policy_curve": {"curve"},
    "duration": {
        "y3m_nominal",
        "y6m_nominal",
        "y1y_nominal",
        "y2y_nominal",
        "y3y_nominal",
        "y5y_nominal",
        "y7y_nominal",
        "y10_nominal",
        "y10_real",
        "y20y_nominal",
        "y30y_nominal",
    },
    "volatility": {"vix", "move", "gvz", "ovx"},
    "liquidity": {"rrp", "rrp_level", "tga_level", "walcl"},
}

INGESTION_REQUIRED_FIELDS = {"value", "status", "source", "fetched_at", "error", "meta"}


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
    required_meta = {"generated_at", "data_health"}
    if meta_keys != required_meta:
        raise ValueError(f"meta must contain only {sorted(required_meta)}")

    for section, keys in EXPECTED_SECTION_KEYS.items():
        section_data = _ensure_dict(section, data[section])
        section_keys = set(section_data.keys())
        if section == "policy_futures":
            if section_keys != keys:
                raise ValueError(f"{section} keys must be {sorted(keys)}")
            zq = _ensure_dict("policy_futures.zq", section_data.get("zq", {}))
            for _, entry in zq.items():
                validate_ingestion_object(entry)
        else:
            if section_keys != keys:
                raise ValueError(f"{section} keys must be {sorted(keys)}")
            for key in keys:
                validate_ingestion_object(section_data[key])
