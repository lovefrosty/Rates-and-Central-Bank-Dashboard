"""FX panel analytics from raw_state.json."""
# NOTE: Evidence-only block. No resolver consumes this data in V1.
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any, Dict, Optional

from Signals import state_paths
from Signals.json_utils import write_json


FX_ORDER = [
    ("USDJPY", "usdjpy"),
    ("EURUSD", "eurusd"),
    ("GBPUSD", "gbpusd"),
    ("USDCAD", "usdcad"),
    ("AUDUSD", "audusd"),
    ("NZDUSD", "nzdusd"),
    ("USDNOK", "usdnok"),
    ("USDMXN", "usdmxn"),
    ("USDZAR", "usdzar"),
    ("USDCHF", "usdchf"),
    ("USDCNH", "usdcnh"),
]

FX_MATRIX_CURRENCIES = ["USD", "EUR", "JPY", "GBP", "CNH", "CHF", "AUD", "CAD"]
FX_TO_USD = {
    "EUR": ("eurusd", "direct"),
    "GBP": ("gbpusd", "direct"),
    "AUD": ("audusd", "direct"),
    "NZD": ("nzdusd", "direct"),
    "JPY": ("usdjpy", "inverse"),
    "CAD": ("usdcad", "inverse"),
    "NOK": ("usdnok", "inverse"),
    "MXN": ("usdmxn", "inverse"),
    "ZAR": ("usdzar", "inverse"),
    "CHF": ("usdchf", "inverse"),
    "CNH": ("usdcnh", "inverse"),
}
POLICY_RATE_KEYS = {
    "EUR": "eur",
    "GBP": "gbp",
    "JPY": "jpy",
    "CHF": "chf",
    "AUD": "aud",
    "NZD": "nzd",
    "CAD": "cad",
    "CNH": "cnh",
}
RISK_ON_CURRENCIES = ["AUD", "NZD", "NOK", "MXN", "ZAR"]
RISK_OFF_CURRENCIES = ["JPY", "CHF", "USD"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



def _get_entry(raw_state: Dict[str, Any], section: str, key: str) -> Dict[str, Any]:
    container = raw_state.get(section, {})
    if not isinstance(container, dict):
        return {}
    entry = container.get(key, {})
    return entry if isinstance(entry, dict) else {}


def _anchors_from_meta(entry: Dict[str, Any]) -> Dict[str, Optional[float]]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    current = meta.get("current", entry.get("value"))
    last_week = meta.get("last_week")
    last_month = meta.get("last_month")
    last_6m = meta.get("last_6m")
    start_of_year = meta.get("start_of_year")
    change_1m_pct = meta.get("1m_change_pct")
    change_6m_pct = meta.get("6m_change_pct")
    if last_month is None and current is not None and change_1m_pct not in (None, -100):
        last_month = current / (1 + (change_1m_pct / 100))
    if last_6m is None and current is not None and change_6m_pct not in (None, -100):
        last_6m = current / (1 + (change_6m_pct / 100))
    return {
        "current": None if current is None else float(current),
        "last_week": None if last_week is None else float(last_week),
        "last_month": None if last_month is None else float(last_month),
        "last_6m": None if last_6m is None else float(last_6m),
        "start_of_year": None if start_of_year is None else float(start_of_year),
    }


def _changes_pct(entry: Dict[str, Any]) -> Dict[str, Optional[float]]:
    meta = entry.get("meta", {}) if isinstance(entry, dict) else {}
    return {
        "1d": meta.get("1d_change_pct"),
        "5d": meta.get("5d_change_pct"),
        "1m": meta.get("1m_change_pct"),
        "6m": meta.get("6m_change_pct"),
    }


def _quality(entry: Dict[str, Any], anchors: Dict[str, Optional[float]]) -> str:
    status = entry.get("status") if isinstance(entry, dict) else None
    if status == "FAILED":
        return "FAILED"
    if all(value is not None for value in anchors.values()):
        return "OK"
    if any(value is not None for value in anchors.values()):
        return "PARTIAL"
    return "FAILED"


def _anchor_value(entry: Dict[str, Any], key: str) -> Optional[float]:
    return _anchors_from_meta(entry).get(key)


def _usd_per_currency(raw_state: Dict[str, Any], currency: str, anchor_key: str) -> Optional[float]:
    if currency == "USD":
        return 1.0
    mapping = FX_TO_USD.get(currency)
    if not mapping:
        return None
    fx_key, orientation = mapping
    entry = _get_entry(raw_state, "fx", fx_key)
    value = _anchor_value(entry, anchor_key)
    if value is None:
        return None
    if orientation == "direct":
        return value
    if value == 0:
        return None
    return 1.0 / value


def _build_rate_differentials(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    fed_entry = _get_entry(raw_state, "policy", "effr")
    fed_meta = fed_entry.get("meta", {}) if isinstance(fed_entry, dict) else {}
    fed_value = fed_meta.get("current", fed_entry.get("value"))
    fed_value = None if fed_value is None else float(fed_value)
    fed_status = fed_entry.get("status") if isinstance(fed_entry, dict) else "FAILED"

    rows = []
    for currency, key in POLICY_RATE_KEYS.items():
        rate_entry = _get_entry(raw_state, "policy_rates", key)
        meta = rate_entry.get("meta", {}) if isinstance(rate_entry, dict) else {}
        rate_value = meta.get("current", rate_entry.get("value"))
        rate_value = None if rate_value is None else float(rate_value)
        rate_status = rate_entry.get("status") if isinstance(rate_entry, dict) else "FAILED"
        diff_bps = None
        if rate_value is not None and fed_value is not None:
            diff_bps = (rate_value - fed_value) * 100

        if rate_status == "FAILED" or fed_status == "FAILED":
            quality = "FAILED"
        elif rate_value is not None and fed_value is not None:
            quality = "OK"
        elif rate_value is not None or fed_value is not None:
            quality = "PARTIAL"
        else:
            quality = "FAILED"

        rows.append(
            {
                "currency": currency,
                "policy_rate": rate_value,
                "fed_funds": fed_value,
                "differential_bps": diff_bps,
                "policy_series_id": meta.get("series_id"),
                "fed_series_id": fed_meta.get("series_id"),
                "data_quality": quality,
                "inputs_used": {
                    "policy_rate": rate_value is not None,
                    "fed_funds": fed_value is not None,
                },
            }
        )
    return {
        "computed_at": _now_iso(),
        "rows": rows,
        "inputs_used": ["policy_rates", "policy.effr"],
    }


def _build_fx_matrix(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    anchor_key = "last_month"
    current_key = "current"
    currencies = FX_MATRIX_CURRENCIES
    anchor_values = {ccy: _usd_per_currency(raw_state, ccy, anchor_key) for ccy in currencies}
    current_values = {ccy: _usd_per_currency(raw_state, ccy, current_key) for ccy in currencies}

    matrix: list[list[Optional[float]]] = []
    for base in currencies:
        row: list[Optional[float]] = []
        for quote in currencies:
            base_current = current_values.get(base)
            quote_current = current_values.get(quote)
            base_anchor = anchor_values.get(base)
            quote_anchor = anchor_values.get(quote)
            if base_current is None or quote_current in (None, 0):
                row.append(None)
                continue
            if base_anchor is None or quote_anchor in (None, 0):
                row.append(None)
                continue
            current_rate = base_current / quote_current
            anchor_rate = base_anchor / quote_anchor
            if anchor_rate == 0:
                row.append(None)
            else:
                row.append((current_rate / anchor_rate - 1) * 100)
        matrix.append(row)

    available = sum(
        1
        for ccy in currencies
        if anchor_values.get(ccy) is not None and current_values.get(ccy) is not None
    )
    if available == len(currencies):
        quality = "OK"
    elif available > 0:
        quality = "PARTIAL"
    else:
        quality = "FAILED"
    return {
        "anchor": "1M",
        "currencies": currencies,
        "values_pct": matrix,
        "data_quality": quality,
        "computed_at": _now_iso(),
    }


def _basket_index(raw_state: Dict[str, Any], currencies: list[str]) -> Dict[str, Any]:
    anchors = ["start_of_year", "last_6m", "last_month", "last_week", "current"]
    base_anchor = "start_of_year"
    included = []
    missing = []

    base_values = {}
    for currency in currencies:
        value = _usd_per_currency(raw_state, currency, base_anchor)
        if value is None:
            missing.append(currency)
        else:
            base_values[currency] = value
            included.append(currency)

    index_anchors: Dict[str, Optional[float]] = {}
    for anchor in anchors:
        values = []
        for currency in included:
            base_value = base_values.get(currency)
            anchor_value = _usd_per_currency(raw_state, currency, anchor)
            if base_value in (None, 0) or anchor_value is None:
                continue
            values.append((anchor_value / base_value) * 100)
        index_anchors[anchor] = sum(values) / len(values) if values else None

    if not included:
        quality = "FAILED"
    elif any(index_anchors[anchor] is None for anchor in anchors):
        quality = "PARTIAL"
    else:
        quality = "OK"

    return {
        "anchors": index_anchors,
        "constituents_used": included,
        "constituents_missing": missing,
        "base_anchor": base_anchor,
        "index_base": 100,
        "data_quality": quality,
        "computed_at": _now_iso(),
    }


def _build_risk_baskets(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    risk_on = _basket_index(raw_state, RISK_ON_CURRENCIES)
    risk_off = _basket_index(raw_state, RISK_OFF_CURRENCIES)
    spread = {}
    for anchor in ("start_of_year", "last_6m", "last_month", "last_week", "current"):
        on_val = risk_on["anchors"].get(anchor)
        off_val = risk_off["anchors"].get(anchor)
        spread[anchor] = None if on_val is None or off_val is None else on_val - off_val
    return {
        "risk_on": risk_on,
        "risk_off": risk_off,
        "spread": {
            "anchors": spread,
            "computed_at": _now_iso(),
        },
    }


def _resolve_dxy(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    dxy_entry = _get_entry(raw_state, "global_policy", "dxy")
    dxy_anchors = _anchors_from_meta(dxy_entry)
    dxy_quality = _quality(dxy_entry, dxy_anchors)
    if dxy_quality == "FAILED":
        usd_entry = _get_entry(raw_state, "global_policy", "usd_index")
        usd_anchors = _anchors_from_meta(usd_entry)
        usd_quality = _quality(usd_entry, usd_anchors)
        return {
            "label": "USD Broad Index",
            "series_id": usd_entry.get("meta", {}).get("series_id"),
            "anchors": usd_anchors,
            "changes_pct": _changes_pct(usd_entry),
            "data_quality": usd_quality,
            "source": usd_entry.get("source"),
        }
    return {
        "label": "DXY",
        "series_id": dxy_entry.get("meta", {}).get("series_id"),
        "anchors": dxy_anchors,
        "changes_pct": _changes_pct(dxy_entry),
        "data_quality": dxy_quality,
        "source": dxy_entry.get("source"),
    }


def build_fx_panel(raw_state: Dict[str, Any]) -> Dict[str, Any]:
    pairs = []
    for label, key in FX_ORDER:
        entry = _get_entry(raw_state, "fx", key)
        anchors = _anchors_from_meta(entry)
        pairs.append(
            {
                "pair": label,
                "series_id": entry.get("meta", {}).get("series_id"),
                "anchors": anchors,
                "data_quality": _quality(entry, anchors),
            }
        )

    dxy = _resolve_dxy(raw_state)

    return {
        "dxy": dxy,
        "pairs": pairs,
        "rate_differentials": _build_rate_differentials(raw_state),
        "matrix_1m_pct": _build_fx_matrix(raw_state),
        "risk_baskets": _build_risk_baskets(raw_state),
    }


def write_daily_state(
    raw_state_path: Path | str = state_paths.RAW_STATE_PATH,
    daily_state_path: Path | str = state_paths.DAILY_STATE_PATH,
) -> Dict[str, Any]:
    raw_state = json.loads(Path(raw_state_path).read_text(encoding="utf-8"))
    daily_path = Path(daily_state_path)
    daily: Dict[str, Any] = {}
    if daily_path.exists():
        daily = json.loads(daily_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(daily, dict):
            daily = {}
    daily["fx"] = build_fx_panel(raw_state)
    write_json(daily_path, daily)
    return daily
