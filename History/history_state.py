"""History state writer for UI-only time series."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from Data import yfinance_provider
from Data.utils.fred_provider import _try_fred_http, _try_openbb_fred
from Data.utils.snapshot_selection import sanitize_float, select_anchor
from Signals import state_paths
from Signals.json_utils import write_json


WINDOW_DAYS = {"6m": 183, "1y": 365, "5y": 1825}
MA_WINDOWS = (50, 200)
Z_WINDOW = 200


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_date(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime()
        except Exception:
            return None
    return None


def _records_from_df(df: pd.DataFrame, value_col: str) -> List[Tuple[datetime, float]]:
    records: List[Tuple[datetime, float]] = []
    for row in df.to_dict("records"):
        dt = _parse_date(row.get("date"))
        val = sanitize_float(row.get(value_col))
        if dt is None or val is None:
            continue
        records.append((dt, val))
    records.sort(key=lambda pair: pair[0])
    return records


def _slice_window(records: List[Tuple[datetime, float]], days: int) -> List[Dict[str, Any]]:
    if not records:
        return []
    last_date = records[-1][0]
    cutoff = last_date - timedelta(days=days)
    sliced = [pair for pair in records if pair[0] >= cutoff]
    return [{"date": dt.date().isoformat(), "value": value} for dt, value in sliced]


def _anchor_points(records: List[Tuple[datetime, float]]) -> Dict[str, Dict[str, Optional[float]]]:
    if not records:
        return {}
    current_date = records[-1][0]
    anchors = {
        "current": current_date,
        "last_week": current_date - timedelta(days=7),
        "last_month": current_date - timedelta(days=30),
        "start_of_year": datetime(current_date.year, 1, 1, tzinfo=current_date.tzinfo),
    }
    output: Dict[str, Dict[str, Optional[float]]] = {}
    for key, anchor_date in anchors.items():
        point = select_anchor(records, anchor_date)
        if point is None:
            output[key] = {"date": None, "value": None}
            continue
        dt, val = point
        output[key] = {"date": dt.date().isoformat(), "value": sanitize_float(val)}
    return output


def _fetch_fred_history(series_id: str, years: int = 5) -> Tuple[List[Tuple[datetime, float]], str, str]:
    start_date = (datetime.now(timezone.utc) - timedelta(days=years * 365 + 10)).date().isoformat()
    try:
        df = _try_openbb_fred(series_id, start_date=start_date)
        source = "openbb:fred"
    except Exception:
        df = _try_fred_http(series_id, start_date=start_date)
        source = "fred_http"
    records = _records_from_df(df, "value")
    return records, source, "OK"


def _fetch_yfinance_history(ticker: str, years: int = 5) -> Tuple[List[Tuple[datetime, float]], str, str]:
    start_date = (datetime.now(timezone.utc) - timedelta(days=years * 365 + 10)).date().isoformat()
    df = yfinance_provider.fetch_price_history(ticker, start_date=start_date)
    records = _records_from_df(df, "close")
    return records, "yfinance", "OK"


def _build_series_entry(
    records: List[Tuple[datetime, float]],
    source: str,
    status: str,
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {"source": source, "status": status}
    for window, days in WINDOW_DAYS.items():
        entry[window] = _slice_window(records, days)
    entry["anchors"] = _anchor_points(records)
    return entry


def _series_from_records(records: List[Tuple[datetime, float]]) -> pd.Series:
    if not records:
        return pd.Series(dtype="float64")
    idx = pd.to_datetime([dt for dt, _ in records])
    vals = [val for _, val in records]
    return pd.Series(vals, index=idx).sort_index()


def _series_records(series: pd.Series, start_date: datetime) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for dt, value in series.items():
        if dt < start_date:
            continue
        val = sanitize_float(value)
        if val is None:
            continue
        rows.append({"date": dt.date().isoformat(), "value": val})
    return rows


def _volatility_transforms(
    vix_records: List[Tuple[datetime, float]],
    move_records: List[Tuple[datetime, float]],
) -> Dict[str, Any]:
    vix_series = _series_from_records(vix_records)
    move_series = _series_from_records(move_records)
    if vix_series.empty and move_series.empty:
        return {}

    df = pd.DataFrame({"vix": vix_series, "move": move_series}).sort_index()
    last_date = df.index.max()
    if pd.isna(last_date):
        return {}

    transforms: Dict[str, Any] = {"meta": {"ma_windows": list(MA_WINDOWS), "z_window": Z_WINDOW}, "windows": {}}
    ma_cols = {}
    std_cols = {}
    for window in MA_WINDOWS:
        ma_cols[window] = df.rolling(window=window, min_periods=window).mean()
        std_cols[window] = df.rolling(window=window, min_periods=window).std()

    z_mean = df.rolling(window=Z_WINDOW, min_periods=Z_WINDOW).mean()
    z_std = df.rolling(window=Z_WINDOW, min_periods=Z_WINDOW).std()
    z_scores = (df - z_mean) / z_std
    pct_of_avg = df / z_mean - 1.0

    ratio_series = df["move"] / df["vix"]
    z_spread = z_scores["move"] - z_scores["vix"]

    for window, days in WINDOW_DAYS.items():
        cutoff = last_date - timedelta(days=days)
        transforms["windows"][window] = {
            "vix": {
                "mean_50": _series_records(ma_cols[50]["vix"], cutoff),
                "mean_200": _series_records(ma_cols[200]["vix"], cutoff),
                "std_50": _series_records(std_cols[50]["vix"], cutoff),
                "std_200": _series_records(std_cols[200]["vix"], cutoff),
                "zscore_200": _series_records(z_scores["vix"], cutoff),
                "pct_of_avg_200": _series_records(pct_of_avg["vix"], cutoff),
            },
            "move": {
                "mean_50": _series_records(ma_cols[50]["move"], cutoff),
                "mean_200": _series_records(ma_cols[200]["move"], cutoff),
                "std_50": _series_records(std_cols[50]["move"], cutoff),
                "std_200": _series_records(std_cols[200]["move"], cutoff),
                "zscore_200": _series_records(z_scores["move"], cutoff),
                "pct_of_avg_200": _series_records(pct_of_avg["move"], cutoff),
            },
            "move_vix_ratio": _series_records(ratio_series, cutoff),
            "move_vix_z_spread": _series_records(z_spread, cutoff),
        }
    return transforms


def build_history_state() -> Dict[str, Any]:
    series: Dict[str, Any] = {}
    vol_records: Dict[str, List[Tuple[datetime, float]]] = {}

    fred_series = {
        "rrp": "RRPONTSYD",
        "tga": "WTREGEN",
        "walcl": "WALCL",
        "unrate": "UNRATE",
        "jolts_openings": "JTSJOL",
        "eci": "ECIALLCIV",
        "real_10y": "DFII10",
        "breakeven_10y": "T10YIE",
    }
    for key, series_id in fred_series.items():
        try:
            records, source, status = _fetch_fred_history(series_id)
            series[key] = _build_series_entry(records, source, status)
        except Exception:
            series[key] = _build_series_entry([], "fred_http", "FAILED")

    vol_series = {
        "vix": "^VIX",
        "move": "^MOVE",
        "gvz": "^GVZ",
        "ovx": "^OVX",
    }
    for key, ticker in vol_series.items():
        try:
            records, source, status = _fetch_yfinance_history(ticker)
            series[key] = _build_series_entry(records, source, status)
            vol_records[key] = records
        except Exception:
            series[key] = _build_series_entry([], "yfinance", "FAILED")
            vol_records[key] = []

    fx_series = {
        "dxy": "DX-Y.NYB",
        "eurusd": "EURUSD=X",
        "gbpusd": "GBPUSD=X",
        "usdcad": "USDCAD=X",
        "usdjpy": "JPY=X",
    }
    for key, ticker in fx_series.items():
        try:
            records, source, status = _fetch_yfinance_history(ticker)
            series[key] = _build_series_entry(records, source, status)
        except Exception:
            if key == "dxy":
                try:
                    records, source, status = _fetch_fred_history("DTWEXBGS")
                    series[key] = _build_series_entry(records, source, status)
                    continue
                except Exception:
                    pass
            series[key] = _build_series_entry([], "yfinance", "FAILED")

    return {
        "meta": {"generated_at": _now_iso(), "windows": WINDOW_DAYS},
        "series": series,
        "volatility_transforms": _volatility_transforms(
            vol_records.get("vix", []),
            vol_records.get("move", []),
        ),
    }


def write_history_state(
    path: Path | str = state_paths.HISTORY_STATE_PATH,
) -> Dict[str, Any]:
    state = build_history_state()
    return write_json(path, state)


def main() -> None:
    write_history_state()


if __name__ == "__main__":
    main()
