"""History state writer for UI-only time series."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from Data import yfinance_provider
from Data.utils.fred_provider import _try_fred_http, _try_openbb_fred
from Data.utils.snapshot_selection import sanitize_float
from Signals import state_paths
from Signals.json_utils import write_json


WINDOW_DAYS = {"1y": 365, "3y": 1095, "5y": 1825}
ROLLING_WINDOWS = {"1y": 252, "3y": 756}
ROC_WINDOWS = (5, 20)


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


def _fetch_fred_history(series_id: str, years: int = 5) -> Tuple[List[Tuple[datetime, float]], str, str]:
    start_date = (datetime.now(timezone.utc) - timedelta(days=years * 365 + 10)).date().isoformat()
    try:
        df = _try_openbb_fred(series_id, start_date=start_date)
        source = "openbb:fred"
    except Exception:
        df = _try_fred_http(series_id, start_date=start_date)
        source = "fred_http"
    records = _records_from_df(df, "value")
    status = "OK" if records else "FAILED"
    return records, source, status


def _fetch_yfinance_history(ticker: str, years: int = 5) -> Tuple[List[Tuple[datetime, float]], str, str]:
    start_date = (datetime.now(timezone.utc) - timedelta(days=years * 365 + 10)).date().isoformat()
    df = yfinance_provider.fetch_price_history(ticker, start_date=start_date)
    records = _records_from_df(df, "close")
    status = "OK" if records else "FAILED"
    return records, "yfinance", status


def _series_entry(
    records: List[Tuple[datetime, float]],
    source: str,
    status: str,
    series_id: str,
) -> Dict[str, Any]:
    dates = [dt.date().isoformat() for dt, _ in records]
    values = [sanitize_float(value) for _, value in records]
    return {
        "series_id": series_id,
        "source": source,
        "status": status,
        "dates": dates,
        "values": values,
    }


def _series_from_records(records: List[Tuple[datetime, float]]) -> pd.Series:
    if not records:
        return pd.Series(dtype="float64")
    idx = pd.to_datetime([dt for dt, _ in records])
    vals = [val for _, val in records]
    return pd.Series(vals, index=idx).sort_index()


def _series_block(series: pd.Series) -> Dict[str, Any]:
    if series.empty:
        return {"dates": [], "values": []}
    dates = [dt.date().isoformat() for dt in series.index]
    values = [sanitize_float(val) for val in series.values]
    return {"dates": dates, "values": values}


def _rolling(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window)


def _roc(series: pd.Series, window: int) -> pd.Series:
    return series.pct_change(periods=window) * 100


def _realized_vol(series: pd.Series, window: int = 20) -> pd.Series:
    returns = series.pct_change()
    return returns.rolling(window=window, min_periods=window).std() * (252**0.5) * 100


def _transforms_for_series(series: pd.Series, include_realized_vol: bool = False) -> Dict[str, Any]:
    if series.empty:
        return {}
    mean_1y = _rolling(series, ROLLING_WINDOWS["1y"]).mean()
    std_1y = _rolling(series, ROLLING_WINDOWS["1y"]).std()
    mean_3y = _rolling(series, ROLLING_WINDOWS["3y"]).mean()
    std_3y = _rolling(series, ROLLING_WINDOWS["3y"]).std()
    zscore_3y = (series - mean_3y) / std_3y
    pct_of_avg_3y = series / mean_3y - 1.0

    transforms = {
        "mean_1y": _series_block(mean_1y),
        "std_1y": _series_block(std_1y),
        "mean_3y": _series_block(mean_3y),
        "std_3y": _series_block(std_3y),
        "zscore_3y": _series_block(zscore_3y),
        "pct_of_avg_3y": _series_block(pct_of_avg_3y),
        "roc_5d_pct": _series_block(_roc(series, ROC_WINDOWS[0])),
        "roc_20d_pct": _series_block(_roc(series, ROC_WINDOWS[1])),
    }
    if include_realized_vol:
        realized = _realized_vol(series)
        transforms["realized_vol_20d_pct"] = _series_block(realized)
        transforms["realized_vol_20d_zscore_3y"] = _series_block(
            (realized - _rolling(realized, ROLLING_WINDOWS["3y"]).mean())
            / _rolling(realized, ROLLING_WINDOWS["3y"]).std()
        )
    return transforms


def _cross_asset_transforms(vix: pd.Series, move: pd.Series) -> Dict[str, Any]:
    if vix.empty or move.empty:
        return {}
    aligned = pd.DataFrame({"vix": vix, "move": move}).dropna()
    if aligned.empty:
        return {}
    mean_3y = aligned.rolling(window=ROLLING_WINDOWS["3y"], min_periods=ROLLING_WINDOWS["3y"]).mean()
    std_3y = aligned.rolling(window=ROLLING_WINDOWS["3y"], min_periods=ROLLING_WINDOWS["3y"]).std()
    zscores = (aligned - mean_3y) / std_3y
    z_spread = zscores["move"] - zscores["vix"]
    z_ratio = zscores["move"] / zscores["vix"].replace(0, pd.NA)
    corr_60d = aligned["move"].rolling(window=60, min_periods=60).corr(aligned["vix"])
    corr_120d = aligned["move"].rolling(window=120, min_periods=120).corr(aligned["vix"])
    return {
        "move_vix_z_spread": _series_block(z_spread),
        "move_vix_z_ratio": _series_block(z_ratio),
        "move_vix_corr_60d": _series_block(corr_60d),
        "move_vix_corr_120d": _series_block(corr_120d),
    }


def build_history_state() -> Dict[str, Any]:
    series: Dict[str, Any] = {}
    transforms: Dict[str, Any] = {}
    records_map: Dict[str, List[Tuple[datetime, float]]] = {}

    fred_series = {
        "rrp": "RRPONTSYD",
        "tga": "WTREGEN",
        "walcl": "WALCL",
        "unrate": "UNRATE",
        "jolts_openings": "JTSJOL",
        "eci": "ECIALLCIV",
        "ig_oas": "BAMLC0A0CM",
        "hy_oas": "BAMLH0A0HYM2",
        "real_10y": "DFII10",
        "breakeven_10y": "T10YIE",
    }
    for key, series_id in fred_series.items():
        try:
            records, source, status = _fetch_fred_history(series_id)
        except Exception:
            records, source, status = [], "fred_http", "FAILED"
        series[key] = _series_entry(records, source, status, series_id)
        records_map[key] = records

    vol_series = {
        "vix": "^VIX",
        "move": "^MOVE",
        "gvz": "^GVZ",
        "ovx": "^OVX",
    }
    for key, ticker in vol_series.items():
        try:
            records, source, status = _fetch_yfinance_history(ticker)
        except Exception:
            records, source, status = [], "yfinance", "FAILED"
        series[key] = _series_entry(records, source, status, ticker)
        records_map[key] = records

    fx_series = {
        "dxy": "DX-Y.NYB",
        "eurusd": "EURUSD=X",
        "gbpusd": "GBPUSD=X",
        "usdcad": "CAD=X",
        "usdjpy": "JPY=X",
        "audusd": "AUDUSD=X",
        "nzdusd": "NZDUSD=X",
        "usdnok": "NOK=X",
        "usdmxn": "MXN=X",
        "usdzar": "ZAR=X",
        "usdchf": "CHF=X",
        "usdcnh": "CNH=X",
    }
    for key, ticker in fx_series.items():
        try:
            records, source, status = _fetch_yfinance_history(ticker)
            series_id = ticker
        except Exception:
            records, source, status = [], "yfinance", "FAILED"
            series_id = ticker

        if key == "dxy" and not records:
            try:
                records, source, status = _fetch_fred_history("DTWEXBGS")
                series_id = "DTWEXBGS"
            except Exception:
                records, source, status = [], "fred_http", "FAILED"
                series_id = "DTWEXBGS"

        if key == "usdcnh" and not records:
            try:
                records, source, status = _fetch_yfinance_history("CNY=X")
                series_id = "CNY=X"
            except Exception:
                records, source, status = [], "yfinance", "FAILED"
                series_id = "CNY=X"

        series[key] = _series_entry(records, source, status, series_id)
        records_map[key] = records

    for key, records in records_map.items():
        series_obj = _series_from_records(records)
        include_realized = key in {
            "dxy",
            "eurusd",
            "gbpusd",
            "usdcad",
            "usdjpy",
            "audusd",
            "nzdusd",
            "usdnok",
            "usdmxn",
            "usdzar",
            "usdchf",
            "usdcnh",
        }
        transforms[key] = _transforms_for_series(series_obj, include_realized_vol=include_realized)

    cross_asset = _cross_asset_transforms(
        _series_from_records(records_map.get("vix", [])),
        _series_from_records(records_map.get("move", [])),
    )

    return {
        "meta": {
            "generated_at": _now_iso(),
            "rolling_windows": ROLLING_WINDOWS,
            "roc_windows": list(ROC_WINDOWS),
        },
        "series": series,
        "transforms": transforms,
        "cross_asset": cross_asset,
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
