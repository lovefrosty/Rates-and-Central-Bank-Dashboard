"""Direct FRED HTTP client (JSON observations)."""
from __future__ import annotations

from typing import Optional
import os

import io
import pandas as pd
import requests


_FRED_OBS_URL = "https://api.stlouisfed.org/fred/series/observations"
_FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


def _fetch_fred_csv(
    series_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    params = {"id": series_id}
    if start_date:
        params["cosd"] = start_date
    if end_date:
        params["coed"] = end_date
    resp = requests.get(_FRED_CSV_URL, params=params, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"FRED CSV {resp.status_code}: {resp.text[:200]}")
    df = pd.read_csv(io.StringIO(resp.text))
    if df.empty or df.shape[1] < 2:
        raise ValueError("FRED CSV missing data columns")
    date_col = df.columns[0]
    value_col = df.columns[1]
    rows = []
    for _, row in df.iterrows():
        raw_value = row.get(value_col)
        if raw_value in (None, "", "."):
            value = None
        else:
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                value = None
        rows.append({"date": row.get(date_col), "value": value})
    return pd.DataFrame(rows, columns=["date", "value"])


def fetch_fred_observations(
    series_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch FRED observations as a DataFrame with columns [date, value]."""
    params = {"series_id": series_id, "file_type": "json"}
    if start_date:
        params["observation_start"] = start_date
    if end_date:
        params["observation_end"] = end_date
    if api_key is None:
        api_key = os.environ.get("FRED_API_KEY")
    if api_key:
        params["api_key"] = api_key

    resp = requests.get(_FRED_OBS_URL, params=params, timeout=10)
    if resp.status_code != 200:
        if resp.status_code == 400 and "api_key" in resp.text:
            return _fetch_fred_csv(series_id, start_date=start_date, end_date=end_date)
        raise RuntimeError(f"FRED HTTP {resp.status_code}: {resp.text[:200]}")
    payload = resp.json()
    observations = payload.get("observations")
    if observations is None:
        raise ValueError("missing observations in FRED response")

    rows = []
    for item in observations:
        date = item.get("date")
        raw_value = item.get("value")
        if raw_value in (None, "", "."):
            value = None
        else:
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                value = None
        rows.append({"date": date, "value": value})

    return pd.DataFrame(rows, columns=["date", "value"])
