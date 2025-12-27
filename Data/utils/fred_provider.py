"""Provider wrappers for FRED series retrieval."""
from __future__ import annotations

from typing import Optional

import pandas as pd

from Data.providers.fred_http import fetch_fred_observations


def _try_openbb_fred(
    series_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    from openbb import obb

    try:
        result = obb.economy.fred_series(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            provider="fred",
        )
    except Exception as first_exc:
        result = obb.economy.fred_series(
            symbol=series_id,
            start_date=start_date,
            end_date=end_date,
            provider="fred",
        )
    if hasattr(result, "to_dataframe"):
        df = result.to_dataframe()
    else:
        results = getattr(result, "results", None)
        if results is None and isinstance(result, dict):
            results = result.get("results")
        if results is None:
            raise ValueError("missing results from OpenBB")
        df = pd.DataFrame(results)

    if "date" not in df.columns or "value" not in df.columns:
        raise ValueError("OpenBB data missing date/value columns")
    return df[["date", "value"]]


def _try_fred_http(
    series_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    return fetch_fred_observations(series_id, start_date=start_date, end_date=end_date, api_key=api_key)
