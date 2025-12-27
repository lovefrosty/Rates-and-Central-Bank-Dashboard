"""yfinance provider wrapper for close-price history."""
from __future__ import annotations

from typing import Optional

import pandas as pd


def fetch_price_history(
    ticker: str,
    period: str = "6mo",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Return a DataFrame with columns [date, close] for the given ticker."""
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance not installed") from exc

    ticker_obj = yf.Ticker(ticker)
    if start_date or end_date:
        history = ticker_obj.history(start=start_date, end=end_date)
    else:
        history = ticker_obj.history(period=period)

    if history is None or history.empty:
        raise ValueError(f"no history for {ticker}")
    if "Close" not in history.columns:
        raise ValueError(f"missing Close column for {ticker}")

    frame = history.reset_index()
    frame = frame.rename(columns={"Date": "date", "Datetime": "date", "Close": "close"})
    if "date" not in frame.columns:
        raise ValueError(f"missing date column for {ticker}")
    return frame[["date", "close"]]
