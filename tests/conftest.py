import sys
from pathlib import Path

import pandas as pd
import pytest

# Ensure repository root is importable for tests that import top-level packages
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _patch_yfinance_provider(monkeypatch):
    def _fake_history(ticker, period="6mo", start_date=None, end_date=None):
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-12-27")],
                "close": [99.5, 99.0],
            }
        )

    monkeypatch.setattr("Data.yfinance_provider.fetch_price_history", _fake_history)
