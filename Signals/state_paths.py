"""Shared paths for signal state files."""
from pathlib import Path

RAW_STATE_PATH = Path("signals/raw_state.json")
DAILY_STATE_PATH = Path("signals/daily_state.json")


def raw_state_path() -> Path:
    return RAW_STATE_PATH


def daily_state_path() -> Path:
    return DAILY_STATE_PATH
