import json
from pathlib import Path

import pytest

from Analytics.global_policy_alignment import build_global_policy_alignment, write_daily_state
from Data import fetch_global_policy
from update import build_raw_state


def test_fetcher_schema_invariance(monkeypatch):
    def _ok(series_id):
        return 1.0, {
            "series_id": series_id,
            "start_of_year": 0.8,
            "last_week": 0.9,
            "current": 1.0,
            "as_of_start_of_year": "2024-01-02",
            "as_of_last_week": "2024-12-20",
            "as_of_current": "2024-12-27",
        }, "OK", "openbb"

    monkeypatch.setattr(fetch_global_policy, "_fetch_fred_series", _ok)
    out = fetch_global_policy.fetch_ecb_deposit_rate()
    for key in ["value", "status", "source", "fetched_at", "error", "meta"]:
        assert key in out


def test_dxy_fetcher_schema(monkeypatch):
    def _history(ticker, period="1y", start_date=None, end_date=None):
        import pandas as pd

        dates = pd.date_range("2024-01-01", periods=5, freq="D")
        closes = [100.0, 101.0, 102.0, 101.5, 103.0]
        return pd.DataFrame({"date": dates, "close": closes})

    monkeypatch.setattr("Data.yfinance_provider.fetch_price_history", _history)
    out = fetch_global_policy.fetch_dxy()
    for key in ["value", "status", "source", "fetched_at", "error", "meta"]:
        assert key in out
    assert out["source"] == "yfinance"


def test_missing_series_returns_failed(monkeypatch):
    def _boom(series_id):
        raise RuntimeError("no data")

    monkeypatch.setattr(fetch_global_policy, "_fetch_fred_series", _boom)
    ecb = fetch_global_policy.fetch_ecb_deposit_rate()
    usd = fetch_global_policy.fetch_usd_index()
    dxy = fetch_global_policy.fetch_dxy()
    assert ecb["status"] == "FAILED"
    assert ecb["value"] is None
    assert usd["status"] == "FAILED"
    assert usd["value"] is None
    assert dxy["status"] in {"OK", "FAILED"}


def test_boj_manual_missing_returns_failed(monkeypatch, tmp_path):
    missing = tmp_path / "boj_stance.json"
    monkeypatch.setattr(fetch_global_policy, "MANUAL_BOJ_PATH", missing)
    out = fetch_global_policy.fetch_boj_stance_manual()
    assert out["status"] == "FAILED"
    assert out["meta"].get("stance") == "UNKNOWN"


def test_raw_state_includes_global_policy(monkeypatch):
    def _failed():
        return {
            "value": None,
            "status": "FAILED",
            "source": None,
            "fetched_at": "now",
            "error": "fail",
            "meta": {},
        }

    monkeypatch.setattr("Data.fetch_global_policy.fetch_ecb_deposit_rate", _failed)
    monkeypatch.setattr("Data.fetch_global_policy.fetch_usd_index", _failed)
    monkeypatch.setattr("Data.fetch_global_policy.fetch_dxy", _failed)
    monkeypatch.setattr("Data.fetch_global_policy.fetch_boj_stance_manual", _failed)
    raw = build_raw_state()
    assert "global_policy" in raw
    assert "ecb_deposit_rate" in raw["global_policy"]
    assert "usd_index" in raw["global_policy"]
    assert "dxy" in raw["global_policy"]
    assert "boj_stance" in raw["global_policy"]


def test_alignment_math():
    raw = {
        "global_policy": {
            "ecb_deposit_rate": {"status": "OK", "meta": {"current": 3.5, "last_week": 3.5}},
            "usd_index": {"status": "OK", "meta": {"current": 102.0, "last_week": 101.0}},
            "boj_stance": {"status": "OK", "meta": {"stance": "YCC"}},
        },
        "policy": {"effr": {"status": "OK", "value": 5.0}},
    }
    out = build_global_policy_alignment(raw)
    assert out["usd_index_weekly_change"] == pytest.approx(1.0)
    assert out["rate_diff"] == pytest.approx(1.5)
    assert out["boj_stance"] == "YCC"


def test_writer_preserves_other_blocks(tmp_path):
    raw = {
        "global_policy": {
            "ecb_deposit_rate": {"status": "OK", "meta": {"current": 3.5, "last_week": 3.5}},
            "usd_index": {"status": "OK", "meta": {"current": 102.0, "last_week": 101.0}},
            "boj_stance": {"status": "OK", "meta": {"stance": "YCC"}},
        },
        "policy": {"effr": {"status": "OK", "value": 5.0}},
    }
    raw_path = tmp_path / "raw_state.json"
    daily_path = tmp_path / "daily_state.json"
    raw_path.write_text(json.dumps(raw))
    daily_path.write_text(json.dumps({"policy": {"spot_stance": "Neutral"}}))
    write_daily_state(raw_state_path=raw_path, daily_state_path=daily_path)
    data = json.loads(daily_path.read_text())
    assert "global_policy_alignment" in data
    assert data["policy"]["spot_stance"] == "Neutral"
