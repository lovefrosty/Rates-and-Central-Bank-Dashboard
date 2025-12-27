import json
from pathlib import Path
import sys
from pathlib import Path as _Path

# Allow direct execution: add repo root to sys.path
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from Signals.resolve_yield_curve import resolve_yield_curve


def _daily_state(y3m, y2y, y10y):
    return {
        "yield_curve": {
            "tenors": ["3M", "2Y", "10Y"],
            "lines": {"current": [y3m, y2y, y10y]},
            "table_rows": [],
        }
    }


def _write_daily_state(path: Path, daily_state: dict) -> None:
    path.write_text(json.dumps(daily_state), encoding="utf-8")


def _run_resolver(tmp_path, y3m, y2y, y10y):
    daily_path = tmp_path / "daily_state.json"
    _write_daily_state(daily_path, _daily_state(y3m, y2y, y10y))
    raw_path = tmp_path / "missing_raw_state.json"
    return resolve_yield_curve(raw_state_path=raw_path, daily_state_path=daily_path)


def test_regime_inverted(tmp_path):
    data = _run_resolver(tmp_path, 5.0, 4.5, 4.0)
    assert data["yield_curve"]["regime"] == "INVERTED"


def test_regime_flat(tmp_path):
    data = _run_resolver(tmp_path, 5.0, 5.02, 5.05)
    assert data["yield_curve"]["regime"] == "FLAT"


def test_regime_steep(tmp_path):
    data = _run_resolver(tmp_path, 2.0, 2.4, 3.3)
    assert data["yield_curve"]["regime"] == "STEEP"


def test_regime_normal(tmp_path):
    data = _run_resolver(tmp_path, 3.0, 3.2, 3.6)
    assert data["yield_curve"]["regime"] == "NORMAL"


def test_resolver_does_not_read_raw_state(tmp_path):
    daily_path = tmp_path / "daily_state.json"
    _write_daily_state(daily_path, _daily_state(3.0, 3.2, 3.6))
    raw_path = tmp_path / "missing_raw_state.json"
    data = resolve_yield_curve(raw_state_path=raw_path, daily_state_path=daily_path)
    assert data["yield_curve"]["regime"] == "NORMAL"
