from pathlib import Path


def test_dashboard_does_not_reference_raw_state():
    content = Path("UI/dashboard.py").read_text(encoding="utf-8")
    assert "raw_state.json" not in content
    assert "load_raw_state" not in content
