from pathlib import Path


def test_ui_does_not_reference_raw_state():
    content = Path("UI/dashboard.py").read_text(encoding="utf-8")
    assert "raw_state.json" not in content
    assert "load_raw_state" not in content


def test_resolvers_do_not_reference_raw_state():
    for path in Path("Signals").glob("resolve_*.py"):
        content = path.read_text(encoding="utf-8")
        assert "raw_state.json" not in content


def test_no_history_state_in_analytics_or_resolvers():
    for path in Path("Signals").glob("resolve_*.py"):
        content = path.read_text(encoding="utf-8")
        assert "history_state.json" not in content
    for path in Path("Analytics").glob("*.py"):
        content = path.read_text(encoding="utf-8")
        assert "history_state.json" not in content
