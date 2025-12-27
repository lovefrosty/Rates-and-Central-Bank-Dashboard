from pathlib import Path


def test_resolvers_do_not_reference_raw_state_json():
    for path in Path("Signals").glob("resolve_*.py"):
        content = path.read_text(encoding="utf-8")
        assert "raw_state.json" not in content
