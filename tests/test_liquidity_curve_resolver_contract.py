import json

from Signals.resolve_liquidity_curve import resolve_liquidity_curve


def _daily_state(rrp_change=None, tga_change=None, rrp_level=None, tga_level=None):
    return {
        "liquidity_analytics": {
            "rrp": {"level": rrp_level, "change_1w": rrp_change, "change_ytd": None},
            "tga": {"level": tga_level, "change_1w": tga_change, "change_ytd": None},
            "data_quality": {},
        }
    }


def test_resolver_reads_liquidity_analytics(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(rrp_change=-2.0, tga_change=1.0, rrp_level=10.0, tga_level=5.0)))
    resolve_liquidity_curve(path)
    data = json.loads(path.read_text())
    assert data["liquidity_curve"]["expected_liquidity"] == "Injecting"


def test_missing_inputs_degrades_explanation(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state()))
    resolve_liquidity_curve(path)
    data = json.loads(path.read_text())
    assert data["liquidity_curve"]["expected_liquidity"] == "Neutral"
    assert "missing" in data["liquidity_curve"]["explanation"].lower()
