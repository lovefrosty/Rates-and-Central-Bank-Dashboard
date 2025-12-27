import json

import pytest

from Signals.resolve_liquidity_curve import resolve_liquidity_curve


def _daily_state(rrp_change=None, tga_change=None, rrp_level=None, tga_level=None):
    return {
        "liquidity_analytics": {
            "rrp": {"level": rrp_level, "change_1w": rrp_change, "change_ytd": None},
            "tga": {"level": tga_level, "change_1w": tga_change, "change_ytd": None},
            "data_quality": {},
        }
    }


def test_resolver_creates_liquidity_curve_block(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(rrp_change=-10.0, tga_change=-5.0, rrp_level=120.0, tga_level=80.0)))
    resolve_liquidity_curve(path)
    data = json.loads(path.read_text())
    assert "liquidity_curve" in data
    assert data["liquidity_curve"]["expected_liquidity"] == "Injecting"


@pytest.mark.parametrize(
    "rrp_change,expected",
    [(-1.0, "Injecting"), (0.0, "Neutral"), (2.5, "Draining")],
)
def test_direction_from_rrp_change(tmp_path, rrp_change, expected):
    path = tmp_path / "daily_state.json"
    path.write_text(
        json.dumps(_daily_state(rrp_change=rrp_change, tga_change=0.0, rrp_level=100.0, tga_level=50.0))
    )
    resolve_liquidity_curve(path)
    data = json.loads(path.read_text())
    assert data["liquidity_curve"]["expected_liquidity"] == expected


def test_tga_reinforces_not_override(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(rrp_change=3.0, tga_change=-4.0, rrp_level=90.0, tga_level=60.0)))
    resolve_liquidity_curve(path)
    data = json.loads(path.read_text())
    assert data["liquidity_curve"]["expected_liquidity"] == "Draining"


def test_missing_inputs_handled(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state()))
    resolve_liquidity_curve(path)
    data = json.loads(path.read_text())
    assert data["liquidity_curve"]["expected_liquidity"] == "Neutral"
    assert "missing" in data["liquidity_curve"]["explanation"].lower()


def test_reads_only_daily_state(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(rrp_change=-1.0, tga_change=1.0, rrp_level=1.0, tga_level=1.0)))
    resolve_liquidity_curve(path)
    data = json.loads(path.read_text())
    assert "liquidity_curve" in data


def test_policy_blocks_unchanged(tmp_path):
    data = _daily_state(rrp_change=-2.0, tga_change=-1.0, rrp_level=100.0, tga_level=80.0)
    data["policy"] = {"spot_stance": "Restrictive", "explanation": "x", "inputs_used": {}}
    data["policy_curve"] = {"expected_direction": "Hold", "explanation": "y", "inputs_used": {}, "horizon": "6-12 months"}
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(data))
    resolve_liquidity_curve(path)
    out = json.loads(path.read_text())
    assert out["policy"] == data["policy"]
    assert out["policy_curve"] == data["policy_curve"]
