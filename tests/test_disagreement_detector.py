import json

from Signals.resolve_disagreements import resolve_disagreements


def _daily_state(spot=None, expected=None, liquidity=None):
    return {
        "policy": {"spot_stance": spot} if spot is not None else {},
        "policy_curve": {"expected_direction": expected} if expected is not None else {},
        "liquidity_curve": {"expected_liquidity": liquidity} if liquidity is not None else {},
    }


def test_disagreements_block_created(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(spot="Restrictive", expected="Easing", liquidity="Injecting")))
    resolve_disagreements(path)
    data = json.loads(path.read_text())
    assert "disagreements" in data


def test_flags_correctly(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(spot="Restrictive", expected="Easing", liquidity="Draining")))
    resolve_disagreements(path)
    data = json.loads(path.read_text())
    assert data["disagreements"]["policy_vs_expectations"]["flag"] is True
    assert data["disagreements"]["policy_vs_liquidity"]["flag"] is False
    assert data["disagreements"]["expectations_vs_liquidity"]["flag"] is True


def test_missing_inputs(tmp_path):
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(_daily_state(spot="Restrictive")))
    resolve_disagreements(path)
    data = json.loads(path.read_text())
    assert data["disagreements"]["policy_vs_expectations"]["flag"] is False
    assert "incomplete" in data["disagreements"]["policy_vs_expectations"]["explanation"].lower()


def test_blocks_unchanged(tmp_path):
    payload = _daily_state(spot="Restrictive", expected="Tightening", liquidity="Injecting")
    payload["policy"]["explanation"] = "keep"
    path = tmp_path / "daily_state.json"
    path.write_text(json.dumps(payload))
    resolve_disagreements(path)
    data = json.loads(path.read_text())
    assert data["policy"]["explanation"] == "keep"
