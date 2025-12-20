import json
from datetime import datetime

import update
from update import build_raw_state, write_raw_state


def test_write_raw_state(tmp_path):
    out = tmp_path / "raw_state.json"
    write_raw_state(str(out))
    assert out.exists()
    j = json.loads(out.read_text())
    # top-level keys
    for k in ["meta", "policy", "duration", "volatility", "liquidity"]:
        assert k in j
    # data_health mapping exists
    assert "data_health" in j["meta"]


def test_data_health_rules(monkeypatch):
    # Create a scenario where all policy subfields are FAILED -> policy FAILED
    def failed():
        return {"value": None, "status": "FAILED", "source": None, "fetched_at": "now", "error": "err", "meta": {}}

    monkeypatch.setattr("Data.fetch_policy.fetch_effr", failed)
    monkeypatch.setattr("Data.fetch_policy.fetch_cpi_yoy", failed)
    raw = build_raw_state()
    assert raw["meta"]["data_health"]["policy"] == "FAILED"


def test_write_raw_state_handles_failures(tmp_path, monkeypatch):
    def _boom():
        raise RuntimeError("fail")

    monkeypatch.setattr(update.fetch_vol, "fetch_vix", _boom)
    path = tmp_path / "raw_state.json"
    write_raw_state(str(path))
    assert path.exists()

    data = json.loads(path.read_text())
    assert data["volatility"]["vix"]["status"] == "FAILED"


def test_write_raw_state_includes_generated_at(tmp_path):
    path = tmp_path / "raw_state.json"
    write_raw_state(str(path))

    data = json.loads(path.read_text())
    generated_at = data["meta"]["generated_at"]
    assert isinstance(generated_at, str)
    datetime.fromisoformat(generated_at.replace("Z", "+00:00"))


def test_output_is_pretty_and_stable(tmp_path):
    path = tmp_path / "raw_state.json"
    write_raw_state(str(path))

    content = path.read_text()
    data = json.loads(content)
    expected = json.dumps(data, indent=2, sort_keys=True)
    assert content == expected
