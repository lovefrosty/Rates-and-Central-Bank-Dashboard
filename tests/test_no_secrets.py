import json
import os

import update


def test_fred_api_key_not_serialized(monkeypatch, tmp_path):
    secret = "SHOULD_NOT_LEAK"
    monkeypatch.setenv("FRED_API_KEY", secret)

    def _stub(_fn):
        return {
            "value": 1.0,
            "status": "OK",
            "source": "openbb:fred",
            "fetched_at": "now",
            "error": None,
            "meta": {"current": 1.0},
        }

    monkeypatch.setattr(update, "_safe_call", _stub)
    out = tmp_path / "raw_state.json"
    update.write_raw_state(str(out))
    content = out.read_text(encoding="utf-8")
    assert secret not in content
