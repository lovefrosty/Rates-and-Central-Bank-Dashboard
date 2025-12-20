import pytest

from Signals.raw_state_schema import EXPECTED_SECTION_KEYS, validate_raw_state


def _valid_raw_state():
    raw_state = {"meta": {"generated_at": "2024-01-01T00:00:00Z"}}
    for section, keys in EXPECTED_SECTION_KEYS.items():
        raw_state[section] = {}
        for key in keys:
            raw_state[section][key] = {
                "status": "OK",
                "value": 1.0,
                "as_of": "2024-01-01",
                "source": "test",
            }
    return raw_state


def test_missing_top_level_key_fails():
    raw_state = _valid_raw_state()
    raw_state.pop("policy")
    with pytest.raises(ValueError):
        validate_raw_state(raw_state)


def test_unexpected_top_level_key_fails():
    raw_state = _valid_raw_state()
    raw_state["extra"] = {}
    with pytest.raises(ValueError):
        validate_raw_state(raw_state)


def test_missing_ingestion_field_fails():
    raw_state = _valid_raw_state()
    raw_state["policy"]["effr"].pop("source")
    with pytest.raises(ValueError):
        validate_raw_state(raw_state)
