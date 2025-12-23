def test_move_vix_ratio():
    raw = {
        "volatility": {
            "vix": {"value": 20, "status": "OK"},
            "move": {"value": 120, "status": "OK"},
        }
    }

    out = build_volatility_witnesses(raw)
    assert out["move_vix_ratio"] == 6
    assert out["data_quality"] == "OK"

def test_partial_data():
    raw = {
        "volatility": {
            "vix": {"value": 20, "status": "OK"},
            "move": {"value": None, "status": "FAILED"},
        }
    }

    out = build_volatility_witnesses(raw)
    assert out["move_vix_ratio"] is None
    assert out["data_quality"] == "PARTIAL"

def test_zero_vix():
    raw = {
        "volatility": {
            "vix": {"value": 0, "status": "OK"},
            "move": {"value": 100, "status": "OK"},
        }
    }

    out = build_volatility_witnesses(raw)
    assert out["move_vix_ratio"] is None
}
