from data.fetch_policy_curve import fetch_policy_curve

def test_policy_curve_failed_shape():
    out = fetch_policy_curve()
    assert isinstance(out, dict)
    assert "value" in out
    assert "status" in out
    assert out["status"] in {"OK", "FAILED", "FALLBACK"}
