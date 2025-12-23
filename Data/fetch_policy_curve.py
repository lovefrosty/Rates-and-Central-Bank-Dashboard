from datetime import datetime

def now():
    return datetime.utcnow().isoformat()

def fetch_policy_curve():
    try:
        return _try_primary()
    except Exception as e:
        return _failed(e)

def _try_primary():
    # TEMP: force failure until source is implemented
    raise NotImplementedError("Policy curve source not implemented yet")

def _failed(e):
    return {
        "value": None,
        "status": "FAILED",
        "source": None,
        "fetched_at": now(),
        "error": str(e),
        "meta": {}
    }
