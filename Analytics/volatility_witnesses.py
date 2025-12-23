def build_volatility_witnesses(raw_state: dict) -> dict:
    vol = raw_state.get("volatility", {})

    vix = vol.get("vix", {})
    move = vol.get("move", {})

    vix_val = vix.get("value")
    move_val = move.get("value")

    ratio = None
    if vix_val is not None and move_val is not None and vix_val > 0:
        ratio = move_val / vix_val

    data_quality = "OK"
    if vix.get("status") != "OK" or move.get("status") != "OK":
        data_quality = "PARTIAL"

    return {
        "vix_current": vix_val,
        "move_current": move_val,
        "move_vix_ratio": ratio,
        "data_quality": data_quality,
    }
