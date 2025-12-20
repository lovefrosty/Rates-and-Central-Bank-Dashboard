def compute_real_policy_rate(effr, cpi_yoy):
    return effr - cpi_yoy

def compute_policy_gap_bps(real_rate, neutral_rate=0.5):
    return (real_rate - neutral_rate) * 100
