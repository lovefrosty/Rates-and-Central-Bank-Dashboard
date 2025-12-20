import json
from analytics.policy import compute_real_policy_rate, compute_policy_gap_bps
from data.fetchers.policy_rates import PolicyRatesFetcher

def resolve_policy_label(real_rate, gap):
    if real_rate > 1.0 and gap > 0:
        return "Restrictive"
    if real_rate < 0:
        return "Accommodative"
    return "Neutral"

def main():
    fetcher = PolicyRatesFetcher()
    data = fetcher.fetch()
    real_rate = compute_real_policy_rate(data['effr'], data['cpi_yoy'])
    gap = compute_policy_gap_bps(real_rate)
    label = resolve_policy_label(real_rate, gap)
    output = {"policy": {"real_policy_rate": real_rate, "policy_gap_bps": gap, "label": label}}
    with open("signals/daily_state.json", "w") as f:
        json.dump(output, f)

if __name__ == "__main__":
    main()
