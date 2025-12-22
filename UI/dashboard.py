"""Streamlit dashboard entrypoint."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st

from Analytics.yield_curve_panel import build_yield_curve_panel, load_raw_state


def _load_daily_state(path: Path | str = Path("signals/daily_state.json")) -> dict:
    daily_path = Path(path)
    if not daily_path.exists():
        return {}
    data = json.loads(daily_path.read_text(encoding="utf-8") or "{}")
    return data if isinstance(data, dict) else {}


def _get_block(data: dict, key: str) -> dict:
    block = data.get(key, {})
    return block if isinstance(block, dict) else {}


def _format_value(value) -> str:
    if value is None:
        return "Unavailable"
    return str(value)


def _format_table_rows(rows):
    formatted = []
    for row in rows:
        formatted.append(
            {
                "Tenor": row["tenor"],
                "Start of Year": row["start_of_year"],
                "Last Week": row["last_week"],
                "Current": row["current"],
                "Weekly Change (bps)": row["weekly_change_bps"],
            }
        )
    return formatted


def render_yield_curve_panel(raw_state_path: Path | str = Path("signals/raw_state.json")) -> None:
    st.header("Yield Curve")
    raw_state = load_raw_state(raw_state_path)
    if not raw_state:
        st.info("raw_state.json not found.")
        return

    panel = build_yield_curve_panel(raw_state)
    curve = panel["curve_lines"]
    table_rows = _format_table_rows(panel["table_rows"])

    chart_df = pd.DataFrame(
        {
            "Start of Year": curve["start_of_year"],
            "Last Week": curve["last_week"],
            "Current": curve["current"],
        },
        index=curve["tenors"],
    )
    table_df = pd.DataFrame(table_rows)
    table_df["Weekly Change (bps)"] = pd.to_numeric(
        table_df["Weekly Change (bps)"], errors="coerce"
    ).round(1)
    cols = st.columns([2, 1])
    cols[0].line_chart(chart_df)
    cols[1].dataframe(table_df, use_container_width=True)


def render_policy_liquidity_disagreements(
    daily_state_path: Path | str = Path("signals/daily_state.json"),
) -> None:
    daily_state = _load_daily_state(daily_state_path)
    if not daily_state:
        st.info("daily_state.json not found.")
        return

    st.header("Policy")
    policy = _get_block(daily_state, "policy")
    st.subheader("Spot Policy")
    st.write(f"Stance: {_format_value(policy.get('spot_stance'))}")
    st.write(_format_value(policy.get("explanation")))

    policy_curve = _get_block(daily_state, "policy_curve")
    st.subheader("Policy Curve")
    expected_direction = _format_value(policy_curve.get("expected_direction"))
    horizon = _format_value(policy_curve.get("horizon"))
    st.write(f"Expected Direction: {expected_direction}")
    st.write(f"Horizon: {horizon}")
    st.write(_format_value(policy_curve.get("explanation")))

    st.header("Liquidity")
    liquidity_curve = _get_block(daily_state, "liquidity_curve")
    expected_liquidity = _format_value(liquidity_curve.get("expected_liquidity"))
    liquidity_horizon = _format_value(liquidity_curve.get("horizon"))
    st.write(f"Expected Liquidity: {expected_liquidity}")
    st.write(f"Horizon: {liquidity_horizon}")
    st.write(_format_value(liquidity_curve.get("explanation")))

    st.header("Disagreements")
    disagreements = _get_block(daily_state, "disagreements")
    entries = [
        ("Policy vs Expectations", disagreements.get("policy_vs_expectations", {})),
        ("Policy vs Liquidity", disagreements.get("policy_vs_liquidity", {})),
        ("Expectations vs Liquidity", disagreements.get("expectations_vs_liquidity", {})),
    ]
    for title, entry in entries:
        entry = entry if isinstance(entry, dict) else {}
        st.subheader(title)
        st.write(_format_value(entry.get("explanation")))


def main() -> None:
    render_yield_curve_panel()
    render_policy_liquidity_disagreements()


if __name__ == "__main__":
    main()
