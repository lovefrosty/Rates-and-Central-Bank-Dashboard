"""Streamlit dashboard entrypoint."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
import altair as alt

from Signals import state_paths


def _load_daily_state(path: Path | str = state_paths.DAILY_STATE_PATH) -> dict:
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


def _format_cell(value, decimals=2):
    if value is None:
        return "Unavailable"
    return f"{value:.{decimals}f}"


def render_yield_curve_panel(daily_state_path: Path | str = state_paths.DAILY_STATE_PATH) -> None:
    st.header("Yield Curve")
    daily_state = _load_daily_state(daily_state_path)
    if not daily_state:
        st.info("daily_state.json not found.")
        return

    yield_curve = _get_block(daily_state, "yield_curve")
    if not yield_curve:
        st.info("yield_curve data not available.")
        return

    lines = yield_curve.get("lines", {}) if isinstance(yield_curve, dict) else {}
    tenors = yield_curve.get("tenors", [])
    curve = {
        "tenors": tenors,
        "start_of_year": lines.get("start_of_year", []),
        "last_week": lines.get("last_week", []),
        "current": lines.get("current", []),
    }
    table_rows = _format_table_rows(yield_curve.get("table_rows", []))

    chart_df = pd.DataFrame(
        {
            "Tenor": tenors,
            "Start of Year": curve["start_of_year"],
            "Last Week": curve["last_week"],
            "Current": curve["current"],
        }
    )
    chart_long = chart_df.melt("Tenor", var_name="Snapshot", value_name="Yield")
    chart = (
        alt.Chart(chart_long)
        .mark_line(point=True)
        .encode(
            x=alt.X("Tenor:N", sort=tenors, title="Tenor"),
            y=alt.Y("Yield:Q", title="Yield (%)"),
            color=alt.Color("Snapshot:N", sort=["Start of Year", "Last Week", "Current"]),
            tooltip=["Tenor", "Snapshot", "Yield"],
        )
    )

    rows_by_tenor = {row["Tenor"]: row for row in table_rows}
    table_df = pd.DataFrame(
        {
            tenor: [
                tenor,
                rows_by_tenor.get(tenor, {}).get("Start of Year"),
                rows_by_tenor.get(tenor, {}).get("Last Week"),
                rows_by_tenor.get(tenor, {}).get("Current"),
                rows_by_tenor.get(tenor, {}).get("Weekly Change (bps)"),
            ]
            for tenor in tenors
        },
        index=["Tenor", "Start of Year", "Last Week", "Current", "Weekly Change (bps)"],
    )
    formatted_table = table_df.copy()
    for row_name in formatted_table.index:
        if row_name == "Tenor":
            formatted_table.loc[row_name] = formatted_table.loc[row_name].apply(
                lambda value: value if value is not None else "Unavailable"
            )
            continue
        decimals = 1 if row_name == "Weekly Change (bps)" else 2
        formatted_table.loc[row_name] = formatted_table.loc[row_name].apply(
            lambda value: _format_cell(value, decimals=decimals)
        )

    cols = st.columns([2, 1])
    cols[0].altair_chart(chart, use_container_width=True)
    cols[1].table(formatted_table)


def render_policy_liquidity_disagreements(
    daily_state_path: Path | str = state_paths.DAILY_STATE_PATH,
) -> None:
    daily_state = _load_daily_state(daily_state_path)
    if not daily_state:
        st.info("daily_state.json not found.")
        return

    st.header("Policy")
    policy = _get_block(daily_state, "policy")
    policy_curve = _get_block(daily_state, "policy_curve")

    cols = st.columns(3)
    cols[0].metric("Spot Stance", _format_value(policy.get("spot_stance")))
    cols[1].metric("Expected Direction", _format_value(policy_curve.get("expected_direction")))
    cols[2].metric("Horizon", _format_value(policy_curve.get("horizon")))

    with st.expander("Policy Details"):
        st.write(_format_value(policy.get("explanation")))
        st.write(_format_value(policy_curve.get("explanation")))

    policy_futures = _get_block(daily_state, "policy_futures_curve")
    st.subheader("Policy Futures (ZQ) Implied Curve")
    if policy_futures:
        curve_lines = policy_futures.get("curve_lines", {})
        tenors = curve_lines.get("tenors", [])
        chart_df = pd.DataFrame(
            {
                "Tenor": tenors,
                "Start of Year": curve_lines.get("start_of_year", []),
                "Last Week": curve_lines.get("last_week", []),
                "Current": curve_lines.get("current", []),
            }
        )
        chart_long = chart_df.melt("Tenor", var_name="Snapshot", value_name="Implied Rate")
        chart = (
            alt.Chart(chart_long)
            .mark_line(point=True)
            .encode(
                x=alt.X("Tenor:N", sort=tenors, title="Contract"),
                y=alt.Y("Implied Rate:Q", title="Implied Rate (%)"),
                color=alt.Color("Snapshot:N", sort=["Start of Year", "Last Week", "Current"]),
                tooltip=["Tenor", "Snapshot", "Implied Rate"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

        contracts = policy_futures.get("contracts", [])
        if isinstance(contracts, list) and contracts:
            table_df = pd.DataFrame(
                [
                    {
                        "Contract": row.get("ticker"),
                        "Start of Year (%)": row.get("start_of_year_rate_pct"),
                        "Last Week (%)": row.get("last_week_rate_pct"),
                        "Current (%)": row.get("current_rate_pct"),
                        "Weekly Change (bps)": row.get("weekly_change_bps"),
                    }
                    for row in contracts
                ]
            )
            for col in ["Start of Year (%)", "Last Week (%)", "Current (%)"]:
                table_df[col] = table_df[col].apply(lambda value: _format_cell(value, decimals=2))
            table_df["Weekly Change (bps)"] = table_df["Weekly Change (bps)"].apply(
                lambda value: _format_cell(value, decimals=1)
            )
            st.table(table_df)
    else:
        st.info("Policy futures curve data not available.")

    st.header("Liquidity")
    liquidity_curve = _get_block(daily_state, "liquidity_curve")
    cols = st.columns(2)
    cols[0].metric("Expected Liquidity", _format_value(liquidity_curve.get("expected_liquidity")))
    cols[1].metric("Horizon", _format_value(liquidity_curve.get("horizon")))
    with st.expander("Liquidity Details"):
        st.write(_format_value(liquidity_curve.get("explanation")))

    st.header("Disagreements")
    disagreements = _get_block(daily_state, "disagreements")
    entries = [
        ("Policy vs Expectations", disagreements.get("policy_vs_expectations", {})),
        ("Policy vs Liquidity", disagreements.get("policy_vs_liquidity", {})),
        ("Expectations vs Liquidity", disagreements.get("expectations_vs_liquidity", {})),
    ]
    table_rows = []
    for title, entry in entries:
        entry = entry if isinstance(entry, dict) else {}
        table_rows.append({"Pair": title, "Flag": _format_value(entry.get("flag"))})
    st.table(pd.DataFrame(table_rows))
    with st.expander("Disagreement Details"):
        for title, entry in entries:
            entry = entry if isinstance(entry, dict) else {}
            st.write(f"{title}: {_format_value(entry.get('explanation'))}")


def render_credit_transmission_panel(
    daily_state_path: Path | str = state_paths.DAILY_STATE_PATH,
) -> None:
    st.header("Credit Transmission")
    daily_state = _load_daily_state(daily_state_path)
    if not daily_state:
        st.info("daily_state.json not found.")
        return

    credit = _get_block(daily_state, "credit_transmission")
    if not credit:
        st.info("credit_transmission data not available.")
        return

    weekly_rows = [
        {"Series": "IG OAS", "Weekly Change (bps)": credit.get("ig_oas_weekly_change_bps")},
        {"Series": "HY OAS", "Weekly Change (bps)": credit.get("hy_oas_weekly_change_bps")},
        {"Series": "10Y Yield", "Weekly Change (bps)": credit.get("treasury_10y_weekly_change_bps")},
    ]
    weekly_df = pd.DataFrame(weekly_rows)
    weekly_chart = (
        alt.Chart(weekly_df)
        .mark_bar()
        .encode(
            x=alt.X("Series:N", sort=["IG OAS", "HY OAS", "10Y Yield"]),
            y=alt.Y("Weekly Change (bps):Q", title="Weekly Change (bps)"),
            tooltip=["Series", "Weekly Change (bps)"],
        )
    )

    levels_df = pd.DataFrame(
        [
            {
                "Series": "IG OAS",
                "Current": credit.get("ig_oas_current"),
                "Weekly Change (bps)": credit.get("ig_oas_weekly_change_bps"),
            },
            {
                "Series": "HY OAS",
                "Current": credit.get("hy_oas_current"),
                "Weekly Change (bps)": credit.get("hy_oas_weekly_change_bps"),
            },
            {
                "Series": "10Y Yield",
                "Current": credit.get("treasury_10y_current"),
                "Weekly Change (bps)": credit.get("treasury_10y_weekly_change_bps"),
            },
        ]
    )
    levels_df["Current"] = levels_df["Current"].apply(lambda value: _format_cell(value, decimals=2))
    levels_df["Weekly Change (bps)"] = levels_df["Weekly Change (bps)"].apply(
        lambda value: _format_cell(value, decimals=1)
    )

    cols = st.columns([1, 2])
    cols[0].altair_chart(weekly_chart, use_container_width=True)
    cols[1].table(levels_df)

    st.subheader("Dollar (DXY)")
    dxy = credit.get("dxy", {}) if isinstance(credit, dict) else {}
    candle = dxy.get("candle_1y", {}) if isinstance(dxy, dict) else {}
    open_val = candle.get("open")
    high_val = candle.get("high")
    low_val = candle.get("low")
    close_val = candle.get("close")

    if None not in (open_val, high_val, low_val, close_val):
        candle_df = pd.DataFrame(
            [
                {
                    "Label": "DXY 1Y",
                    "open": open_val,
                    "high": high_val,
                    "low": low_val,
                    "close": close_val,
                }
            ]
        )
        rule = alt.Chart(candle_df).mark_rule().encode(
            x=alt.X("Label:N", title=""),
            y=alt.Y("low:Q", title="DXY"),
            y2="high:Q",
        )
        bar = alt.Chart(candle_df).mark_bar(size=18).encode(
            x=alt.X("Label:N"),
            y=alt.Y("open:Q"),
            y2="close:Q",
            color=alt.condition(
                "datum.close >= datum.open", alt.value("#1f77b4"), alt.value("#d62728")
            ),
            tooltip=["open", "high", "low", "close"],
        )
        st.altair_chart(rule + bar, use_container_width=True)
    else:
        st.info("DXY data not available.")

    changes = dxy.get("changes_pct", {}) if isinstance(dxy, dict) else {}
    dxy_table = pd.DataFrame(
        [
            {
                "Index": "DXY",
                "1D %": changes.get("1d"),
                "5D %": changes.get("5d"),
                "1M %": changes.get("1m"),
                "6M %": changes.get("6m"),
            }
        ]
    )
    for col in ["1D %", "5D %", "1M %", "6M %"]:
        dxy_table[col] = dxy_table[col].apply(lambda value: _format_cell(value, decimals=1))
    st.table(dxy_table)


def render_volatility_panel(daily_state_path: Path | str = state_paths.DAILY_STATE_PATH) -> None:
    st.header("Volatility")
    daily_state = _load_daily_state(daily_state_path)
    if not daily_state:
        st.info("daily_state.json not found.")
        return

    volatility = _get_block(daily_state, "volatility")
    if not volatility:
        st.info("volatility data not available.")
        return

    levels = {
        "VIX": volatility.get("vix"),
        "MOVE": volatility.get("move"),
        "GVZ": volatility.get("gvz"),
        "OVX": volatility.get("ovx"),
    }
    chart_df = pd.DataFrame({"Index": list(levels.keys()), "Level": list(levels.values())})
    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X("Index:N", sort=list(levels.keys())),
            y=alt.Y("Level:Q", title="Level"),
            tooltip=["Index", "Level"],
        )
    )

    changes = volatility.get("changes_pct", {})
    ratios = {
        "VIX": None,
        "MOVE": volatility.get("move_vix_ratio"),
        "GVZ": volatility.get("gvz_vix_ratio"),
        "OVX": volatility.get("ovx_vix_ratio"),
    }
    rows = []
    for key, label in [("vix", "VIX"), ("move", "MOVE"), ("gvz", "GVZ"), ("ovx", "OVX")]:
        entry = changes.get(key, {}) if isinstance(changes, dict) else {}
        rows.append(
            {
                "Index": label,
                "Level": levels.get(label),
                "1D %": entry.get("1d_pct"),
                "5D %": entry.get("5d_pct"),
                "1M %": entry.get("1m_pct"),
                "6M %": entry.get("6m_pct"),
                "Ratio vs VIX": ratios.get(label),
            }
        )

    table_df = pd.DataFrame(rows)
    for col in ["Level", "Ratio vs VIX"]:
        table_df[col] = table_df[col].apply(lambda value: _format_cell(value, decimals=2))
    for col in ["1D %", "5D %", "1M %", "6M %"]:
        table_df[col] = table_df[col].apply(lambda value: _format_cell(value, decimals=1))

    cols = st.columns([1, 2])
    cols[0].altair_chart(chart, use_container_width=True)
    cols[1].table(table_df)

    with st.expander("Why"):
        st.write(_format_value(volatility.get("stress_origin_read")))
        cross = _get_block(daily_state, "vol_credit_cross")
        if cross:
            label = _format_value(cross.get("label"))
            explanation = _format_value(cross.get("explanation"))
            st.write(f"{label}: {explanation}" if explanation != "Unavailable" else label)


def main() -> None:
    render_yield_curve_panel()
    render_volatility_panel()
    render_credit_transmission_panel()
    render_policy_liquidity_disagreements()


if __name__ == "__main__":
    main()
