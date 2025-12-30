"""Streamlit dashboard entrypoint."""
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import altair as alt
import pandas as pd
import streamlit as st

from Signals import state_paths


ANCHOR_ORDER: List[Tuple[str, str]] = [
    ("Start of Year", "start_of_year"),
    ("1M", "last_month"),
    ("1W", "last_week"),
    ("Current", "current"),
]
ANCHOR_LABELS = [label for label, _ in ANCHOR_ORDER]

SNAPSHOT_STYLE = {
    "Start of Year": [6, 3],
    "1M": [2, 2],
    "1W": [4, 2],
    "Current": [1, 0],
}
SNAPSHOT_COLORS = {
    "Start of Year": "#56B4E9",
    "1M": "#E69F00",
    "1W": "#CC79A7",
    "Current": "#F0E442",
}
SERIES_COLORS = ["#56B4E9", "#E69F00", "#CC79A7", "#009E73", "#0072B2"]
WINDOW_OPTIONS = [("6m", "6M"), ("1y", "1Y"), ("5y", "5Y")]
ANCHOR_SHAPES = {
    "Start of Year": "diamond",
    "1M": "square",
    "1W": "triangle-up",
    "Current": "circle",
}


st.set_page_config(
    page_title="Macro Strategy Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    .stApp { background-color: #0b0f14; color: #f2f2f2; }
    h1, h2, h3, h4 { color: #f2f2f2; }
    .stDataFrame, .stTable { background-color: #0b0f14; }
</style>
""",
    unsafe_allow_html=True,
)


def _load_daily_state(path: Path | str = state_paths.DAILY_STATE_PATH) -> dict:
    daily_path = Path(path)
    if not daily_path.exists():
        return {}
    data = json.loads(daily_path.read_text(encoding="utf-8") or "{}")
    return data if isinstance(data, dict) else {}


def _load_history_state(path: Path | str = state_paths.HISTORY_STATE_PATH) -> dict:
    history_path = Path(path)
    if not history_path.exists():
        return {}
    data = json.loads(history_path.read_text(encoding="utf-8") or "{}")
    return data if isinstance(data, dict) else {}


def _get_block(data: dict, key: str) -> dict:
    block = data.get(key, {})
    return block if isinstance(block, dict) else {}


def _format_cell(value, decimals: int = 2) -> str:
    if value is None:
        return "Unavailable"
    return f"{value:.{decimals}f}"


def _format_percent(value, decimals: int = 2) -> str:
    if value is None:
        return "Unavailable"
    return f"{value:.{decimals}f}%"


def _format_bps(value, decimals: int = 1) -> str:
    if value is None:
        return "Unavailable"
    return f"{value:.{decimals}f} bps"


def _format_number(value, decimals: int = 2) -> str:
    if value is None:
        return "Unavailable"
    return f"{value:,.{decimals}f}"


def _format_table_rows(rows):
    formatted = []
    for row in rows:
        formatted.append(
            {
                "Tenor": row["tenor"],
                "Start of Year": row.get("start_of_year"),
                "Last Month": row.get("last_month"),
                "Last Week": row.get("last_week"),
                "Current": row.get("current"),
                "Weekly Change (bps)": row.get("weekly_change_bps"),
            }
        )
    return formatted


def _history_rows(history: dict, key: str, window: str, label: str) -> list[dict]:
    series = history.get("series", {}).get(key, {}) if isinstance(history, dict) else {}
    data = series.get(window, [])
    if not isinstance(data, list):
        return []
    rows = []
    for row in data:
        if not isinstance(row, dict):
            continue
        rows.append({"Date": row.get("date"), "Value": row.get("value"), "Series": label})
    return rows


def _history_anchor_rows(history: dict, key: str, label: str) -> list[dict]:
    series = history.get("series", {}).get(key, {}) if isinstance(history, dict) else {}
    anchors = series.get("anchors", {}) if isinstance(series, dict) else {}
    rows = []
    for anchor_label, anchor_key in ANCHOR_ORDER:
        anchor = anchors.get(anchor_key, {})
        if not isinstance(anchor, dict):
            continue
        date = anchor.get("date")
        value = anchor.get("value")
        if date is None or value is None:
            continue
        rows.append({"Date": date, "Value": value, "Series": label, "Anchor": anchor_label})
    return rows


def _history_chart(
    history: dict, window: str, series_map: dict[str, str], title: str
) -> Optional[alt.Chart]:
    rows: list[dict] = []
    anchor_rows: list[dict] = []
    for key, label in series_map.items():
        rows.extend(_history_rows(history, key, window, label))
        anchor_rows.extend(_history_anchor_rows(history, key, label))
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    if df.empty:
        return None
    series_domain = list(series_map.values())
    color = alt.Color(
        "Series:N",
        scale=alt.Scale(domain=series_domain, range=SERIES_COLORS[: len(series_domain)]),
    )
    base = (
        alt.Chart(df)
        .mark_line(interpolate="linear")
        .encode(
            x=alt.X("Date:T", title="Date"),
            y=alt.Y("Value:Q", title="Level"),
            color=color,
            tooltip=["Date", "Series", "Value"],
        )
        .properties(title=title)
    )
    if not anchor_rows:
        return base
    anchor_df = pd.DataFrame(anchor_rows)
    anchor_df["Date"] = pd.to_datetime(anchor_df["Date"], errors="coerce")
    anchor_df = anchor_df.dropna(subset=["Date"])
    if anchor_df.empty:
        return base
    points = (
        alt.Chart(anchor_df)
        .mark_point(size=80, filled=True)
        .encode(
            x=alt.X("Date:T", title="Date"),
            y=alt.Y("Value:Q", title="Level"),
            color=color,
            shape=alt.Shape(
                "Anchor:N",
                sort=ANCHOR_LABELS,
                scale=alt.Scale(domain=ANCHOR_LABELS, range=[ANCHOR_SHAPES[a] for a in ANCHOR_LABELS]),
            ),
            tooltip=["Series", "Anchor", "Date", "Value"],
        )
    )
    labels = (
        alt.Chart(anchor_df)
        .mark_text(dy=-12, size=10)
        .encode(
            x=alt.X("Date:T"),
            y=alt.Y("Value:Q"),
            text="Anchor:N",
            color=color,
        )
    )
    return (base + points + labels).properties(title=title)


def _history_chart_independent(
    history: dict, window: str, series_map: dict[str, str], title: str
) -> Optional[alt.Chart]:
    charts: list[alt.Chart] = []
    anchor_rows: list[dict] = []
    for key, label in series_map.items():
        rows = _history_rows(history, key, window, label)
        if rows:
            df = pd.DataFrame(rows)
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])
            if not df.empty:
                charts.append(
                    alt.Chart(df)
                    .mark_line(interpolate="linear")
                    .encode(
                        x=alt.X("Date:T", title="Date"),
                        y=alt.Y("Value:Q", title="Level"),
                        color=alt.Color("Series:N"),
                        tooltip=["Date", "Series", "Value"],
                    )
                )
        anchor_rows.extend(_history_anchor_rows(history, key, label))
    if not charts:
        return None
    layered = alt.layer(*charts).resolve_scale(y="independent").properties(title=title)
    if not anchor_rows:
        return layered
    anchor_df = pd.DataFrame(anchor_rows)
    anchor_df["Date"] = pd.to_datetime(anchor_df["Date"], errors="coerce")
    anchor_df = anchor_df.dropna(subset=["Date"])
    if anchor_df.empty:
        return layered
    points = (
        alt.Chart(anchor_df)
        .mark_point(size=80, filled=True)
        .encode(
            x=alt.X("Date:T", title="Date"),
            y=alt.Y("Value:Q", title="Level"),
            color=alt.Color("Series:N"),
            shape=alt.Shape(
                "Anchor:N",
                sort=ANCHOR_LABELS,
                scale=alt.Scale(domain=ANCHOR_LABELS, range=[ANCHOR_SHAPES[a] for a in ANCHOR_LABELS]),
            ),
            tooltip=["Series", "Anchor", "Date", "Value"],
        )
    )
    labels = (
        alt.Chart(anchor_df)
        .mark_text(dy=-12, size=10)
        .encode(
            x=alt.X("Date:T"),
            y=alt.Y("Value:Q"),
            text="Anchor:N",
            color=alt.Color("Series:N"),
        )
    )
    return (layered + points + labels).properties(title=title)


def _vol_transform_rows(
    history: dict,
    window: str,
    series_key: str,
    transform_key: str,
    label: str,
) -> list[dict]:
    transforms = history.get("volatility_transforms", {}) if isinstance(history, dict) else {}
    window_block = transforms.get("windows", {}).get(window, {}) if isinstance(transforms, dict) else {}
    series_block = window_block.get(series_key, {}) if isinstance(window_block, dict) else {}
    data = series_block.get(transform_key, [])
    if not isinstance(data, list):
        return []
    rows = []
    for row in data:
        if not isinstance(row, dict):
            continue
        rows.append({"Date": row.get("date"), "Value": row.get("value"), "Series": label})
    return rows


def _vol_simple_rows(history: dict, window: str, key: str, label: str) -> list[dict]:
    transforms = history.get("volatility_transforms", {}) if isinstance(history, dict) else {}
    window_block = transforms.get("windows", {}).get(window, {}) if isinstance(transforms, dict) else {}
    data = window_block.get(key, [])
    if not isinstance(data, list):
        return []
    rows = []
    for row in data:
        if not isinstance(row, dict):
            continue
        rows.append({"Date": row.get("date"), "Value": row.get("value"), "Series": label})
    return rows


def _select_window(label: str, key: str) -> str:
    options = [opt for opt, _ in WINDOW_OPTIONS]
    labels = {opt: display for opt, display in WINDOW_OPTIONS}
    return st.selectbox(label, options, format_func=lambda opt: labels.get(opt, opt), key=key)


def _collect_values(values: Iterable[Optional[float]]) -> List[float]:
    return [float(v) for v in values if v is not None]


def _calc_domain(values: Iterable[Optional[float]]) -> Optional[List[float]]:
    numeric = _collect_values(values)
    if not numeric:
        return None
    min_val = min(numeric)
    max_val = max(numeric)
    if min_val == max_val:
        pad = 0.5 if min_val else 1.0
    else:
        pad = (max_val - min_val) * 0.08
    return [min_val - pad, max_val + pad]


def _y_scale(domain: Optional[List[float]]) -> alt.Scale:
    if domain is None:
        return alt.Scale()
    return alt.Scale(domain=domain)


def _anchor_rows(series_label: str, anchors: Dict[str, Optional[float]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for label, key in ANCHOR_ORDER:
        rows.append({"Anchor": label, "Series": series_label, "Value": anchors.get(key)})
    return rows


def _anchor_chart(
    rows: List[Dict[str, Any]],
    y_title: str,
    domain: Optional[List[float]] = None,
    series_domain: Optional[List[str]] = None,
) -> alt.Chart:
    df = pd.DataFrame(rows)
    if series_domain:
        color = alt.Color(
            "Series:N",
            sort=series_domain,
            scale=alt.Scale(domain=series_domain, range=SERIES_COLORS),
        )
    else:
        color = alt.Color("Series:N")
    chart = (
        alt.Chart(df)
        .mark_line(point=True, interpolate="linear")
        .encode(
            x=alt.X("Anchor:N", sort=ANCHOR_LABELS, title=""),
            y=alt.Y("Value:Q", title=y_title, scale=_y_scale(domain)),
            color=color,
            tooltip=["Series", "Anchor", "Value"],
        )
    )
    return chart


def _snapshot_curve_chart(tenors: List[str], lines: Dict[str, List[Optional[float]]]) -> alt.Chart:
    rows = []
    for label, key in ANCHOR_ORDER:
        series = lines.get(key, [])
        for tenor, value in zip(tenors, series):
            rows.append({"Tenor": tenor, "Snapshot": label, "Yield": value})
    df = pd.DataFrame(rows)
    domain = _calc_domain(df["Yield"].tolist())
    chart = (
        alt.Chart(df)
        .mark_line(point=True, interpolate="linear")
        .encode(
            x=alt.X("Tenor:N", sort=tenors, title="Tenor"),
            y=alt.Y("Yield:Q", title="Yield (%)", scale=_y_scale(domain)),
            color=alt.Color(
                "Snapshot:N",
                sort=ANCHOR_LABELS,
                scale=alt.Scale(domain=ANCHOR_LABELS, range=[SNAPSHOT_COLORS[s] for s in ANCHOR_LABELS]),
            ),
            strokeDash=alt.StrokeDash(
                "Snapshot:N",
                sort=ANCHOR_LABELS,
                scale=alt.Scale(domain=ANCHOR_LABELS, range=[SNAPSHOT_STYLE[s] for s in ANCHOR_LABELS]),
            ),
            tooltip=["Tenor", "Snapshot", "Yield"],
        )
    )
    return chart


def render_yield_curve_panel(daily_state: Dict[str, Any]) -> None:
    st.header("Yield Curve")
    yield_curve = _get_block(daily_state, "yield_curve")
    if not yield_curve:
        st.info("yield_curve data not available.")
        return

    tenors = yield_curve.get("tenors", [])
    lines = yield_curve.get("lines", {}) if isinstance(yield_curve, dict) else {}
    chart = _snapshot_curve_chart(tenors, lines)

    table = pd.DataFrame(index=ANCHOR_LABELS, columns=tenors)
    for idx, (label, key) in enumerate(ANCHOR_ORDER):
        values = lines.get(key, [])
        for tenor, value in zip(tenors, values):
            table.loc[label, tenor] = _format_percent(value, decimals=2)

    cols = st.columns([2, 1])
    cols[0].altair_chart(chart, width="stretch")
    cols[0].caption("Anchor points only. Axis is data-range (not zero-based).")
    cols[1].dataframe(table, width="stretch")


def render_real_rates_panel(daily_state: Dict[str, Any]) -> None:
    st.subheader("Real Yields & Breakevens")
    infl = _get_block(daily_state, "inflation_real_rates")
    if not infl:
        st.info("inflation_real_rates data not available.")
        return

    real_anchors = infl.get("real_10y_anchors", {})
    breakeven_anchors = infl.get("breakeven_10y_anchors", {})

    chart_rows = _anchor_rows("Real 10Y", real_anchors) + _anchor_rows("10Y Breakeven", breakeven_anchors)
    domain = _calc_domain([row["Value"] for row in chart_rows])
    chart = _anchor_chart(chart_rows, "Yield (%)", domain=domain, series_domain=["Real 10Y", "10Y Breakeven"])

    cols = st.columns(2)
    cols[0].altair_chart(chart, width="stretch")
    cols[0].caption("Anchor points only. Axis is data-range (not zero-based).")

    real_rows = infl.get("real_yields", []) if isinstance(infl.get("real_yields"), list) else []
    breakeven_rows = infl.get("breakevens", []) if isinstance(infl.get("breakevens"), list) else []

    def _table_from_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
        table = []
        for row in rows:
            table.append(
                {
                    "Tenor": row.get("tenor"),
                    "Start of Year": _format_percent(row.get("start_of_year"), decimals=2),
                    "1M": _format_percent(row.get("last_month"), decimals=2),
                    "1W": _format_percent(row.get("last_week"), decimals=2),
                    "Current": _format_percent(row.get("current"), decimals=2),
                }
            )
        return pd.DataFrame(table)

    cols[1].dataframe(_table_from_rows(real_rows), width="stretch")
    st.dataframe(_table_from_rows(breakeven_rows), width="stretch")

    driver = infl.get("driver_read")
    if isinstance(driver, str):
        st.caption(f"Evidence read: {driver}.")

    history = _load_history_state()
    if history:
        window = _select_window("Real Rates History Window", key="real_rates_history_window")
        chart = _history_chart(
            history,
            window,
            {"real_10y": "Real 10Y", "breakeven_10y": "10Y Breakeven"},
            "Real Yields & Breakevens History",
        )
        if chart is not None:
            st.altair_chart(chart, width="stretch")
        else:
            st.info("Real rates history not available.")


def render_policy_futures_panel(daily_state: Dict[str, Any]) -> None:
    st.header("Policy Futures")
    futures = _get_block(daily_state, "policy_futures_curve")
    if not futures:
        st.info("policy_futures_curve data not available.")
        return

    curve_lines = futures.get("curve_lines", {}) if isinstance(futures, dict) else {}
    tenors = curve_lines.get("tenors", [])
    labels = curve_lines.get("labels", tenors)
    display_labels = []
    for tenor, label in zip(tenors, labels):
        display = f"{label}\n{tenor}" if label else tenor
        display_labels.append(display)

    rows = []
    for label, key in ANCHOR_ORDER:
        series = curve_lines.get(key, [])
        for display, value in zip(display_labels, series):
            rows.append({"Contract": display, "Snapshot": label, "Implied Rate": value})
    df = pd.DataFrame(rows)
    domain = _calc_domain(df["Implied Rate"].tolist())
    curve_chart = (
        alt.Chart(df)
        .mark_line(point=True, interpolate="linear")
        .encode(
            x=alt.X("Contract:N", sort=display_labels, title="Contract"),
            y=alt.Y("Implied Rate:Q", title="Implied Rate (%)", scale=alt.Scale(domain=domain)),
            color=alt.Color(
                "Snapshot:N",
                sort=ANCHOR_LABELS,
                scale=alt.Scale(domain=ANCHOR_LABELS, range=[SNAPSHOT_COLORS[s] for s in ANCHOR_LABELS]),
            ),
            strokeDash=alt.StrokeDash(
                "Snapshot:N",
                sort=ANCHOR_LABELS,
                scale=alt.Scale(domain=ANCHOR_LABELS, range=[SNAPSHOT_STYLE[s] for s in ANCHOR_LABELS]),
            ),
            tooltip=["Contract", "Snapshot", "Implied Rate"],
        )
    )

    change_vs_now = curve_lines.get("change_vs_now_bps", [])
    change_df = pd.DataFrame({"Contract": display_labels, "Change vs Now (bps)": change_vs_now})
    change_chart = (
        alt.Chart(change_df)
        .mark_line(point=True, interpolate="linear")
        .encode(
            x=alt.X("Contract:N", sort=display_labels, title="Contract"),
            y=alt.Y("Change vs Now (bps):Q", title="Cumulative Cuts vs Now (bps)"),
            tooltip=["Contract", "Change vs Now (bps)"],
        )
    )

    cols = st.columns(2)
    cols[0].altair_chart(curve_chart, width="stretch")
    cols[0].caption("Anchor points only. Axis is data-range (not zero-based).")
    cols[1].altair_chart(change_chart, width="stretch")

    contracts = futures.get("contracts", []) if isinstance(futures.get("contracts"), list) else []
    table_cols = {}
    for idx, contract in enumerate(contracts):
        label = contract.get("label") or tenors[idx] if idx < len(tenors) else contract.get("ticker")
        header = f"{label}\n{contract.get('ticker', '')}"
        status = contract.get("status")
        def _value_or_failed(value, fmt):
            if status == "FAILED":
                return "FAILED"
            return fmt(value)
        table_cols[header] = [
            _value_or_failed(contract.get("current_price"), lambda v: _format_number(v, decimals=2)),
            _value_or_failed(contract.get("current_rate_pct"), lambda v: _format_percent(v, decimals=2)),
            _value_or_failed(contract.get("change_vs_now_bps"), lambda v: _format_bps(v, decimals=1)),
        ]

    table_df = pd.DataFrame(table_cols, index=["Price", "Implied Rate (%)", "Change vs Now (bps)"])
    st.dataframe(table_df, width="stretch")


def render_volatility_panel(daily_state: Dict[str, Any]) -> None:
    st.header("Volatility")
    volatility = _get_block(daily_state, "volatility")
    if not volatility:
        st.info("volatility data not available.")
        return

    changes = volatility.get("changes_pct", {}) if isinstance(volatility.get("changes_pct"), dict) else {}
    rows = []
    for key, label in [("vix", "VIX"), ("move", "MOVE"), ("gvz", "GVZ"), ("ovx", "OVX")]:
        entry = changes.get(key, {}) if isinstance(changes, dict) else {}
        rows.append(
            {
                "Index": label,
                "Current": _format_number(volatility.get(key), decimals=2),
                "1D %": _format_percent(entry.get("1d_pct"), decimals=1),
                "5D %": _format_percent(entry.get("5d_pct"), decimals=1),
                "1M %": _format_percent(entry.get("1m_pct"), decimals=1),
                "6M %": _format_percent(entry.get("6m_pct"), decimals=1),
                "MOVE/VIX": _format_number(volatility.get("move_vix_ratio"), decimals=2)
                if label == "MOVE"
                else "",
                "GVZ/VIX": _format_number(volatility.get("gvz_vix_ratio"), decimals=2)
                if label == "GVZ"
                else "",
                "OVX/VIX": _format_number(volatility.get("ovx_vix_ratio"), decimals=2)
                if label == "OVX"
                else "",
            }
        )

    st.dataframe(pd.DataFrame(rows), width="stretch")

    history = _load_history_state()
    if history:
        window = _select_window("Volatility History Window", key="vol_history_window")

        raw_chart = _history_chart(
            history,
            window,
            {"vix": "VIX", "move": "MOVE"},
            "Volatility Levels (Raw)",
        )
        if raw_chart is not None:
            ma_rows = []
            for series_key, label in [("vix", "VIX"), ("move", "MOVE")]:
                ma_rows += _vol_transform_rows(history, window, series_key, "mean_50", f"{label} MA 50")
                ma_rows += _vol_transform_rows(history, window, series_key, "mean_200", f"{label} MA 200")
            ma_df = pd.DataFrame(ma_rows)
            if not ma_df.empty:
                ma_df["Date"] = pd.to_datetime(ma_df["Date"], errors="coerce")
                ma_df = ma_df.dropna(subset=["Date"])
                ma_chart = (
                    alt.Chart(ma_df)
                    .mark_line(interpolate="linear", strokeDash=[4, 3])
                    .encode(
                        x=alt.X("Date:T", title="Date"),
                        y=alt.Y("Value:Q", title="Level"),
                        color=alt.Color("Series:N"),
                        tooltip=["Date", "Series", "Value"],
                    )
                )
                st.altair_chart(raw_chart + ma_chart, width="stretch")
            else:
                st.altair_chart(raw_chart, width="stretch")
        else:
            st.info("Volatility history not available.")

        z_rows = []
        z_rows += _vol_transform_rows(history, window, "vix", "zscore_200", "VIX Z (200d)")
        z_rows += _vol_transform_rows(history, window, "move", "zscore_200", "MOVE Z (200d)")
        z_df = pd.DataFrame(z_rows)
        if not z_df.empty:
            z_df["Date"] = pd.to_datetime(z_df["Date"], errors="coerce")
            z_df = z_df.dropna(subset=["Date"])
            zero_line = alt.Chart(pd.DataFrame({"Value": [0]})).mark_rule(color="#9aa0a6").encode(y="Value:Q")
            z_chart = (
                alt.Chart(z_df)
                .mark_line(interpolate="linear")
                .encode(
                    x=alt.X("Date:T", title="Date"),
                    y=alt.Y("Value:Q", title="Z-score (200d)"),
                    color=alt.Color("Series:N"),
                    tooltip=["Date", "Series", "Value"],
                )
                .properties(title="Volatility Standardized (Z-score)")
            )
            st.altair_chart(z_chart + zero_line, width="stretch")

        ratio_rows = _vol_simple_rows(history, window, "move_vix_ratio", "MOVE/VIX Ratio")
        ratio_df = pd.DataFrame(ratio_rows)
        if not ratio_df.empty:
            ratio_df["Date"] = pd.to_datetime(ratio_df["Date"], errors="coerce")
            ratio_df = ratio_df.dropna(subset=["Date"])
            ratio_chart = (
                alt.Chart(ratio_df)
                .mark_line(interpolate="linear")
                .encode(
                    x=alt.X("Date:T", title="Date"),
                    y=alt.Y("Value:Q", title="MOVE/VIX Ratio"),
                    color=alt.Color("Series:N"),
                    tooltip=["Date", "Series", "Value"],
                )
                .properties(title="Rates vs Equity Volatility")
            )
            st.altair_chart(ratio_chart, width="stretch")

        spread_rows = _vol_simple_rows(history, window, "move_vix_z_spread", "MOVE Z - VIX Z")
        spread_df = pd.DataFrame(spread_rows)
        if not spread_df.empty:
            spread_df["Date"] = pd.to_datetime(spread_df["Date"], errors="coerce")
            spread_df = spread_df.dropna(subset=["Date"])
            zero_line = alt.Chart(pd.DataFrame({"Value": [0]})).mark_rule(color="#9aa0a6").encode(y="Value:Q")
            spread_chart = (
                alt.Chart(spread_df)
                .mark_line(interpolate="linear")
                .encode(
                    x=alt.X("Date:T", title="Date"),
                    y=alt.Y("Value:Q", title="Z-score Spread"),
                    color=alt.Color("Series:N"),
                    tooltip=["Date", "Series", "Value"],
                )
                .properties(title="Volatility Z-Score Spread (MOVE - VIX)")
            )
            st.altair_chart(spread_chart + zero_line, width="stretch")


def render_liquidity_panel(daily_state: Dict[str, Any]) -> None:
    st.header("Liquidity Evidence")
    liquidity = _get_block(daily_state, "liquidity_analytics")
    if not liquidity:
        st.info("liquidity_analytics data not available.")
        return

    table_rows = []
    for label, entry in {
        "RRP": liquidity.get("rrp", {}),
        "TGA": liquidity.get("tga", {}),
        "WALCL": liquidity.get("walcl", {}),
    }.items():
        table_rows.append(
            {
                "Series": label,
                "Level": _format_number(entry.get("level"), decimals=2),
                "Δ1W": _format_number(entry.get("change_1w"), decimals=2),
                "Δ1M": _format_number(entry.get("change_1m"), decimals=2),
                "ΔSOY": _format_number(entry.get("change_ytd"), decimals=2),
            }
        )
    st.dataframe(pd.DataFrame(table_rows), width="stretch")

    history = _load_history_state()
    if history:
        window = _select_window("Liquidity History Window", key="liq_history_window")
        chart = _history_chart(
            history,
            window,
            {"rrp": "RRP", "tga": "TGA", "walcl": "WALCL"},
            "Liquidity History",
        )
        if chart is not None:
            st.altair_chart(chart, width="stretch")
        else:
            st.info("Liquidity history not available.")


def render_labor_panel(daily_state: Dict[str, Any]) -> None:
    st.header("Labor Market")
    labor = _get_block(daily_state, "labor_market")
    if not labor:
        st.info("labor_market data not available.")
        return

    history = _load_history_state()
    if history:
        window = _select_window("Labor History Window", key="labor_history_window")
        cols = st.columns(3)
        for idx, (key, label) in enumerate(
            [("unrate", "Unemployment"), ("jolts_openings", "JOLTS"), ("eci", "ECI")]
        ):
            chart = _history_chart(history, window, {key: label}, f"{label} History")
            if chart is not None:
                cols[idx].altair_chart(chart, width="stretch")
            else:
                cols[idx].info(f"{label} history not available.")
    else:
        st.info("Labor history not available.")

    table = [
        {"Series": "Unemployment", "Current": _format_number(labor.get("unrate_current"), decimals=2)},
        {"Series": "JOLTS Openings", "Current": _format_number(labor.get("jolts_openings_current"), decimals=2)},
        {"Series": "ECI", "Current": _format_number(labor.get("eci_index_current"), decimals=2)},
    ]
    st.dataframe(pd.DataFrame(table), width="stretch")


def render_fx_panel(daily_state: Dict[str, Any]) -> None:
    st.header("FX Conditions")
    fx = _get_block(daily_state, "fx")
    if not fx:
        st.info("fx data not available.")
        return

    history = _load_history_state()
    if history:
        window = _select_window("FX History Window", key="fx_history_window")
        cols = st.columns(2)
        dxy_chart = _history_chart(history, window, {"dxy": "DXY"}, "DXY History")
        if dxy_chart is not None:
            cols[0].altair_chart(dxy_chart, width="stretch")
        else:
            cols[0].info("DXY history not available.")

        pairs_chart = _history_chart_independent(
            history,
            window,
            {"eurusd": "EURUSD", "gbpusd": "GBPUSD", "usdcad": "USDCAD"},
            "Major Pairs Overlay",
        )
        if pairs_chart is not None:
            cols[1].altair_chart(pairs_chart, width="stretch")
            cols[1].caption("Independent y-scales. Compare direction, not level.")
        else:
            cols[1].info("FX pair history not available.")

        usdjpy_chart = _history_chart(history, window, {"usdjpy": "USDJPY"}, "USDJPY Focus")
        if usdjpy_chart is not None:
            st.altair_chart(usdjpy_chart, width="stretch")
        else:
            st.info("USDJPY history not available.")
    else:
        st.info("FX history not available.")


def render_credit_panel(daily_state: Dict[str, Any]) -> None:
    st.header("Credit Detail")
    credit = _get_block(daily_state, "credit_transmission")
    if not credit:
        st.info("credit_transmission data not available.")
        return

    rows = [
        {
            "Series": "IG OAS",
            "Current": _format_number(credit.get("ig_oas_current"), decimals=2),
            "Weekly Change (bps)": _format_bps(credit.get("ig_oas_weekly_change_bps"), decimals=1),
        },
        {
            "Series": "HY OAS",
            "Current": _format_number(credit.get("hy_oas_current"), decimals=2),
            "Weekly Change (bps)": _format_bps(credit.get("hy_oas_weekly_change_bps"), decimals=1),
        },
        {
            "Series": "10Y Treasury",
            "Current": _format_percent(credit.get("treasury_10y_current"), decimals=2),
            "Weekly Change (bps)": _format_bps(credit.get("treasury_10y_weekly_change_bps"), decimals=1),
        },
    ]
    st.dataframe(pd.DataFrame(rows), width="stretch")


def render_cross_signals(daily_state: Dict[str, Any]) -> None:
    st.header("Cross-Signals")
    policy = _get_block(daily_state, "policy")
    policy_curve = _get_block(daily_state, "policy_curve")
    liquidity_curve = _get_block(daily_state, "liquidity_curve")
    disagreements = _get_block(daily_state, "disagreements")
    vol_cross = _get_block(daily_state, "vol_credit_cross")

    cols = st.columns(3)
    cols[0].metric("Spot Stance", policy.get("spot_stance", "Unavailable"))
    cols[1].metric("Expected Direction", policy_curve.get("expected_direction", "Unavailable"))
    cols[2].metric("Liquidity", liquidity_curve.get("expected_liquidity", "Unavailable"))

    with st.expander("Explanations"):
        st.write(policy.get("explanation", "Unavailable"))
        st.write(policy_curve.get("explanation", "Unavailable"))
        st.write(liquidity_curve.get("explanation", "Unavailable"))

    disagreement_rows = []
    for label, key in [
        ("Policy vs Expectations", "policy_vs_expectations"),
        ("Policy vs Liquidity", "policy_vs_liquidity"),
        ("Expectations vs Liquidity", "expectations_vs_liquidity"),
    ]:
        entry = disagreements.get(key, {}) if isinstance(disagreements, dict) else {}
        disagreement_rows.append(
            {
                "Pair": label,
                "Flag": entry.get("flag", "Unavailable"),
                "Explanation": entry.get("explanation", "Unavailable"),
            }
        )
    st.dataframe(pd.DataFrame(disagreement_rows), width="stretch")

    if vol_cross:
        st.subheader("Volatility vs Credit")
        st.write(f"{vol_cross.get('label', 'Unavailable')}: {vol_cross.get('explanation', 'Unavailable')}")


def render_system_health(daily_state: Dict[str, Any]) -> None:
    st.header("System Health & Metadata")
    health = _get_block(daily_state, "system_health")
    if not health:
        st.info("system_health data not available.")
        return

    rows = []
    blocks = health.get("blocks", {}) if isinstance(health.get("blocks"), dict) else {}
    for label, entry in blocks.items():
        status = entry.get("status")
        icon = "✅" if status == "OK" else "⚠️"
        rows.append(
            {
                "Block": label,
                "Status": f"{icon} {status}",
                "Failed": entry.get("failed"),
                "Total": entry.get("total"),
            }
        )

    st.dataframe(pd.DataFrame(rows), width="stretch")
    st.write(f"Last updated: {health.get('generated_at', 'Unavailable')}")
    st.write(f"Age: {health.get('age_human', 'Unavailable')}")
    st.write(f"Failed series: {health.get('failed_series', 'Unavailable')} / {health.get('total_series', 'Unavailable')}")


def render_sidebar_reasoning() -> None:
    st.sidebar.divider()
    st.sidebar.subheader("Reasoning Guide")
    with st.sidebar.expander("Rates Drivers", expanded=True):
        st.markdown(
            """
- Is the move in 10Y driven by real rates or breakevens?
- Are curve changes led by the front end or the long end?
- Are real yields and breakevens moving together or diverging?
"""
        )
    with st.sidebar.expander("Policy vs Liquidity"):
        st.markdown(
            """
- Does the policy path align with the liquidity read?
- Are futures implying a path different from spot stance?
- Are liquidity balances reinforcing or offsetting policy stance?
"""
        )
    with st.sidebar.expander("Vol & Credit"):
        st.markdown(
            """
- Is MOVE diverging from VIX?
- Are credit spreads widening alongside volatility?
- Is FX strength coinciding with higher rates volatility?
"""
        )


def main() -> None:
    daily_state = _load_daily_state()
    if not daily_state:
        st.info("daily_state.json not found.")
        return

    st.title("Macro Strategy Dashboard")
    st.markdown("Decision Support System | _Snapshot Mode_")

    render_sidebar_reasoning()

    tab_rates, tab_policy, tab_risk, tab_health = st.tabs(
        ["📈 Rates & Inflation", "🏦 Policy & Liquidity", "⚡ Volatility & Risk", "🛠 System Health"]
    )

    with tab_rates:
        render_yield_curve_panel(daily_state)
        render_real_rates_panel(daily_state)
        render_labor_panel(daily_state)

    with tab_policy:
        render_policy_futures_panel(daily_state)
        render_liquidity_panel(daily_state)
        render_cross_signals(daily_state)

    with tab_risk:
        render_volatility_panel(daily_state)
        render_credit_panel(daily_state)
        render_fx_panel(daily_state)

    with tab_health:
        render_system_health(daily_state)
        with st.expander("Raw JSON"):
            st.json(daily_state)


if __name__ == "__main__":
    main()
