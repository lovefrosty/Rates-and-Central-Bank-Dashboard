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
    ("6M", "last_6m"),
    ("1M", "last_month"),
    ("1W", "last_week"),
    ("Current", "current"),
]
ANCHOR_LABELS = [label for label, _ in ANCHOR_ORDER]

SNAPSHOT_STYLE = {
    "Start of Year": [6, 3],
    "6M": [5, 2],
    "1M": [2, 2],
    "1W": [4, 2],
    "Current": [1, 0],
}
SNAPSHOT_COLORS = {
    "Start of Year": "#56B4E9",
    "6M": "#0072B2",
    "1M": "#E69F00",
    "1W": "#CC79A7",
    "Current": "#F0E442",
}
SERIES_COLORS = ["#56B4E9", "#E69F00", "#CC79A7", "#009E73", "#0072B2"]
WINDOW_OPTIONS = [("1y", "1Y"), ("3y", "3Y"), ("5y", "5Y")]
WINDOW_DAYS = {"1y": 365, "3y": 1095, "5y": 1825}
ANCHOR_SHAPES = {
    "Start of Year": "diamond",
    "6M": "triangle-down",
    "1M": "square",
    "1W": "triangle-up",
    "Current": "circle",
}

MISSING_DISPLAY = "—"



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
        return MISSING_DISPLAY
    return f"{value:.{decimals}f}"


def _format_percent(value, decimals: int = 2) -> str:
    if value is None:
        return MISSING_DISPLAY
    return f"{value:.{decimals}f}%"


def _format_bps(value, decimals: int = 1) -> str:
    if value is None:
        return MISSING_DISPLAY
    return f"{value:.{decimals}f} bps"


def _format_number(value, decimals: int = 2) -> str:
    if value is None:
        return MISSING_DISPLAY
    return f"{value:,.{decimals}f}"


def _percent_formatter(decimals: int = 2):
    return lambda value: _format_percent(value, decimals=decimals)


def _bps_formatter(decimals: int = 1):
    return lambda value: _format_bps(value, decimals=decimals)


def _number_formatter(decimals: int = 2):
    return lambda value: _format_number(value, decimals=decimals)


def _style_magnitude(df: pd.DataFrame, columns: List[str]):
    styler = df.style
    if not columns:
        return styler
    numeric = df[columns].apply(pd.to_numeric, errors="coerce")
    max_val = numeric.abs().max().max()
    if max_val is None or pd.isna(max_val) or max_val == 0:
        return styler

    def _colorize(value):
        if value is None or pd.isna(value):
            return ""
        intensity = min(abs(float(value)) / max_val, 1.0)
        alpha = 0.08 + (0.5 * intensity)
        return f"background-color: rgba(86, 180, 233, {alpha:.2f});"

    return styler.applymap(_colorize, subset=columns)


def _apply_window(rows: list[dict], window: str) -> list[dict]:
    days = WINDOW_DAYS.get(window)
    if not rows or days is None:
        return rows
    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    if df.empty:
        return []
    last_date = df["Date"].max()
    cutoff = last_date - pd.Timedelta(days=days)
    df = df[df["Date"] >= cutoff]
    df["Date"] = df["Date"].dt.date.astype(str)
    return df.to_dict("records")


def _history_rows(history: dict, key: str, window: str, label: str) -> list[dict]:
    series = history.get("series", {}).get(key, {}) if isinstance(history, dict) else {}
    dates = series.get("dates", [])
    values = series.get("values", [])
    if not isinstance(dates, list) or not isinstance(values, list):
        return []
    rows = [{"Date": dt, "Value": val, "Series": label} for dt, val in zip(dates, values)]
    return _apply_window(rows, window)


def _history_chart(
    history: dict, window: str, series_map: dict[str, str], title: str
) -> Optional[alt.Chart]:
    rows: list[dict] = []
    for key, label in series_map.items():
        rows.extend(_history_rows(history, key, window, label))
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
    return base


def _history_chart_independent(
    history: dict, window: str, series_map: dict[str, str], title: str
) -> Optional[alt.Chart]:
    charts: list[alt.Chart] = []
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
    if not charts:
        return None
    layered = alt.layer(*charts).resolve_scale(y="independent").properties(title=title)
    return layered


def _transform_rows(history: dict, series_key: str, transform_key: str, window: str, label: str) -> list[dict]:
    transforms = history.get("transforms", {}) if isinstance(history, dict) else {}
    series_block = transforms.get(series_key, {}) if isinstance(transforms, dict) else {}
    block = series_block.get(transform_key, {})
    dates = block.get("dates", []) if isinstance(block, dict) else []
    values = block.get("values", []) if isinstance(block, dict) else []
    if not isinstance(dates, list) or not isinstance(values, list):
        return []
    rows = [{"Date": dt, "Value": val, "Series": label} for dt, val in zip(dates, values)]
    return _apply_window(rows, window)


def _cross_asset_rows(history: dict, key: str, window: str, label: str) -> list[dict]:
    cross_asset = history.get("cross_asset", {}) if isinstance(history, dict) else {}
    block = cross_asset.get(key, {}) if isinstance(cross_asset, dict) else {}
    dates = block.get("dates", []) if isinstance(block, dict) else []
    values = block.get("values", []) if isinstance(block, dict) else []
    if not isinstance(dates, list) or not isinstance(values, list):
        return []
    rows = [{"Date": dt, "Value": val, "Series": label} for dt, val in zip(dates, values)]
    return _apply_window(rows, window)


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


def _matrix_rows(matrix: Dict[str, Any]) -> List[Dict[str, Any]]:
    currencies = matrix.get("currencies", []) if isinstance(matrix, dict) else []
    values = matrix.get("values_pct", []) if isinstance(matrix, dict) else []
    rows: List[Dict[str, Any]] = []
    for i, base in enumerate(currencies):
        row_vals = values[i] if i < len(values) and isinstance(values[i], list) else []
        for j, quote in enumerate(currencies):
            value = row_vals[j] if j < len(row_vals) else None
            rows.append({"Base": base, "Quote": quote, "Value": value})
    return rows


def _snapshot_curve_chart(
    tenors: List[str],
    lines: Dict[str, List[Optional[float]]],
    y_title: str = "Yield (%)",
) -> alt.Chart:
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
            y=alt.Y("Yield:Q", title=y_title, scale=_y_scale(domain)),
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

    cols = st.columns([2, 1])
    cols[0].altair_chart(chart, width="stretch")
    cols[0].caption("Anchor points only. Axis is data-range (not zero-based).")
    rows = yield_curve.get("table_rows", []) if isinstance(yield_curve.get("table_rows"), list) else []
    table_df = pd.DataFrame(
        [
            {
                "Tenor": row.get("tenor"),
                "Start of Year": row.get("start_of_year"),
                "6M": row.get("last_6m"),
                "1M": row.get("last_month"),
                "1W": row.get("last_week"),
                "Current": row.get("current"),
                "Δ1W (bps)": row.get("weekly_change_bps"),
                "Δ1M (bps)": row.get("change_1m_bps"),
                "Δ6M (bps)": row.get("change_6m_bps"),
                "ΔYTD (bps)": row.get("change_ytd_bps"),
            }
            for row in rows
        ]
    )
    delta_cols = ["Δ1W (bps)", "Δ1M (bps)", "Δ6M (bps)", "ΔYTD (bps)"]
    styler = _style_magnitude(table_df, delta_cols).format(
        {
            "Start of Year": _percent_formatter(2),
            "6M": _percent_formatter(2),
            "1M": _percent_formatter(2),
            "1W": _percent_formatter(2),
            "Current": _percent_formatter(2),
            "Δ1W (bps)": _bps_formatter(1),
            "Δ1M (bps)": _bps_formatter(1),
            "Δ6M (bps)": _bps_formatter(1),
            "ΔYTD (bps)": _bps_formatter(1),
        },
        na_rep=MISSING_DISPLAY,
    )
    cols[1].dataframe(styler, width="stretch")


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
                    "Start of Year": row.get("start_of_year"),
                    "6M": row.get("last_6m"),
                    "1M": row.get("last_month"),
                    "1W": row.get("last_week"),
                    "Current": row.get("current"),
                    "Δ1W (bps)": row.get("change_1w_bps"),
                    "Δ1M (bps)": row.get("change_1m_bps"),
                    "Δ6M (bps)": row.get("change_6m_bps"),
                    "ΔYTD (bps)": row.get("change_ytd_bps"),
                }
            )
        return pd.DataFrame(table)

    delta_cols = ["Δ1W (bps)", "Δ1M (bps)", "Δ6M (bps)", "ΔYTD (bps)"]
    real_df = _table_from_rows(real_rows)
    real_styler = _style_magnitude(real_df, delta_cols).format(
        {
            "Start of Year": _percent_formatter(2),
            "6M": _percent_formatter(2),
            "1M": _percent_formatter(2),
            "1W": _percent_formatter(2),
            "Current": _percent_formatter(2),
            "Δ1W (bps)": _bps_formatter(1),
            "Δ1M (bps)": _bps_formatter(1),
            "Δ6M (bps)": _bps_formatter(1),
            "ΔYTD (bps)": _bps_formatter(1),
        },
        na_rep=MISSING_DISPLAY,
    )
    cols[1].dataframe(real_styler, width="stretch")

    breakeven_df = _table_from_rows(breakeven_rows)
    breakeven_styler = _style_magnitude(breakeven_df, delta_cols).format(
        {
            "Start of Year": _percent_formatter(2),
            "6M": _percent_formatter(2),
            "1M": _percent_formatter(2),
            "1W": _percent_formatter(2),
            "Current": _percent_formatter(2),
            "Δ1W (bps)": _bps_formatter(1),
            "Δ1M (bps)": _bps_formatter(1),
            "Δ6M (bps)": _bps_formatter(1),
            "ΔYTD (bps)": _bps_formatter(1),
        },
        na_rep=MISSING_DISPLAY,
    )
    st.dataframe(breakeven_styler, width="stretch")

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
    st.header("Policy Futures (Evidence-Only)")
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
            rows.append({"Contract": display, "Snapshot": label, "Price": value})
    df = pd.DataFrame(rows)
    domain = _calc_domain(df["Price"].tolist())
    curve_chart = (
        alt.Chart(df)
        .mark_line(point=True, interpolate="linear")
        .encode(
            x=alt.X("Contract:N", sort=display_labels, title="Contract"),
            y=alt.Y("Price:Q", title="Price", scale=alt.Scale(domain=domain)),
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
            tooltip=["Contract", "Snapshot", "Price"],
        )
    )

    cols = st.columns(2)
    cols[0].altair_chart(curve_chart, width="stretch")
    cols[0].caption("Anchor points only. Axis is data-range (not zero-based).")

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
            _value_or_failed(contract.get("last_week_price"), lambda v: _format_number(v, decimals=2)),
            _value_or_failed(contract.get("last_month_price"), lambda v: _format_number(v, decimals=2)),
            _value_or_failed(contract.get("last_6m_price"), lambda v: _format_number(v, decimals=2)),
            _value_or_failed(contract.get("start_of_year_price"), lambda v: _format_number(v, decimals=2)),
        ]

    table_df = pd.DataFrame(
        table_cols,
        index=["Current", "1W", "1M", "6M", "SOY"],
    )
    cols[1].dataframe(table_df, width="stretch")


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
                "Current": volatility.get(key),
                "1D %": entry.get("1d_pct"),
                "5D %": entry.get("5d_pct"),
                "1M %": entry.get("1m_pct"),
                "6M %": entry.get("6m_pct"),
                "MOVE/VIX": volatility.get("move_vix_ratio") if label == "MOVE" else None,
                "GVZ/VIX": volatility.get("gvz_vix_ratio") if label == "GVZ" else None,
                "OVX/VIX": volatility.get("ovx_vix_ratio") if label == "OVX" else None,
            }
        )

    table_df = pd.DataFrame(rows)
    change_cols = ["1D %", "5D %", "1M %", "6M %"]
    styler = _style_magnitude(table_df, change_cols).format(
        {
            "Current": _number_formatter(2),
            "1D %": _percent_formatter(1),
            "5D %": _percent_formatter(1),
            "1M %": _percent_formatter(1),
            "6M %": _percent_formatter(1),
            "MOVE/VIX": _number_formatter(2),
            "GVZ/VIX": _number_formatter(2),
            "OVX/VIX": _number_formatter(2),
        },
        na_rep=MISSING_DISPLAY,
    )
    st.dataframe(styler, width="stretch")

    regime = _get_block(daily_state, "volatility_regime")
    if regime:
        zscores = regime.get("zscore_3y", {}) if isinstance(regime.get("zscore_3y"), dict) else {}
        regime_rows = [
            {
                "Segment": "Equity (VIX)",
                "Regime": regime.get("equity"),
                "Z-score (3Y)": _format_number(zscores.get("vix"), decimals=2),
            },
            {
                "Segment": "Rates (MOVE)",
                "Regime": regime.get("rates"),
                "Z-score (3Y)": _format_number(zscores.get("move"), decimals=2),
            },
            {"Segment": "Joint", "Regime": regime.get("joint"), "Z-score (3Y)": MISSING_DISPLAY},
        ]
        st.dataframe(pd.DataFrame(regime_rows), width="stretch")
        if regime.get("boundary_case"):
            st.caption("Boundary case detected in regime thresholds.")

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
            st.altair_chart(raw_chart, width="stretch")
        else:
            st.info("Volatility history not available.")

        z_rows = []
        z_rows += _transform_rows(history, "vix", "zscore_3y", window, "VIX Z (3Y)")
        z_rows += _transform_rows(history, "move", "zscore_3y", window, "MOVE Z (3Y)")
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
                    y=alt.Y("Value:Q", title="Z-score (3Y)"),
                    color=alt.Color("Series:N"),
                    tooltip=["Date", "Series", "Value"],
                )
                .properties(title="Volatility Standardized (Z-score)")
            )
            st.altair_chart(z_chart + zero_line, width="stretch")

        ratio_rows = _transform_rows(history, "move", "pct_of_avg_3y", window, "MOVE % of Avg (3Y)")
        ratio_rows += _transform_rows(history, "vix", "pct_of_avg_3y", window, "VIX % of Avg (3Y)")
        ratio_df = pd.DataFrame(ratio_rows)
        if not ratio_df.empty:
            ratio_df["Date"] = pd.to_datetime(ratio_df["Date"], errors="coerce")
            ratio_df = ratio_df.dropna(subset=["Date"])
            ratio_chart = (
                alt.Chart(ratio_df)
                .mark_line(interpolate="linear")
                .encode(
                    x=alt.X("Date:T", title="Date"),
                    y=alt.Y("Value:Q", title="Percent of Avg"),
                    color=alt.Color("Series:N"),
                    tooltip=["Date", "Series", "Value"],
                )
                .properties(title="Volatility Percent of Avg (3Y)")
            )
            st.altair_chart(ratio_chart, width="stretch")

        spread_rows = _cross_asset_rows(history, "move_vix_z_spread", window, "MOVE Z - VIX Z")
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
                "Level": entry.get("level"),
                "Δ1W": entry.get("change_1w"),
                "Δ1M": entry.get("change_1m"),
                "Δ6M": entry.get("change_6m"),
                "ΔSOY": entry.get("change_ytd"),
            }
        )
    table_df = pd.DataFrame(table_rows)
    delta_cols = ["Δ1W", "Δ1M", "Δ6M", "ΔSOY"]
    styler = _style_magnitude(table_df, delta_cols).format(
        {
            "Level": _number_formatter(2),
            "Δ1W": _number_formatter(2),
            "Δ1M": _number_formatter(2),
            "Δ6M": _number_formatter(2),
            "ΔSOY": _number_formatter(2),
        },
        na_rep=MISSING_DISPLAY,
    )
    st.dataframe(styler, width="stretch")

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
    st.header("Labor Market (Evidence-Only)")
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
    st.header("FX Conditions (Evidence-Only)")
    fx = _get_block(daily_state, "fx")
    if not fx:
        st.info("fx data not available.")
        return

    dxy_block = fx.get("dxy", {}) if isinstance(fx.get("dxy"), dict) else {}
    dxy_anchors = dxy_block.get("anchors", {}) if isinstance(dxy_block.get("anchors"), dict) else {}
    dxy_changes = dxy_block.get("changes_pct", {}) if isinstance(dxy_block.get("changes_pct"), dict) else {}
    dxy_table = [
        {
            "Series": dxy_block.get("label", "DXY"),
            "Current": dxy_anchors.get("current"),
            "1D %": dxy_changes.get("1d"),
            "5D %": dxy_changes.get("5d"),
            "1M %": dxy_changes.get("1m"),
            "6M %": dxy_changes.get("6m"),
        }
    ]
    dxy_df = pd.DataFrame(dxy_table)
    dxy_change_cols = ["1D %", "5D %", "1M %", "6M %"]
    dxy_styler = _style_magnitude(dxy_df, dxy_change_cols).format(
        {
            "Current": _number_formatter(2),
            "1D %": _percent_formatter(1),
            "5D %": _percent_formatter(1),
            "1M %": _percent_formatter(1),
            "6M %": _percent_formatter(1),
        },
        na_rep=MISSING_DISPLAY,
    )
    st.dataframe(dxy_styler, width="stretch")

    rate_block = fx.get("rate_differentials", {}) if isinstance(fx.get("rate_differentials"), dict) else {}
    rate_rows = rate_block.get("rows", []) if isinstance(rate_block.get("rows"), list) else []
    if rate_rows:
        rate_table = []
        for row in rate_rows:
            rate_table.append(
                {
                    "Currency": row.get("currency"),
                    "Policy Rate": row.get("policy_rate"),
                    "Fed Funds": row.get("fed_funds"),
                    "Diff (bps)": row.get("differential_bps"),
                    "Data Quality": row.get("data_quality"),
                }
            )
        st.subheader("Rate Differentials vs Fed (Evidence-Only)")
        rate_df = pd.DataFrame(rate_table)
        rate_styler = _style_magnitude(rate_df, ["Diff (bps)"]).format(
            {
                "Policy Rate": _number_formatter(2),
                "Fed Funds": _number_formatter(2),
                "Diff (bps)": _bps_formatter(1),
            },
            na_rep=MISSING_DISPLAY,
        )
        st.dataframe(rate_styler, width="stretch")

    matrix = fx.get("matrix_1m_pct", {}) if isinstance(fx.get("matrix_1m_pct"), dict) else {}
    matrix_rows = _matrix_rows(matrix)
    if matrix_rows:
        df = pd.DataFrame(matrix_rows)
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
        max_abs = df["Value"].abs().max()
        if pd.notna(max_abs) and max_abs > 0:
            heatmap = (
                alt.Chart(df)
                .mark_rect()
                .encode(
                    x=alt.X("Quote:N", sort=matrix.get("currencies", []), title="Quote"),
                    y=alt.Y("Base:N", sort=matrix.get("currencies", []), title="Base"),
                    color=alt.Color(
                        "Value:Q",
                        scale=alt.Scale(domain=[-max_abs, max_abs], scheme="blueorange"),
                        title="1M % Change",
                    ),
                    tooltip=["Base", "Quote", "Value"],
                )
                .properties(title="FX Matrix (1M % Change)")
            )
            st.altair_chart(heatmap, width="stretch")
        else:
            pivot = df.pivot(index="Base", columns="Quote", values="Value")
            table = pivot.applymap(lambda v: _format_percent(v, decimals=2) if pd.notna(v) else MISSING_DISPLAY)
            st.subheader("FX Matrix (1M % Change)")
            st.dataframe(table, width="stretch")

    baskets = fx.get("risk_baskets", {}) if isinstance(fx.get("risk_baskets"), dict) else {}
    if baskets:
        def _basket_row(label: str, entry: Dict[str, Any]) -> Dict[str, Any]:
            anchors = entry.get("anchors", {}) if isinstance(entry.get("anchors"), dict) else {}
            return {
                "Basket": label,
                "SOY": _format_number(anchors.get("start_of_year"), decimals=2),
                "6M": _format_number(anchors.get("last_6m"), decimals=2),
                "1M": _format_number(anchors.get("last_month"), decimals=2),
                "1W": _format_number(anchors.get("last_week"), decimals=2),
                "Current": _format_number(anchors.get("current"), decimals=2),
                "Data Quality": entry.get("data_quality"),
            }

        rows = [
            _basket_row("Risk-On", baskets.get("risk_on", {})),
            _basket_row("Risk-Off", baskets.get("risk_off", {})),
        ]
        spread_anchors = baskets.get("spread", {}).get("anchors", {})
        rows.append(
            {
                "Basket": "Spread (On - Off)",
                "SOY": _format_number(spread_anchors.get("start_of_year"), decimals=2),
                "6M": _format_number(spread_anchors.get("last_6m"), decimals=2),
                "1M": _format_number(spread_anchors.get("last_month"), decimals=2),
                "1W": _format_number(spread_anchors.get("last_week"), decimals=2),
                "Current": _format_number(spread_anchors.get("current"), decimals=2),
                "Data Quality": MISSING_DISPLAY,
            }
        )
        st.subheader("Risk-On vs Risk-Off FX Basket (Index)")
        st.dataframe(pd.DataFrame(rows), width="stretch")

    fx_vol = _get_block(daily_state, "fx_volatility")
    if fx_vol:
        vol_entries = fx_vol.get("entries", []) if isinstance(fx_vol.get("entries"), list) else []
        if vol_entries:
            vol_rows = []
            for entry in vol_entries:
                vol_rows.append(
                    {
                        "Pair": entry.get("pair"),
                        "Realized Vol (20D) %": _format_percent(entry.get("realized_vol_20d_pct"), decimals=2),
                        "Z-score (3Y)": _format_number(entry.get("zscore_3y"), decimals=2),
                        "Regime": entry.get("regime"),
                        "Data Quality": entry.get("data_quality"),
                    }
                )
            st.subheader("FX Volatility (Realized 20D)")
            st.dataframe(pd.DataFrame(vol_rows), width="stretch")

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
    st.header("Credit Detail (Evidence-Only)")
    credit = _get_block(daily_state, "credit_transmission")
    if not credit:
        st.info("credit_transmission data not available.")
        return

    rows = [
        {
            "Series": "IG OAS",
            "Current": _format_number(credit.get("ig_oas_current"), decimals=2),
            "Weekly Change (bps)": credit.get("ig_oas_weekly_change_bps"),
        },
        {
            "Series": "HY OAS",
            "Current": _format_number(credit.get("hy_oas_current"), decimals=2),
            "Weekly Change (bps)": credit.get("hy_oas_weekly_change_bps"),
        },
        {
            "Series": "10Y Treasury",
            "Current": _format_percent(credit.get("treasury_10y_current"), decimals=2),
            "Weekly Change (bps)": credit.get("treasury_10y_weekly_change_bps"),
        },
    ]
    credit_df = pd.DataFrame(rows)
    credit_styler = _style_magnitude(credit_df, ["Weekly Change (bps)"]).format(
        {
            "Weekly Change (bps)": _bps_formatter(1),
        },
        na_rep=MISSING_DISPLAY,
    )
    st.dataframe(credit_styler, width="stretch")

    history = _load_history_state()
    if history:
        window = _select_window("Credit History Window", key="credit_history_window")
        chart = _history_chart(
            history,
            window,
            {"ig_oas": "IG OAS", "hy_oas": "HY OAS"},
            "Credit Spreads History",
        )
        if chart is not None:
            st.altair_chart(chart, width="stretch")
        else:
            st.info("Credit history not available.")


def render_cross_signals(daily_state: Dict[str, Any]) -> None:
    st.header("Cross-Signals")
    policy = _get_block(daily_state, "policy")
    policy_curve = _get_block(daily_state, "policy_curve")
    liquidity_curve = _get_block(daily_state, "liquidity_curve")
    disagreements = _get_block(daily_state, "disagreements")
    vol_cross = _get_block(daily_state, "vol_credit_cross")

    cols = st.columns(3)
    cols[0].metric("Spot Stance", policy.get("spot_stance") or MISSING_DISPLAY)
    cols[1].metric("Expected Direction", policy_curve.get("expected_direction") or MISSING_DISPLAY)
    cols[2].metric("Liquidity", liquidity_curve.get("expected_liquidity") or MISSING_DISPLAY)

    with st.expander("Explanations"):
        st.write(policy.get("explanation") or MISSING_DISPLAY)
        st.write(policy_curve.get("explanation") or MISSING_DISPLAY)
        st.write(liquidity_curve.get("explanation") or MISSING_DISPLAY)

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
                "Flag": entry.get("flag", MISSING_DISPLAY),
                "Explanation": entry.get("explanation") or MISSING_DISPLAY,
            }
        )
    st.dataframe(pd.DataFrame(disagreement_rows), width="stretch")

    if vol_cross:
        st.subheader("Volatility vs Credit")
        label = vol_cross.get("label") or MISSING_DISPLAY
        explanation = vol_cross.get("explanation") or MISSING_DISPLAY
        st.write(f"{label}: {explanation}")


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
    st.write(f"Last updated: {health.get('generated_at') or MISSING_DISPLAY}")
    st.write(f"Age: {health.get('age_human') or MISSING_DISPLAY}")
    failed_series = health.get("failed_series")
    total_series = health.get("total_series")
    if failed_series is None or total_series is None:
        st.write(f"Failed series: {MISSING_DISPLAY}")
    else:
        st.write(f"Failed series: {failed_series} / {total_series}")
    history_flag = health.get("history_state_available")
    if history_flag is not None:
        st.write(f"History state: {'Available' if history_flag else 'Missing'}")
    failed_list = health.get("failed_series_list")
    if isinstance(failed_list, list) and failed_list:
        st.caption("Failed series list:")
        st.caption(", ".join(str(item) for item in failed_list))


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

    tab_rates, tab_policy, tab_labor, tab_fx, tab_risk, tab_health = st.tabs(
        [
            "📈 Rates & Inflation",
            "🏦 Policy & Liquidity",
            "👷 Labor Market",
            "💱 FX Conditions",
            "⚡ Volatility & Risk",
            "🛠 System Health",
        ]
    )

    with tab_rates:
        render_yield_curve_panel(daily_state)
        render_real_rates_panel(daily_state)

    with tab_policy:
        render_policy_futures_panel(daily_state)
        render_liquidity_panel(daily_state)
        render_cross_signals(daily_state)

    with tab_labor:
        render_labor_panel(daily_state)

    with tab_fx:
        render_fx_panel(daily_state)

    with tab_risk:
        render_volatility_panel(daily_state)
        render_credit_panel(daily_state)

    with tab_health:
        render_system_health(daily_state)
        with st.expander("Raw JSON"):
            st.json(daily_state)


if __name__ == "__main__":
    main()
