"""Streamlit dashboard entrypoint."""
import json
import sys
from pathlib import Path

# --- SETUP PATHS ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
import altair as alt

from Signals import state_paths

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Macro Strategy Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- HELPER FUNCTIONS ---
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
    return str(value) if value is not None else "Unavailable"

def _format_cell(value, decimals=2):
    return f"{value:.{decimals}f}" if value is not None else "Unavailable"

def _format_table_rows(rows):
    formatted = []
    for row in rows:
        formatted.append({
            "Tenor": row["tenor"],
            "Start of Year": row["start_of_year"],
            "Last Week": row["last_week"],
            "Current": row["current"],
            "Weekly Change (bps)": row["weekly_change_bps"],
        })
    return formatted

# --- RENDER FUNCTIONS (PANELS) ---

def render_rates_tab(daily_state: dict):
    """
    Combines Nominal Yields, Real Yield Decomposition, and Inflation/Labor Context.
    """
    
    # --- DATA EXTRACTION ---
    # 1. Yield Curve Data
    yield_curve = _get_block(daily_state, "yield_curve")
    
    # 2. Real Rates & Decomposition
    real_rates = _get_block(daily_state, "inflation_real_rates")
    nominal_10y = real_rates.get("nominal_10y")
    real_10y = real_rates.get("real_10y")
    be_10y = real_rates.get("breakeven_10y")
    
    # 3. Inflation Context
    inf_wit = _get_block(daily_state, "inflation_witnesses")
    cpi_head = inf_wit.get("cpi_headline_yoy_pct")
    cpi_core = inf_wit.get("cpi_core_yoy_pct")
    
    # 4. Labor Context
    labor = _get_block(daily_state, "labor_market")
    unrate = labor.get("unrate_current")

    # --- SECTION 1: YIELD CURVE (EXISTING) ---
    st.subheader("1. Nominal Yield Curve")
    if yield_curve:
        lines = yield_curve.get("lines", {})
        tenors = yield_curve.get("tenors", [])
        
        # Chart Data
        chart_df = pd.DataFrame({
            "Tenor": tenors,
            "Start of Year": lines.get("start_of_year", []),
            "Last Week": lines.get("last_week", []),
            "Current": lines.get("current", []),
        })
        chart_long = chart_df.melt("Tenor", var_name="Snapshot", value_name="Yield")
        
        curve_chart = (
            alt.Chart(chart_long)
            .mark_line(point=True)
            .encode(
                x=alt.X("Tenor:N", sort=tenors, title="Tenor"),
                y=alt.Y("Yield:Q", title="Yield (%)", scale=alt.Scale(zero=False)),
                color=alt.Color("Snapshot:N", sort=["Start of Year", "Last Week", "Current"]),
                tooltip=["Tenor", "Snapshot", "Yield"],
            )
            .properties(height=300)
        )
        st.altair_chart(curve_chart, use_container_width=True)
    else:
        st.info("Yield Curve data unavailable")

    st.divider()
    
    # --- SECTION 2: YIELD DECOMPOSITION (NEW) ---
    st.subheader("2. Yield Drivers: Real vs. Inflation")
    st.caption("Decomposing the 10Y Yield into Growth (Real) and Inflation (Breakeven) components.")

    col1, col2 = st.columns([1, 2])

    with col1:
        # Metrics Stack
        st.metric("10Y Nominal", f"{_format_cell(nominal_10y)}%")
        st.metric("10Y Real (TIPS)", f"{_format_cell(real_10y)}%", help="Proxy for Growth expectations")
        st.metric("10Y Breakeven", f"{_format_cell(be_10y)}%", help="Market implied inflation")
    
    with col2:
        # Stacked Bar Chart: Real + Breakeven = Nominal
        if nominal_10y and real_10y and be_10y:
            decomp_data = pd.DataFrame([
                {"Component": "Real Yield (Growth)", "Value": real_10y, "Order": 1},
                {"Component": "Breakeven (Inflation)", "Value": be_10y, "Order": 2}
            ])
            
            decomp_chart = (
                alt.Chart(decomp_data)
                .mark_bar()
                .encode(
                    x=alt.X("Value:Q", title="Yield Contribution (%)"),
                    y=alt.Y("Component:N", sort=None, title=""), # Hide axis title
                    color=alt.Color("Component:N", legend=None),
                    tooltip=["Component", "Value"]
                )
                .properties(height=200, title=f"10Y Yield Decomposition ({nominal_10y}%)")
            )
            
            # Add a vertical rule for the total nominal yield for comparison
            rule = alt.Chart(pd.DataFrame({'x': [nominal_10y]})).mark_rule(color='red', strokeDash=[5,5]).encode(x='x')
            
            st.altair_chart(decomp_chart, use_container_width=True)
        else:
            st.warning("Decomposition data incomplete")

    st.divider()

    # --- SECTION 3: MACRO CONTEXT (NEW) ---
    st.subheader("3. Macro Context: Reality vs. Pricing")
    
    c1, c2, c3, c4 = st.columns(4)
    
    # Comparison: CPI (Reality) vs Breakeven (Market Pricing)
    delta = (be_10y - cpi_head) if (be_10y and cpi_head) else 0
    c1.metric("Headline CPI", f"{_format_cell(cpi_head)}%", help="Most recent YoY Print")
    c2.metric("Market Breakeven", f"{_format_cell(be_10y)}%", delta=f"{delta:.2f}% vs CPI", delta_color="inverse")
    
    c3.metric("Core CPI", f"{_format_cell(cpi_core)}%", help="Ex-Food & Energy")
    c4.metric("Unemployment Rate", f"{_format_cell(unrate)}%", help="Fed Dual Mandate Variable")

def render_policy_tab(daily_state: dict):
    """
    Debug Version: Wraps rendering logic in try/except to identify the crash.
    """
    
    # --- DATA EXTRACTION ---
    policy = _get_block(daily_state, "policy")
    policy_curve = _get_block(daily_state, "policy_curve")
    policy_futures = _get_block(daily_state, "policy_futures_curve")
    liquidity_curve = _get_block(daily_state, "liquidity_curve")
    
    # --- 1. POLICY STANCE ---
    st.subheader("1. The Policy Stance")
    c1, c2, c3 = st.columns(3)
    c1.metric("Current Fed Rate", _format_value(policy.get("spot_stance")))
    c2.metric("Market Expectation", _format_value(policy_curve.get("expected_direction")))
    c3.metric("Liquidity Outlook", _format_value(liquidity_curve.get("expected_liquidity")))

    st.divider()

    # --- 2. ZQ FUTURES (CRASH ZONE) ---
    st.subheader("2. Market Implied Path (ZQ Futures)")
    
    try:
        # Debug: Check if data exists
        contracts = policy_futures.get("contracts", [])
        if not contracts:
            st.warning("⚠️ No 'contracts' found in policy_futures_curve block.")
        
        else:
            # Build DataFrame safely
            data_rows = []
            for c in contracts:
                # Ensure we handle missing keys gracefully
                data_rows.append({
                    "Tenor": str(c.get("ticker", "Unknown")), 
                    "Current": c.get("current_rate_pct", 0.0),
                    "Last Week": c.get("last_week_rate_pct", 0.0),
                    "Start of Year": c.get("start_of_year_rate_pct", 0.0)
                })
            
            chart_df = pd.DataFrame(data_rows)
            chart_long = chart_df.melt("Tenor", var_name="Snapshot", value_name="Implied Rate")
            
            # Ensure types are correct for Altair
            chart_long["Implied Rate"] = pd.to_numeric(chart_long["Implied Rate"], errors='coerce')

            # Render Chart
            lines = (
                alt.Chart(chart_long)
                .mark_line(point=True)
                .encode(
                    x=alt.X("Tenor:N", sort=None, title="Contract"),
                    y=alt.Y("Implied Rate:Q", title="Implied Rate (%)", scale=alt.Scale(zero=False)),
                    color=alt.Color("Snapshot:N", sort=["Start of Year", "Last Week", "Current"]),
                    tooltip=["Tenor", "Snapshot", "Implied Rate"],
                )
                .properties(height=350)
            )
            st.altair_chart(lines, use_container_width=True)

    except Exception as e:
        st.error(f"🚨 Section 2 Crashed: {e}")
        st.write("Debug Data:", contracts[:1] if 'contracts' in locals() else "No data loaded")

    st.divider()

    # --- 3. LIQUIDITY (PREVIOUSLY MISSING) ---
    st.subheader("3. Liquidity & Divergences")
    
    try:
        c1, c2 = st.columns([1, 1])
        
        with c1:
            st.markdown("**Liquidity Context**")
            status = liquidity_curve.get("expected_liquidity", "Unknown")
            expl = liquidity_curve.get("explanation", "No data")
            color = "green" if "Add" in status or "Inject" in status else "red"
            st.markdown(f":{color}[**{status}**]")
            st.write(expl)

        with c2:
            st.markdown("**Key Disagreements**")
            disagreements = _get_block(daily_state, "disagreements")
            if disagreements:
                for k, v in disagreements.items():
                    if isinstance(v, dict) and v.get("flag") is True:
                        st.warning(f"**{k.replace('_', ' ').title()}:** {v.get('explanation')}")
            else:
                st.info("No disagreement data found.")
                
    except Exception as e:
        st.error(f"🚨 Section 3 Crashed: {e}")

def render_sidebar_reasoning():
    """
    The 'Reasoning Layer'. A persistent set of questions to frame the analyst's thinking.
    No answers, only questions to enforce discipline.
    """
    st.sidebar.divider()
    st.sidebar.subheader("🧠 Reasoning Guide")
    
    # Section 1: Rates (The "Why")
    with st.sidebar.expander("Rates Drivers", expanded=True):
        st.markdown("""
        **Decomposition**
        * Is the move in 10Y driven by **Real Rates** (Growth outlook) or **Breakevens** (Inflation fear)?
        * *Check:* If Real Yields are flat but Nominal is up → Pure Inflation trade.
        
        **Curve Shape**
        * **Bull Steepener:** Short rates falling faster than long? (Recession pricing?)
        * **Bear Steepener:** Long rates rising faster than short? (Fiscal/Term Premium panic?)
        """)

    # Section 2: Policy (The "Path")
    with st.sidebar.expander("Policy vs Market"):
        st.markdown("""
        **The Disagreement**
        * Where does the **ZQ Implied Rate** cross the Fed's dot plot?
        * Is the market pricing cuts that the Fed has explicitly ruled out?
        
        **Liquidity Impulse**
        * Is the TGA (Treasury Account) draining? (Net liquidity injection)
        * Is the RRP facility empty? (Loss of liquidity buffer)
        """)

    # Section 3: Risk (The "Stress")
    with st.sidebar.expander("Vol & Credit"):
        st.markdown("""
        **Signal Quality**
        * Is **VIX** rising *without* Credit Spreads (HY OAS) widening? (If so, it's just equity noise, not systemic).
        * Is **MOVE** (Bond Vol) diverging from VIX? (Rates market is the source of stress).
        * Is **DXY** rising alongside yields? (Tightening financial conditions).
        """)
def render_risk_tab(daily_state: dict):
    """
    Volatility & Risk View.
    Visualizes the "Big 4" Vol Indices and the MOVE/VIX spread.
    """
    # --- DATA EXTRACTION ---
    # We now look for the correct key you provided: "volatility"
    vol = _get_block(daily_state, "volatility")
    
    if not vol:
        st.info("Volatility data unavailable")
        return

    # Extract Changes for Deltas
    changes = vol.get("changes_pct", {})
    
    # --- SECTION 1: THE FEAR GAUGES ---
    st.subheader("1. Volatility Monitor")
    
    c1, c2, c3, c4 = st.columns(4)
    
    # Helper to get change safely
    def get_chg(ticker, period="1d_pct"):
        return changes.get(ticker, {}).get(period, 0.0)

    # 1. VIX (Equities)
    c1.metric(
        "VIX (Equity)", 
        f"{vol.get('vix', 0):.2f}", 
        f"{get_chg('vix'):.2f}%", 
        delta_color="inverse" # Red is bad (up), Green is good (down)
    )
    
    # 2. MOVE (Rates)
    c2.metric(
        "MOVE (Bonds)", 
        f"{vol.get('move', 0):.2f}", 
        f"{get_chg('move'):.2f}%", 
        delta_color="inverse"
    )
    
    # 3. OVX (Oil)
    c3.metric(
        "OVX (Oil)", 
        f"{vol.get('ovx', 0):.2f}", 
        f"{get_chg('ovx'):.2f}%", 
        delta_color="inverse"
    )
    
    # 4. GVZ (Gold)
    c4.metric(
        "GVZ (Gold)", 
        f"{vol.get('gvz', 0):.2f}", 
        f"{get_chg('gvz'):.2f}%", 
        delta_color="inverse"
    )

    st.divider()

    # --- SECTION 2: SIGNAL INTERPRETATION ---
    st.subheader("2. Stress Signal")
    
    # The Text Reading
    signal = vol.get("stress_origin_read", "Unknown")
    
    # Visual Logic for the Banner
    if "Low" in signal:
        banner_color = "green"
        icon = "✅"
    elif "Rates" in signal:
        banner_color = "orange" 
        icon = "⚠️" # Warning for Rates Stress
    else:
        banner_color = "red"
        icon = "🚨"

    st.markdown(f"""
    <div style="padding: 15px; border-radius: 5px; border-left: 5px solid {banner_color}; background-color: rgba(50, 50, 50, 0.3);">
        <h4 style="margin:0;">{icon} System Status: {signal}</h4>
    </div>
    """, unsafe_allow_html=True)
    
    st.write("") # Spacer

    # --- SECTION 3: CROSS-ASSET RATIOS ---
    # We want to see if Bonds are panicking more than Stocks (MOVE / VIX)
    
    ratio = vol.get("move_vix_ratio")
    
    if ratio:
        st.markdown("**The 'Panic Spread' (MOVE / VIX Ratio)**")
        st.caption("High ratio (> 3.0) = Bond Market is the source of stress. Low ratio = Equity Market stress.")
        
        # Simple Bar to visualize the ratio
        # We normalize it visually: >5 is extreme, <3 is normal
        progress_val = min(max(ratio / 6.0, 0.0), 1.0) # Cap at 100% for the bar
        
        st.progress(progress_val)
        st.text(f"Current Ratio: {ratio:.2f}x (Bonds are {ratio:.1f} times more volatile than Equities)")
    
    else:
        st.warning("Ratio data missing")

def render_health_tab(daily_state: dict):
    """
    System Health Tab: Raw JSON viewer for debugging.
    """
    st.subheader("System State (Raw JSON)")
    st.caption("Use this to inspect the raw data structure when building new charts.")
    st.json(daily_state)

# --- MAIN APP SKELETON ---

def main():
    st.title("Macro Signal Dashboard")
    st.markdown("Decision Support System | _Snapshot Mode_")

    # Load Data ONCE
    daily_state = _load_daily_state()
    
    render_sidebar_reasoning()
    if not daily_state:
        st.error("�� `daily_state.json` not found.")
        st.markdown("""
        **System Status:**
        1. `raw_state.json`: ✅ Verified (Update.py works)
        2. `daily_state.json`: ❌ Missing
        
        **Action Required:**
        Please run your processing/analytics script to generate `daily_state.json` from the raw data.
        """)
        return

    # Create Tabs
    tab_rates, tab_policy, tab_risk, tab_health = st.tabs([
        "📈 Rates & Inflation",
        "🏦 Policy & Liquidity",
        "⚡ Volatility & Risk",
        "🛠 System Health"
    ])

    with tab_rates:
        render_rates_tab(daily_state)

    with tab_policy:
        render_policy_tab(daily_state)

    with tab_risk:
        render_risk_tab(daily_state)

    with tab_health:
        st.subheader("System Health & JSON Inspection")
        st.json(daily_state)

if __name__ == "__main__":
    main()