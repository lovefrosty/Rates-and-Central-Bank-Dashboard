# Quant Macro Dashboard

**Evidence-First Macro State Monitoring**

---

## Overview

This repository implements a **rule-based, evidence-first macro dashboard** designed to explain *state*, not predict returns.

The system ingests macroeconomic data, market-implied signals, and liquidity balances, transforms them into transparent mechanical metrics, and resolves interpretable regime labels — all surfaced in a visual, professional Streamlit UI.

The dashboard is intentionally **non-prescriptive**. Its purpose is to place all relevant macro evidence on screen simultaneously so the user can reason independently.

> **Principle:** Facts → Mechanics → Evidence → Interpretation
> No layer skips the one before it.

---

## Design Philosophy

### 1. Explain State, Not Returns

This system does **not** forecast asset prices or optimize portfolios.
It answers a simpler but more durable question:

> *“What is the macro environment right now, and why?”*

Returns are a downstream consequence of state — not the input.

---

### 2. Evidence-First, Text-Minimal UI

The UI is deliberately **visual-first**:

* Line charts over bar charts where possible
* Snapshot anchors (Current / 1W / 1M / SOY) over dense time series
* Tables that resemble Excel (explicit numbers, percentages, deltas)
* Minimal narrative text

Text is used only for:

* Section labels
* Resolver summaries
* Thought-provoking questions (not answers)

The goal is **interpretability without instruction**.

---

### 3. Ambiguity Is a Feature

Macro regimes are rarely clean.

The dashboard:

* Allows contradictions to coexist
* Surfaces disagreements explicitly
* Avoids forced consensus labels
* Encourages user judgment

If policy, expectations, and liquidity disagree — that disagreement is shown, not hidden.

---

## Architectural Invariants (Strictly Enforced)

The pipeline is intentionally rigid. These rules are tested and enforced.

### Data Flow

```
Fetchers → raw_state.json → Analytics → daily_state.json → Resolvers → UI
```

### Layer Rules

#### Fetchers

* Pull raw data only
* No analytics, labels, or UI logic
* Write only to `signals/raw_state.json`
* Missing data → explicit `FAILED`

#### Analytics

* Read **only** `raw_state.json`
* Write **one block each** to `signals/daily_state.json`
* Mechanical transforms only (rates, spreads, ratios, deltas)
* No inference, smoothing, or forecasting

#### Resolvers

* Read **only** `daily_state.json`
* Write labels **only when evidence exists**
* No access to raw data

#### UI

* Reads **only** `daily_state.json`
* No calculations
* No hidden logic
* Purely presentational

---

## Data Discipline

### Snapshot-Only State

* No stored time series
* No forward fills
* No resampling
* No smoothing

All metrics are derived from fixed snapshot anchors:

* **Current**
* **1 Week**
* **1 Month**
* **Start of Year**

Charts may visually overlay these anchors, but **only snapshots are persisted**.

---

## Data Sources

| Category                            | Source                                  |
| ----------------------------------- | --------------------------------------- |
| Macro levels (rates, CPI, balances) | FRED (via OpenBB or HTTP fallback)      |
| Market-implied volatility           | Yahoo Finance (^VIX, ^MOVE, ^GVZ, ^OVX) |
| Policy futures (ZQ)                 | Yahoo Finance                           |
| FX indices                          | Yahoo Finance                           |
| Global policy rates                 | FRED                                    |

All sources flow through provider wrappers and emit identical schema-validated output.

---

## Key Dashboard Sections

* **Yield Curve**

  * Nominal curve (multi-anchor)
  * Real yields
  * Breakeven inflation
* **Policy**

  * Spot stance
  * Policy futures implied path
* **Liquidity**

  * RRP, TGA, Fed balance sheet
* **Volatility**

  * VIX, MOVE, GVZ, OVX
  * Ratios vs VIX
* **Credit**

  * IG / HY spreads
  * Vol-credit cross signals
* **Labor**

  * Employment, wages, slack
* **Global Policy**

  * ECB, BOJ, USD alignment
* **Disagreements**

  * Policy vs expectations vs liquidity
* **Reasoning Guide**

  * Questions only, no answers
* **System Health**

  * Data coverage
  * Last update timestamp

---

## What This System Intentionally Does NOT Do

* No price targets
* No return forecasts
* No portfolio optimization
* No hidden heuristics
* No forced regime classification

This is a **thinking tool**, not a signal generator.

---

## System Health & Transparency

The dashboard self-reports:

* Which data blocks are populated
* Which are missing or FAILED
* Last successful update timestamp

Silent failures are treated as bugs.

---

## Guiding Ethos

> “A good macro dashboard does not tell you what to think.
> It makes it impossible to think without evidence.”

This repository is designed to scale in depth, not opinion.

---

## Running the Dashboard

```bash
export FRED_API_KEY=your_key_here
python update.py
streamlit run UI/dashboard.py
```

---

## Contribution Guidelines

When adding new features:

* Preserve layer purity
* Prefer visuals over text
* Store snapshots, not time series
* Allow ambiguity
* Never infer missing data

If in doubt, **do less**.

---

End of README.
