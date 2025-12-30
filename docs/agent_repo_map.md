# Agent Repo Map

## Layers and Locations
- Data ingestion: `Data/` (fetchers returning canonical ingestion objects, writing to `signals/raw_state.json` via `update.py`).
- Analytics writers: `Analytics/` (read `signals/raw_state.json`, write one top-level key into `signals/daily_state.json`).
- Resolvers: `Signals/resolve_*.py` (read `signals/daily_state.json`, write one top-level key into `signals/daily_state.json`).
- UI: `UI/` (Streamlit entrypoints and components, read-only consumers).
- Tests: `tests/`.
- Orchestrator: `update.py` (raw_state build, analytics writers, resolvers).
- History: `History/` (builds `signals/history_state.json`, and history-derived daily-state writers).

## Raw State Schema / Validator
- Schema and validator: `Signals/raw_state_schema.py` (expected keys + ingestion fields).
- Raw-state structural validation used by orchestrator: `Signals/validate.py`.

## Orchestration Flow (current)
- `update.py` builds raw_state, writes `signals/raw_state.json`, then calls analytics writers, history-derived writers, and resolvers to update `signals/daily_state.json`.
- `history_update.py` writes `signals/history_state.json` (time-series only).

## Snapshot Helper
- Shared snapshot selection helper: `Data/utils/snapshot_selection.py`.
- Used by fetchers that add snapshot meta to ingestion objects.

## Daily State Writers
- Analytics writers (examples):
  - `Analytics/policy_witnesses.py`
  - `Analytics/inflation_real_rates.py`
  - `Analytics/inflation_witnesses.py`
  - `Analytics/labor_market.py`
  - `Analytics/volatility_analytics.py`
  - `Analytics/liquidity_analytics.py`
  - `Analytics/credit_transmission.py`
  - `Analytics/global_policy_alignment.py`
  - `Analytics/fx_panel.py`
  - `Analytics/system_health.py`
  - `Analytics/yield_curve_analytics.py`
- History-derived writers:
  - `History/volatility_regime.py`
  - `History/fx_volatility.py`
- Resolvers (examples):
  - `Signals/resolve_policy.py`
  - `Signals/resolve_policy_curve.py`
  - `Signals/resolve_liquidity_curve.py`
  - `Signals/resolve_disagreements.py`
  - `Signals/resolve_vol_credit_cross.py`

## Files to Avoid Touching Without Explicit Need
- `update.py` (orchestrator)
- `Signals/raw_state_schema.py` / `Signals/validate.py`
- `UI/`
