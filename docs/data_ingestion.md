
# Phase 1 — Data Ingestion & Universe Creation

## Purpose

Phase 1 establishes the **single source of truth** for the entire system.  
All downstream analytics, indicators, scanners, and models depend on the **accuracy, completeness, and reproducibility** of this data.

If data integrity is compromised at this stage, no amount of logic later can fix it.

---

## Core Responsibilities

- Build the complete **Indian equity universe**
- Ingest **raw OHLCV market data**
- Validate data correctness and consistency
- Produce clean, auditable datasets for downstream use

This phase focuses on **data engineering discipline**, not trading logic.

---

## Data Sources

### Equity Universe
- Source: **NSE official equity list**
- Endpoint:
  ```
  https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv
  ```
- Filters applied:
  - Series = `EQ` only
  - Valid listing date
  - Active symbols only

This file is treated as the **canonical universe**.

---

### Price Data (OHLCV)
- Source: **Yahoo Finance**
- Mapping:
  - `RELIANCE` → `RELIANCE.NS`
- Frequency: Daily
- Adjustments:
  - `auto_adjust = False`
  - Raw prices preserved intentionally

---

### Benchmark & Sector Indices
- Benchmark: NIFTY (used later for RS and regime detection)
- Sector indices: NSE sector indices (used in later phases)

---

## Folder Structure (Phase 1 Scope)

```
data/
├── raw/
│   ├── nse_universe.py
│   └── yahoo_ohlcv.py
│
├── processed/
│   ├── equity_universe.csv
│   └── ohlcv_equities.csv
│
configs/
├── data_sources.py
│
utils/
├── http.py
└── validation.py
│
pipelines/
└── build_universe.py
```

Only these folders/files are touched in Phase 1.

---

## Validation Rules

Before data is accepted:

### OHLCV Sanity Checks
- High ≥ max(Open, Close)
- Low ≤ min(Open, Close)
- Prices > 0
- Volume ≥ 0
- Monotonic increasing dates

### Structural Rules
- No forward filling of prices
- Missing days remain missing
- No silent symbol drops (must be logged)

Invalid symbols are **discarded explicitly**.

---

## Testing Philosophy

Phase 1 uses **lightweight sanity tests**, not heavy mocks.

Tests validate:
- NSE endpoint availability
- Correct parsing of universe CSV
- Equity series filtering logic
- Data cleaning behavior

Tests **do not**:
- Write CSV files
- Run full pipelines
- Depend on large datasets

---

## Pipeline Execution

The full Phase 1 pipeline is executed via:

```bash
python pipelines/build_universe.py
```

This command:
1. Downloads NSE universe
2. Cleans and filters symbols
3. Fetches Yahoo OHLCV data
4. Validates OHLCV integrity
5. Writes processed CSV outputs

---

## Outputs

After successful execution:

```
data/processed/
├── equity_universe.csv
└── ohlcv_equities.csv
```

These files are **immutable inputs** for all subsequent phases.

---

## Common Pitfalls Avoided

- Mixing multiple data vendors
- Ignoring corporate action distortions without documentation
- Forward-filling missing market data
- Running analysis on unvalidated datasets

---

## Interview-Ready Summary

If asked:

**“How did you handle data ingestion?”**

Answer:

> “I built a reproducible data ingestion pipeline using official NSE listings and Yahoo Finance, enforced strict OHLCV validation rules, and separated raw and processed data to ensure auditability.”

---

## Summary

Phase 1 establishes **trust in data**.

Without this foundation:
- Indicators fail silently
- Backtests lie
- Models overfit

This phase ensures the system starts on **solid ground**.
