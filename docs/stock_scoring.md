
# Phase 5 — Stock–Sector Fusion, Filtering & Final Stock Ranking

## Purpose

Phase 5 is the **decision layer** of the system.
It combines **stock-level structure**, **sector intelligence**, and **ranking logic** to produce a **focused, actionable stock list**.

This phase answers:

> “Among structurally valid stocks, which ones deserve priority right now?”

---

## Phase 5 Overview

Phase 5 is deliberately split into **three sub-phases**, each with a single responsibility:

1. **Phase 5.1 — Stock–Sector Fusion**
2. **Phase 5.2 — VCP-only & Sector-Regime Filtering**
3. **Phase 5.3 — Final Stock Scoring & Ranking**

---

## Phase 5.1 — Stock–Sector Fusion

### Objective
Attach **sector metadata** to each stock without applying any business logic.

### Inputs
```
data/processed/indicators/equity_indicators.csv
data/processed/vcp_candidates.csv
data/processed/sector_regime.csv
data/reference/stock_sector_mapping.csv
```

### Output
```
data/processed/stock_sector_fused.csv
```

---

## Phase 5.2 — VCP-only & Sector-Regime Filtering

### Objective
Reduce the universe to **only high-quality candidates** before scoring.

### Filters Applied
- `vcp_candidate == True`
- `sector_regime ∈ {LEADING, IMPROVING}`

### Output
```
data/processed/vcp_sector_filtered.csv
```

---

## Phase 5.3 — Final Stock Scoring & Ranking

### Objective
Rank the filtered stocks using a transparent, explainable composite score.

### Output
```
outputs/final_stock_scores.csv
```

---

## Scoring Components

- Trend Strength
- Momentum
- Volatility Tightness
- Structure Bonus (VCP)

Weights are deliberately conservative and interpretable.

---

## Testing Philosophy

Phase 5 emphasizes:
- Invariant testing
- Deterministic pipelines
- Economic sanity checks

---

## Interview-Ready Summary

> “I implemented a multi-stage, sector-aware stock ranking system that filters structural patterns first and ranks candidates using an explainable composite score.”

---

## Summary

Phase 5 converts analysis into decision-ready output and completes the research layer of the system.
