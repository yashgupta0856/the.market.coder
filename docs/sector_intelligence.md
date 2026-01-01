
# Phase 4 — Sector Intelligence & Sector Strength Modeling

## Purpose

Phase 4 introduces **cross-sectional market intelligence** into the system.
While earlier phases focus on individual stocks, Phase 4 evaluates **sector-level leadership** to ensure stock selection aligns with broader market forces.

This phase answers:

> “Which sectors are leading, improving, weakening, or lagging relative to the market?”

---

## Phase 4 Overview

Phase 4 is composed of four clearly separated sub-phases:

1. **Phase 4.1 — Sector Mapping**
2. **Phase 4.2 — Sector Indicator Computation**
3. **Phase 4.3 — Sector Relative Strength Modeling**
4. **Phase 4.4 — Sector Regime Classification**

Each sub-phase has a single responsibility and is orchestrated through a unified pipeline.

---

## Phase 4.1 — Sector Mapping (Reference Layer)

### Objective
Create a deterministic mapping between stocks and sectors.

### Key Characteristics
- Manual, reference-driven data
- No computation or inference
- Single source of truth

### Artifacts
```
data/reference/stock_sector_mapping.csv
sector_intelligence/sector_mapping.py
```

---

## Phase 4.2 — Sector Indicator Computation

### Objective
Compute technical indicators for **sector indices**, treating them as macro instruments.

### Output
```
data/processed/sector_indicators.csv
```

---

## Phase 4.3 — Sector Relative Strength Modeling

### Objective
Measure **relative performance** of sectors versus the benchmark (NIFTY 50).

### Output
```
data/processed/sector_strength.csv
```

---

## Phase 4.4 — Sector Regime Classification

### Objective
Convert sector strength scores into discrete regimes.

### Output
```
data/processed/sector_regime.csv
```

---

## Pipeline

```
pipelines/build_phase4_sector_intelligence.py
```

---

## Summary

Phase 4 adds macro-level context and ensures stock selection aligns with sector leadership.
