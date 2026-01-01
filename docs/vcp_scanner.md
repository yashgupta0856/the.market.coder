
# Phase 3 — VCP Scanner (Volatility Contraction Pattern)

## Purpose

Phase 3 implements a **deterministic, institutional-grade VCP scanner**.  
This phase is responsible for identifying **structural VCP candidates**, not breakouts or trades.

The output of this phase answers one question only:

> “Does this stock currently exhibit a valid Volatility Contraction Pattern structure?”

---

## What Phase 3 Does

- Applies **rule-based market structure logic**
- Evaluates stocks **independently**
- Uses only **latest available data**
- Produces a **binary classification** (VCP candidate: True / False)

This phase does **not** rank, score, or predict performance.

---

## Inputs

Consumed from Phase 2:

```
data/processed/indicators/equity_indicators.csv
```

Required columns include:
- Price: `close`, `high`, `low`
- Trend: `sma_50`, `sma_200`, `reg_slope_100`
- Volatility: `atr_14`, `atr_100`, `range_compression`
- Volume: `volume`

---

## Core Design Principle — Gated Architecture

VCP detection is implemented as **ordered gates**.  
Each gate has a **single responsibility** and can independently reject a stock.

Order matters.

---

## Gate 0 — Data Sufficiency Gate

**Purpose**
- Ensure enough historical data exists
- Prevent silent NaN-driven rejections

**Logic**
- Latest row must have non-NaN values for all required indicators

Stocks failing this gate are excluded immediately.

---

## Gate 1 — Trend Gate

**Purpose**
- Confirm a healthy primary uptrend

**Rules**
- Close > SMA(50)
- Close > SMA(200)
- SMA(50) > SMA(200)
- Long-term regression slope ≥ ~0
- Price within top 20% of 52-week range (soft filter)

This ensures VCP is treated strictly as a **continuation pattern**.

---

## Gate 2 — Volatility Contraction Gate (Hybrid)

**Purpose**
- Detect tightening volatility

**Hybrid Logic**
- Short-term contraction:
  - ATR(14) < ATR(14) from ~10 days ago
- OR
- Long-term contraction:
  - ATR(14) / ATR(100) < 0.95

Additional requirement:
- Range compression < 1.1

This aligns partially with popular scanners while preserving institutional discipline.

---

## Gate 3 — Price Tightness Gate

**Purpose**
- Ensure calm, controlled price behavior near the right edge

**Rules**
- Close range (recent window) < ~6%
- No wide-range candles (> ~5%) in recent window

This filters out loose consolidations and unstable bases.

---

## Gate 4 — Volume Dry-Up Gate (Optional)

**Purpose**
- Confirm absence of supply during contraction

**Rules**
- Average recent volume ≤ prior volume baseline
- No high-volume distribution days

This gate can be toggled on/off depending on desired strictness.

---

## Output

Phase 3 produces:

```
data/processed/vcp_candidates.csv
```

Schema:
```
symbol | vcp_candidate
```

Each symbol is evaluated **once**, using its most recent data.

---

## What Phase 3 Does NOT Do

- No breakout detection
- No scoring or ranking
- No sector filtering
- No probability estimation
- No backtesting

All of the above are handled in later phases.

---

## Testing Philosophy

- Each gate has dedicated unit tests
- Gates are tested in isolation
- Debug mode allows gate-level failure attribution
- Pipeline testing is limited to smoke tests

This ensures **auditability and debuggability**.

---

## Why This Architecture Matters

Most retail scanners:
- Combine all logic into one condition
- Provide no explanation for failures
- Are impossible to debug or extend

This VCP scanner:
- Is explainable
- Is tunable
- Mirrors institutional research workflows

---

## Interview-Ready Summary

If asked:

**“How did you implement VCP detection?”**

Answer:

> “I implemented VCP as a gated structural classifier that independently validates trend alignment, volatility contraction, price tightness, and volume behavior, producing a deterministic binary signal without look-ahead bias.”

---

## Summary

Phase 3 converts **raw indicators into market structure intelligence**.

It is the first phase where the system begins to express **alpha logic**, and it does so in a controlled, professional, and extensible manner.
