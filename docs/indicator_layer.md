
# Indicator Layer Documentation

## Purpose
The Indicator Layer is the **foundational mathematical engine** of the system.  
It converts raw OHLCV time-series data into **bias-free, reusable quantitative metrics**.

This layer contains **no trading logic**, **no thresholds**, and **no market interpretation**.

---

## Design Principles

- **Pure functions only**
  - Same input → same output
  - No side effects
- **No look-ahead bias**
  - All calculations are strictly backward-looking
- **Stateless**
  - No shared state across symbols or time
- **Reusable**
  - Indicators can be used by scanners, sector logic, and ML models
- **Fully unit-tested**
  - Each indicator has dedicated tests

---

## Indicators Implemented

### Moving Averages (`indicators/moving_averages.py`)
- Simple Moving Average (SMA)
- Exponential Moving Average (EMA)

Use cases:
- Trend structure
- Price positioning
- Moving average stacks

---

### Volatility (`indicators/volatility.py`)
- True Range (TR)
- Average True Range (ATR)
- Rolling Standard Deviation
- Range Compression Ratio

Use cases:
- Volatility contraction analysis
- Regime detection
- Risk normalization

---

### Momentum (`indicators/momentum.py`)
- Rate of Change (ROC)

Use cases:
- Medium-term momentum measurement
- Sector acceleration detection

---

### Trend (`indicators/trend.py`)
- Linear Regression Slope

Use cases:
- Trend persistence
- Institutional trend strength

---

## What This Layer Does NOT Do

- No VCP detection
- No breakout logic
- No scoring or ranking
- No sector comparisons
- No machine learning
- No file I/O

All interpretation happens in **higher layers**.

---

## Why This Architecture Matters

Separating indicators from strategy logic:
- Prevents strategy leakage
- Avoids hidden biases
- Improves backtest validity
- Enables institutional-grade explainability

This design mirrors how **professional quant research systems** are structured.

---

## Testing Philosophy

- One test file per indicator group
- Tests validate:
  - Correct math
  - NaN behavior
  - Edge cases
- No tests write files or call external APIs

---

## Summary

The Indicator Layer is the **bedrock** of the project.  
Every scanner, sector model, and ML component depends on its correctness.

If indicators are wrong → everything above is wrong.

