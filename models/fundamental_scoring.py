"""
Fundamental health scoring model.

Produces a composite score (0–100) and letter grade (A–F) from raw
fundamental metrics.  Uses percentile-rank normalisation so the score
is always relative to the current universe, not hard-coded thresholds.
"""

import numpy as np
import pandas as pd


# ── Weights ───────────────────────────────────────────────────────────
WEIGHTS = {
    "earnings":      0.25,
    "profitability": 0.25,
    "growth":        0.20,
    "leverage":      0.15,
    "valuation":     0.15,
}


# ── Helpers ───────────────────────────────────────────────────────────

def _pct_rank(series: pd.Series) -> pd.Series:
    """Percentile rank (0–1) within the series; NaN stays NaN."""
    return series.rank(pct=True, method="average")


def _inverse_pct_rank(series: pd.Series) -> pd.Series:
    """Lower raw value = higher rank (good for P/E, debt)."""
    return 1 - series.rank(pct=True, method="average")


# ── Scoring ───────────────────────────────────────────────────────────

def compute_fundamental_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute fundamental health scores for each stock.

    Parameters
    ----------
    df : DataFrame
        Must contain column ``symbol`` plus any available fundamental
        columns (market_cap, trailing_pe, roe, etc.).

    Returns
    -------
    DataFrame
        Original columns plus:
        - ``f_earnings``, ``f_profitability``, ``f_growth``,
          ``f_leverage``, ``f_valuation`` — pillar scores (0–1)
        - ``fundamental_score``  — composite (0–100)
        - ``fundamental_grade``  — letter grade A / B / C / D / F
    """
    scored = df.copy()

    # yfinance sometimes returns strings (e.g. "Infinity", locale-formatted
    # numbers). Coerce every numeric column to float; unparseable → NaN.
    numeric_cols = [
        "trailing_pe", "forward_pe", "trailing_eps", "forward_eps",
        "eps_current_year",
        "roe", "roa", "debt_to_equity", "revenue_growth",
        "earnings_growth", "earnings_quarterly_growth",
        "gross_margin", "operating_margin", "profit_margin",
        "ebitda_margin",
        "dividend_yield", "book_value", "current_ratio", "quick_ratio",
        "market_cap", "enterprise_value",
        "peg_ratio", "price_to_book", "price_to_sales",
        "ev_to_ebitda", "ev_to_revenue",
        "beta", "total_revenue", "ebitda", "net_income",
        "free_cashflow", "operating_cashflow",
        "total_cash", "total_debt",
    ]
    for col in numeric_cols:
        if col in scored.columns:
            scored[col] = pd.to_numeric(scored[col], errors="coerce")

    # ── Earnings quality (25%) ────────────────────────────────────
    #   Positive EPS is good; higher EPS growth is better.
    trailing_eps = scored.get("trailing_eps", pd.Series(dtype=float))
    forward_eps = scored.get("forward_eps", pd.Series(dtype=float))

    eps_positive = (trailing_eps.fillna(0) > 0).astype(float)

    eps_growth = np.where(
        trailing_eps.notna() & (trailing_eps != 0),
        (forward_eps.fillna(0) - trailing_eps) / trailing_eps.abs(),
        0,
    )
    eps_growth_rank = _pct_rank(pd.Series(eps_growth, index=scored.index))

    scored["f_earnings"] = 0.5 * eps_positive + 0.5 * eps_growth_rank.fillna(0.5)

    # ── Profitability (25%) ───────────────────────────────────────
    roe_rank = _pct_rank(scored.get("roe", pd.Series(dtype=float))).fillna(0.5)
    margin_col = "operating_margin" if "operating_margin" in scored.columns else None
    margin_rank = _pct_rank(scored[margin_col]).fillna(0.5) if margin_col else pd.Series(0.5, index=scored.index)
    scored["f_profitability"] = 0.6 * roe_rank + 0.4 * margin_rank

    # ── Growth (20%) ──────────────────────────────────────────────
    growth_col = "revenue_growth" if "revenue_growth" in scored.columns else None
    scored["f_growth"] = _pct_rank(scored[growth_col]).fillna(0.5) if growth_col else 0.5

    # ── Leverage (15%) — lower debt = better ──────────────────────
    de_col = "debt_to_equity" if "debt_to_equity" in scored.columns else None
    scored["f_leverage"] = _inverse_pct_rank(scored[de_col]).fillna(0.5) if de_col else 0.5

    # ── Valuation (15%) — lower P/E = better ─────────────────────
    #   Only use positive P/E; negative P/E gets 0.5 (neutral).
    if "trailing_pe" in scored.columns:
        pe_series = scored["trailing_pe"].copy()
        pe_series = pe_series.where(pe_series > 0)
        scored["f_valuation"] = _inverse_pct_rank(pe_series).fillna(0.5)
    else:
        scored["f_valuation"] = 0.5

    # ── Composite ─────────────────────────────────────────────────
    scored["fundamental_score"] = (
        WEIGHTS["earnings"]      * scored["f_earnings"]
        + WEIGHTS["profitability"] * scored["f_profitability"]
        + WEIGHTS["growth"]        * scored["f_growth"]
        + WEIGHTS["leverage"]      * scored["f_leverage"]
        + WEIGHTS["valuation"]     * scored["f_valuation"]
    ) * 100  # scale to 0–100

    scored["fundamental_score"] = scored["fundamental_score"].round(2)

    # ── Letter grade (quintile-based) ─────────────────────────────
    scored["fundamental_grade"] = pd.cut(
        scored["fundamental_score"],
        bins=[-0.01, 20, 40, 60, 80, 100.01],
        labels=["F", "D", "C", "B", "A"],
    )

    return scored
