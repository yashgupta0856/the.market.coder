"""
Fundamental data fetcher — pulls comprehensive financial metrics from yfinance.

Covers data from three Yahoo Finance pages:
  • Key Statistics  — valuation, trading info
  • Financials      — income, balance sheet, cash flow
  • Analysis        — analyst ratings, earnings estimates

Uses Ticker.info to extract all available fields in a single API call.
"""

import time
import math
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from configs.data_sources import YAHOO_SUFFIX


# ── yfinance info key → our column name ──────────────────────────────

# Valuation
VALUATION_FIELDS = {
    "marketCap":                "market_cap",
    "enterpriseValue":          "enterprise_value",
    "trailingPE":               "trailing_pe",
    "forwardPE":                "forward_pe",
    "pegRatio":                 "peg_ratio",
    "priceToBook":              "price_to_book",
    "priceToSalesTrailing12Months": "price_to_sales",
    "enterpriseToEbitda":       "ev_to_ebitda",
    "enterpriseToRevenue":      "ev_to_revenue",
}

# Profitability & Earnings
PROFITABILITY_FIELDS = {
    "trailingEps":              "trailing_eps",
    "forwardEps":               "forward_eps",
    "epsCurrentYear":           "eps_current_year",
    "returnOnEquity":           "roe",
    "returnOnAssets":           "roa",
    "grossMargins":             "gross_margin",
    "operatingMargins":         "operating_margin",
    "profitMargins":            "profit_margin",
    "ebitdaMargins":            "ebitda_margin",
    "revenueGrowth":            "revenue_growth",
    "earningsGrowth":           "earnings_growth",
    "earningsQuarterlyGrowth":  "earnings_quarterly_growth",
}

# Financial Health
FINANCIAL_FIELDS = {
    "totalRevenue":             "total_revenue",
    "ebitda":                   "ebitda",
    "netIncomeToCommon":        "net_income",
    "freeCashflow":             "free_cashflow",
    "operatingCashflow":        "operating_cashflow",
    "totalCash":                "total_cash",
    "totalCashPerShare":        "cash_per_share",
    "totalDebt":                "total_debt",
    "debtToEquity":             "debt_to_equity",
    "currentRatio":             "current_ratio",
    "quickRatio":               "quick_ratio",
    "bookValue":                "book_value",
    "revenuePerShare":          "revenue_per_share",
}

# Trading & Analyst
TRADING_FIELDS = {
    "beta":                     "beta",
    "fiftyTwoWeekHigh":         "fifty_two_week_high",
    "fiftyTwoWeekLow":          "fifty_two_week_low",
    "fiftyDayAverage":          "fifty_day_avg",
    "twoHundredDayAverage":     "two_hundred_day_avg",
    "averageVolume":            "avg_volume",
    "dividendYield":            "dividend_yield",
    "payoutRatio":              "payout_ratio",
    "targetMeanPrice":          "analyst_target_mean",
    "targetHighPrice":          "analyst_target_high",
    "targetLowPrice":           "analyst_target_low",
    "recommendationKey":        "recommendation",
    "numberOfAnalystOpinions":  "analyst_count",
}

# Combined map for fetching
FUNDAMENTAL_FIELDS = {
    **VALUATION_FIELDS,
    **PROFITABILITY_FIELDS,
    **FINANCIAL_FIELDS,
    **TRADING_FIELDS,
}

MAX_RETRIES = 3


def _fetch_single(symbol: str) -> dict:
    """Fetch fundamentals for one symbol with retries + backoff."""
    import yfinance as yf

    logging.getLogger("yfinance").setLevel(logging.CRITICAL)

    record = {"symbol": symbol}

    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(f"{symbol}{YAHOO_SUFFIX}")
            info = ticker.info or {}

            # yfinance returns a minimal dict when rate-limited or
            # the symbol is invalid.  Check that at least ONE financial
            # key is present before accepting the result.
            has_data = any(info.get(k) is not None for k in FUNDAMENTAL_FIELDS)

            if not has_data:
                # Likely rate-limited — backoff and retry
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 * (attempt + 1))
                continue

            for yf_key, col_name in FUNDAMENTAL_FIELDS.items():
                val = info.get(yf_key)
                # Sanitize non-finite floats from yfinance
                if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    val = None
                record[col_name] = val

            # Also grab company name for display
            record["company_name"] = info.get("longName") or info.get("shortName")
            record["sector"] = info.get("sectorDisp") or info.get("sector")
            record["industry"] = info.get("industryDisp") or info.get("industry")

            return record

        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 * (attempt + 1))
            continue

    # All retries failed — return record with None values
    for col_name in FUNDAMENTAL_FIELDS.values():
        record.setdefault(col_name, None)

    return record


def fetch_fundamentals(
    symbols: list[str],
    max_workers: int = 4,
) -> pd.DataFrame:
    """
    Fetch fundamental data for a list of symbols in parallel.

    Returns a DataFrame with columns: symbol + all FUNDAMENTAL_FIELDS values.
    Never raises — failed symbols get None values.
    """
    results = []
    completed = 0
    effective_workers = min(max_workers, 4)

    print(
        f"Fetching fundamentals for {len(symbols)} symbols "
        f"({effective_workers} parallel threads)..."
    )

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {
            executor.submit(_fetch_single, sym): sym
            for sym in symbols
        }

        for future in as_completed(futures):
            completed += 1
            if completed % 50 == 0 or completed == len(symbols):
                print(f"  Fundamentals progress: {completed}/{len(symbols)}")
            results.append(future.result())

    df = pd.DataFrame(results)

    # Stats
    non_null_counts = df.drop(columns=["symbol"]).notna().sum()
    best_field = non_null_counts.idxmax()
    coverage = non_null_counts.max()

    print(
        f"Fundamentals fetched: {len(df)} symbols, "
        f"best coverage: {best_field} ({coverage}/{len(df)})"
    )

    return df
