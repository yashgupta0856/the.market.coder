"""
Phase 5.5 — Full-Universe VCP Metrics + Fundamental Data Ingestion

Runs after Phase 5 (final_stock_scores is available).

Two responsibilities:

A) FUNDAMENTALS — fetch comprehensive yfinance fundamental metrics for
   ALL vcp_candidate=True stocks (not just the sector-filtered ranked list).
   Results are stored in ``equity_fundamentals``.

B) TECHNICAL METRICS — pull the latest indicator row from ``equity_indicators``
   for every VCP candidate and derive the same scoring metrics that the ranked
   pipeline computes (trend_strength, momentum, volatility_tightness,
   stock_score, plus raw indicators ema_20, ema_50, roc_126, sma_50, etc.).
   All derived metrics are upserted back into ``vcp_candidates`` so that
   non-ranked VCP stocks have a complete analytical profile available via
   the API and dashboard watchlist.

C) BACK-FILL final_stock_scores — the fundamental_score / grade computed here
   is written back into final_stock_scores (keeping the ranked collection fresh).
"""

import math
from datetime import datetime

import pandas as pd

from data.raw.fundamental_fetcher import fetch_fundamentals
from models.fundamental_scoring import compute_fundamental_scores
from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo, upsert_df_to_mongo


# Indicator columns to pull from equity_indicators for VCP candidates
_INDICATOR_COLS = {
    "_id": 0,
    "symbol": 1,
    "close": 1,
    "sma_50": 1,
    "sma_200": 1,
    "ema_20": 1,
    "ema_50": 1,
    "roc_63": 1,
    "roc_126": 1,
    "atr_14": 1,
    "atr_100": 1,
    "std_20": 1,
    "std_100": 1,
    "reg_slope_50": 1,
    "reg_slope_100": 1,
    "range_compression": 1,
}


def _sanitize_records(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN / Inf floats with None so MongoDB and JSON don't choke."""
    records = df.to_dict("records")
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                rec[k] = None
    return pd.DataFrame(records)


def run_phase5_5():
    """
    Run Phase 5.5 — full-universe VCP metrics + fundamental ingestion.
    """

    # ── A) Determine scope: ALL vcp_candidate=True symbols ───────────────
    vcp_col = get_collection("vcp_candidates")
    symbols = vcp_col.distinct("symbol", {"vcp_candidate": True})

    if not symbols:
        print("Phase 5.5: No VCP candidates found — skipping")
        return

    print(f"Phase 5.5: processing {len(symbols)} VCP candidates "
          f"(was limited to ranked stocks only before this change)")

    # ── B) Fetch & score fundamentals for all VCP candidates ─────────────
    print("  Fetching fundamentals from yfinance...")
    raw_df = fetch_fundamentals(symbols, max_workers=min(len(symbols), 4))
    scored_df = compute_fundamental_scores(raw_df)

    # Convert Categorical grade to plain string before storage.
    # pd.cut() produces NaN for stocks whose fundamental_score is NaN.
    # astype(str) turns those NaNs into the literal string "nan", which
    # bypasses the float-NaN check in _sanitize_records.  Replace explicitly.
    if "fundamental_grade" in scored_df.columns:
        scored_df["fundamental_grade"] = (
            scored_df["fundamental_grade"]
            .astype(str)
            .replace("nan", None)
        )

    scored_df = _sanitize_records(scored_df)
    scored_df["fetched_at"] = datetime.utcnow()

    inserted = df_to_mongo(scored_df, "equity_fundamentals")
    get_collection("equity_fundamentals").create_index(
        [("symbol", 1)], background=True
    )
    print(f"  Stored {inserted} fundamental records in equity_fundamentals")

    # ── C) Pull latest indicator row for each VCP candidate ───────────────
    stocks_col = get_collection("equity_indicators")
    latest_doc = stocks_col.find_one({}, {"date": 1, "_id": 0}, sort=[("date", -1)])

    if not latest_doc:
        print("  equity_indicators is empty — skipping technical metric enrichment")
        return

    latest_date = latest_doc["date"]

    indicator_docs = list(
        stocks_col.find(
            {"symbol": {"$in": symbols}, "date": latest_date},
            _INDICATOR_COLS,
        )
    )

    if not indicator_docs:
        print("  No indicator data found for latest date — skipping enrichment")
        return

    ind_df = pd.DataFrame(indicator_docs)

    # ── D) Derive scoring metrics (same formula as compute_stock_score) ───
    ind_df["trend_strength"] = (
        (ind_df["close"] - ind_df["sma_200"]) / ind_df["sma_200"]
    )
    ind_df["momentum"] = ind_df["roc_63"]
    ind_df["volatility_tightness"] = 1 - (ind_df["atr_14"] / ind_df["close"])

    # Merge fundamental score so stock_score includes it
    fund_scores = scored_df[["symbol", "fundamental_score"]].copy()
    ind_df = ind_df.merge(fund_scores, on="symbol", how="left")

    fund_factor = ind_df["fundamental_score"].fillna(50) / 100
    ind_df["stock_score"] = (
        0.35 * ind_df["trend_strength"] +
        0.35 * ind_df["momentum"] +
        0.20 * ind_df["volatility_tightness"] +
        0.10 * fund_factor
    )

    # ── E) Merge fundamental grade then upsert into vcp_candidates ────────
    grade_df = scored_df[["symbol", "fundamental_grade"]].copy()
    enrich_df = ind_df.merge(grade_df, on="symbol", how="left")

    # Columns we want to write into vcp_candidates
    enrich_cols = [
        "symbol",
        # Raw indicators
        "ema_20", "ema_50",
        "roc_63", "roc_126",
        "atr_14", "atr_100",
        "sma_50", "sma_200",
        "std_20", "std_100",
        "reg_slope_50", "reg_slope_100",
        "range_compression",
        # Derived scoring
        "trend_strength", "momentum", "volatility_tightness", "stock_score",
        # Fundamentals
        "fundamental_score", "fundamental_grade",
    ]
    enrich_df = enrich_df[[c for c in enrich_cols if c in enrich_df.columns]]
    enrich_df = _sanitize_records(enrich_df)

    upsert_df_to_mongo(enrich_df, "vcp_candidates", key_fields=["symbol"])
    print(f"  Enriched {len(enrich_df)} vcp_candidates rows with technical + fundamental metrics")

    # ── F) Back-fill final_stock_scores with updated fundamental scores ───
    fund_col = get_collection("equity_fundamentals")
    final_col = get_collection("final_stock_scores")
    final_symbols = final_col.distinct("symbol")

    updated = 0
    for sym in final_symbols:
        fund_doc = fund_col.find_one(
            {"symbol": sym},
            {"_id": 0, "fundamental_score": 1, "fundamental_grade": 1},
        )
        if fund_doc:
            final_col.update_one(
                {"symbol": sym},
                {"$set": {
                    "fundamental_score": fund_doc.get("fundamental_score"),
                    "fundamental_grade": fund_doc.get("fundamental_grade"),
                }},
            )
            updated += 1

    print(f"  Back-filled fundamental scores for {updated} ranked stocks in final_stock_scores")
    print(f"Phase 5.5 completed.")


if __name__ == "__main__":
    run_phase5_5()
