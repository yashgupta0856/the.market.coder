"""
Phase 5.5 — VCP Fundamental Data Ingestion

Fetches comprehensive fundamental metrics ONLY for VCP ranked stocks
(from ``final_stock_scores``) and stores them in ``equity_fundamentals``.

This runs after Phase 5 scoring, so we know exactly which stocks are
VCP-qualified.  Fetching for ~3-50 stocks instead of 2000+ is fast
and avoids yfinance rate-limiting.
"""

import math
from datetime import datetime

from data.raw.fundamental_fetcher import fetch_fundamentals
from models.fundamental_scoring import compute_fundamental_scores
from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo


def run_phase5_5():
    """
    Run Phase 5.5 — Fetch fundamentals for VCP ranked stocks only.
    """
    scores_col = get_collection("final_stock_scores")
    symbols = scores_col.distinct("symbol")

    if not symbols:
        print("Phase 5.5: No VCP ranked stocks found — skipping fundamentals")
        return

    print(f"Phase 5.5: fetching fundamentals for {len(symbols)} VCP stocks...")

    # Fetch rich fundamentals (40+ fields per stock)
    raw_df = fetch_fundamentals(symbols, max_workers=min(len(symbols), 4))

    # Compute fundamental scores
    scored_df = compute_fundamental_scores(raw_df)

    # Sanitize NaN/Inf before MongoDB storage — replace with None
    import math
    records = scored_df.to_dict("records")
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                rec[k] = None
    import pandas as pd
    scored_df = pd.DataFrame(records)

    # Add timestamp
    scored_df["fetched_at"] = datetime.utcnow()

    # Store in MongoDB (replaces old data)
    inserted = df_to_mongo(scored_df, "equity_fundamentals")

    # Create index for fast lookups
    col = get_collection("equity_fundamentals")
    col.create_index([("symbol", 1)], background=True)

    # Also update final_stock_scores with fundamental_score and grade
    fund_col = get_collection("equity_fundamentals")
    final_col = get_collection("final_stock_scores")

    for sym in symbols:
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

    print(f"Phase 5.5 completed: {inserted} fundamental records stored.")


if __name__ == "__main__":
    run_phase5_5()
