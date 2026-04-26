"""
Phase 1.5 — Fundamental Data Ingestion

Fetches fundamental metrics for every symbol in the equity universe and
stores them in the ``equity_fundamentals`` MongoDB collection.

Fundamentals are quarterly data, so we cache aggressively:
  - Incremental mode skips if data is < 7 days old.
  - Full mode always re-fetches.
"""

import pandas as pd
from datetime import datetime, timedelta

from data.raw.fundamental_fetcher import fetch_fundamentals
from models.fundamental_scoring import compute_fundamental_scores
from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo


CACHE_DAYS = 7  # refresh fundamentals weekly


def _is_cache_fresh() -> bool:
    """Check if equity_fundamentals was refreshed within CACHE_DAYS."""
    col = get_collection("equity_fundamentals")

    sample = col.find_one({}, {"_id": 0, "fetched_at": 1})
    if not sample or "fetched_at" not in sample:
        return False

    fetched_at = pd.to_datetime(sample["fetched_at"])
    return (datetime.utcnow() - fetched_at) < timedelta(days=CACHE_DAYS)


def run_phase1_5(incremental: bool = True):
    """
    Run Phase 1.5 — Fundamental Data Ingestion + Scoring.

    Args:
        incremental: If True, skip fetch when cached data is fresh.
    """
    if incremental and _is_cache_fresh():
        print("Fundamentals cache is fresh — skipping Phase 1.5")
        return

    # Load universe symbols
    universe_col = get_collection("equity_universe")
    symbols = universe_col.distinct("SYMBOL")

    if not symbols:
        raise RuntimeError(
            "equity_universe is empty. Run Phase 1 first."
        )

    print(f"Phase 1.5: fetching fundamentals for {len(symbols)} symbols...")

    # Fetch raw fundamentals
    raw_df = fetch_fundamentals(symbols, max_workers=4)

    # Compute fundamental scores
    scored_df = compute_fundamental_scores(raw_df)

    # Add timestamp for cache freshness check
    scored_df["fetched_at"] = datetime.utcnow()

    # Store in MongoDB
    inserted = df_to_mongo(scored_df, "equity_fundamentals")

    # Create index for fast lookups
    col = get_collection("equity_fundamentals")
    col.create_index([("symbol", 1)], background=True)

    print(f"Phase 1.5 completed: {inserted} fundamental records stored.")


if __name__ == "__main__":
    run_phase1_5(incremental=False)
