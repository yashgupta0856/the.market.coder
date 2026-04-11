"""
Phase 1 — Universe + OHLCV Ingestion

Supports two modes:
  - incremental=True  (default): only fetches new data since last stored date,
                                  caches sector mapping, uses parallel threads.
  - incremental=False (full):     re-downloads everything from DEFAULT_START_DATE,
                                  identical to original behavior.

On an empty database, both modes behave identically.
"""

import pandas as pd
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from data.raw.nse_universe import download_equity_universe
from data.processed.universe_cleaner import clean_equity_universe
from data.raw.yahoo_ohlcv import fetch_ohlcv
from data.reference.stock_with_sector import nse_sector_enrichment_to_csv

from utils.validation import validate_ohlcv
from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo
from configs.data_sources import DEFAULT_START_DATE



# =========================================================
# HELPERS
# =========================================================


def get_last_dates():
    """
    Get the last stored OHLCV date per symbol from MongoDB.
    Returns empty dict if collection is empty.
    """
    col = get_collection("ohlcv_equities")

    if col.count_documents({}) == 0:
        return {}

    pipeline = [
        {"$group": {"_id": "$symbol", "last_date": {"$max": "$date"}}},
    ]

    return {
        doc["_id"]: pd.to_datetime(doc["last_date"])
        for doc in col.aggregate(pipeline)
    }


def fetch_symbol_ohlcv(symbol, start_date):
    """Fetch and validate OHLCV for a single symbol (thread-safe)."""
    try:
        df = fetch_ohlcv(symbol, start_date)
        if df is not None and validate_ohlcv(df):
            return df
    except Exception:
        pass
    return None


def ensure_ohlcv_index():
    """Create compound index on (symbol, date) for fast lookups."""
    col = get_collection("ohlcv_equities")
    col.create_index(
        [("symbol", 1), ("date", -1)],
        background=True
    )



# =========================================================
# PHASE 1 RUNNER
# =========================================================


def run_phase1(incremental=True, max_workers=10):
    """
    Run Phase 1 — Universe + OHLCV Ingestion.

    Args:
        incremental: If True, only fetch new data since last stored date.
                     If False, full re-download from DEFAULT_START_DATE.
        max_workers: Number of parallel threads for OHLCV fetching.
    """

    # ----- Universe download (always refresh, it's small) -----

    print("Downloading NSE equity universe...")
    raw_df = download_equity_universe()

    print("Cleaning equity universe...")
    universe_df = clean_equity_universe(raw_df)

    print(f"Universe size: {len(universe_df)} symbols")


    # ----- Determine start dates per symbol -----

    last_dates = {}

    if incremental:
        last_dates = get_last_dates()
        if last_dates:
            print(f"Incremental mode: found existing data for {len(last_dates)} symbols")


    # ----- Build fetch jobs -----

    fetch_jobs = []
    skipped = 0
    today = pd.Timestamp.now().normalize()

    for symbol in universe_df["SYMBOL"]:
        if incremental and symbol in last_dates:
            next_date = last_dates[symbol] + timedelta(days=1)
            if next_date >= today:
                skipped += 1
                continue
            start = next_date.strftime("%Y-%m-%d")
        else:
            start = DEFAULT_START_DATE
        fetch_jobs.append((symbol, start))

    if skipped:
        print(f"Skipped {skipped} symbols (already up to date)")


    # ----- Parallel OHLCV fetch -----

    all_ohlcv = []

    if fetch_jobs:
        print(f"Fetching OHLCV for {len(fetch_jobs)} symbols "
              f"({max_workers} parallel threads)...")

        completed = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_symbol_ohlcv, sym, start): sym
                for sym, start in fetch_jobs
            }

            for future in as_completed(futures):
                completed += 1
                if completed % 100 == 0:
                    print(f"  Progress: {completed}/{len(fetch_jobs)} "
                          f"({len(all_ohlcv)} valid)")

                df = future.result()
                if df is not None:
                    all_ohlcv.append(df)
                else:
                    failed += 1

        print(f"Fetch complete: {len(all_ohlcv)} valid, {failed} failed/empty")


    # ----- Store OHLCV -----

    if all_ohlcv:
        final_ohlcv = pd.concat(all_ohlcv, ignore_index=True)

        if incremental and last_dates:
            # Append mode: keep existing data, add new rows
            print(f"Appending {len(final_ohlcv)} new OHLCV rows to MongoDB...")
            df_to_mongo(final_ohlcv, "ohlcv_equities", clear_existing=False)
        else:
            # Full mode: replace entire collection
            print(f"Storing {len(final_ohlcv)} OHLCV rows in MongoDB...")
            df_to_mongo(final_ohlcv, "ohlcv_equities", clear_existing=True)
    else:
        if not last_dates:
            raise RuntimeError("No valid OHLCV data fetched.")
        print("All symbols already up to date — no new OHLCV data needed")


    # ----- Index for fast lookups -----

    ensure_ohlcv_index()


    # ----- Store universe (always refresh) -----

    print("Storing cleaned universe in MongoDB...")
    df_to_mongo(universe_df, "equity_universe")


    # ----- Sector Mapping Enrichment (cached if exists) -----

    sector_col = get_collection("stock_sector_mapping")

    if incremental and sector_col.count_documents({}) > 0:
        print("Using cached sector mapping from MongoDB (skipping enrichment)")
    else:
        print("Building sector mapping (this may take a while)...")
        mapping_df = nse_sector_enrichment_to_csv(output_csv=None)

        print("Storing sector mapping in MongoDB...")
        df_to_mongo(mapping_df, "stock_sector_mapping")

    print("Phase 1 completed successfully.")


if __name__ == "__main__":
    run_phase1()
