import pandas as pd

from data.raw.nse_universe import download_equity_universe
from data.processed.universe_cleaner import clean_equity_universe
from data.raw.yahoo_ohlcv import fetch_ohlcv
from data.reference.stock_with_sector import nse_sector_enrichment_to_csv

from utils.validation import validate_ohlcv
from utils.mongo import get_collection
from configs.data_sources import DEFAULT_START_DATE



# Mongo Writer Helper


def df_to_mongo(df, collection_name, clear_existing=True, batch_size=1000):
    if df is None or df.empty:
        return 0

    col = get_collection(collection_name)

    if clear_existing:
        col.delete_many({})

    records = df.to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        col.insert_many(records[i:i + batch_size])

    return len(records)



# Phase 1 — Universe + OHLCV Ingestion


def run_phase1():

    print("Downloading NSE equity universe...")
    raw_df = download_equity_universe()

    print("Cleaning equity universe...")
    universe_df = clean_equity_universe(raw_df)

    print(f"Universe size: {len(universe_df)} symbols")

    all_ohlcv = []

    print("Fetching OHLCV data from Yahoo Finance...")

    for symbol in universe_df["SYMBOL"]:
        try:
            df = fetch_ohlcv(symbol, DEFAULT_START_DATE)

            if df is not None and validate_ohlcv(df):
                all_ohlcv.append(df)

        except Exception:
            continue  # skip failing symbols safely

    if not all_ohlcv:
        raise RuntimeError("No valid OHLCV data fetched.")

    final_ohlcv = pd.concat(all_ohlcv, ignore_index=True)

    print("Storing OHLCV data in MongoDB...")
    df_to_mongo(final_ohlcv, "ohlcv_equities")

    print("Storing cleaned universe in MongoDB...")
    df_to_mongo(universe_df, "equity_universe")

    
    # Sector Mapping Enrichment
    

    print("Building sector mapping...")

    mapping_df = nse_sector_enrichment_to_csv(
        output_csv=None  # we ignore CSV writing
    )

    print("Storing sector mapping in MongoDB...")
    df_to_mongo(mapping_df, "stock_sector_mapping")

    print("Phase 1 completed successfully.")


if __name__ == "__main__":
    run_phase1()
