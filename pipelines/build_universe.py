from data.raw.nse_universe import download_equity_universe
from data.processed.universe_cleaner import clean_equity_universe
from data.raw.yahoo_ohlcv import fetch_ohlcv
from utils.validation import validate_ohlcv
from configs.data_sources import DEFAULT_START_DATE
from data.reference.stock_with_sector import nse_sector_enrichment_to_csv
from utils.mongo_loader import csv_to_mongo
import pandas as pd


def run_phase1():
    # Step 1: Download & clean universe
    raw_df = download_equity_universe()
    universe_df = clean_equity_universe(raw_df)

    all_ohlcv = []

    # Step 2: Fetch OHLCV
    for symbol in universe_df["SYMBOL"]:
        df = fetch_ohlcv(symbol, DEFAULT_START_DATE)
        if validate_ohlcv(df):
            all_ohlcv.append(df)

    # Step 3: Save OHLCV
    if all_ohlcv:
        final_df = pd.concat(all_ohlcv, ignore_index=True)
        final_df.to_csv("data/processed/ohlcv_equities.csv", index=False)

        csv_to_mongo(
            "data/processed/ohlcv_equities.csv",
            "ohlcv_equities"
        )

    # Step 4: Save universe
    universe_df.to_csv("data/processed/equity_universe.csv", index=False)

    csv_to_mongo(
        "data/processed/equity_universe.csv",
        "equity_universe"
    )

    # Step 5: Sector enrichment
    nse_sector_enrichment_to_csv(
        output_csv="data/reference/stock_sector_mapping.csv"
    )

    csv_to_mongo(
        "data/reference/stock_sector_mapping.csv",
        "stock_sector_mapping"
    )


if __name__ == "__main__":
    run_phase1()
