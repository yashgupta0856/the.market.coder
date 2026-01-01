from data.raw.nse_universe import download_equity_universe
from data.processed.universe_cleaner import clean_equity_universe
from data.raw.yahoo_ohlcv import fetch_ohlcv
from utils.validation import validate_ohlcv
from configs.data_sources import DEFAULT_START_DATE
from data.reference.stock_with_sector import nse_sector_enrichment_to_csv
import pandas as pd

def run_phase1():
    raw_df = download_equity_universe()
    universe_df = clean_equity_universe(raw_df)

    all_ohlcv = []

    for symbol in universe_df["SYMBOL"]:
        df = fetch_ohlcv(symbol, DEFAULT_START_DATE)

        if validate_ohlcv(df):
            all_ohlcv.append(df)

    if all_ohlcv:
        final_df = pd.concat(all_ohlcv, ignore_index=True)
        final_df.to_csv("data/processed/ohlcv_equities.csv", index=False)

    universe_df.to_csv("data/processed/equity_universe.csv", index=False)


    nse_sector_enrichment_to_csv(
        output_csv="data/reference/stock_sector_mapping.csv"
    )


if __name__ == "__main__":
    run_phase1()