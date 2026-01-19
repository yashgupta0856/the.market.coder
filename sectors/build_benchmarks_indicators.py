import pandas as pd

from sectors.sector_indicators import (
    fetch_sector_ohlcv,
    compute_sector_indicators
)

from configs.data_sources import DEFAULT_START_DATE
from utils.mongo_loader import csv_to_mongo


BENCHMARK_INDEX = "NSEI"
OUTPUT_PATH = "data/processed/benchmark_indicators.csv"


def run_benchmark_pipeline():
    # Fetch benchmark OHLCV
    ohlcv = fetch_sector_ohlcv(
        sector_index=BENCHMARK_INDEX,
        start=DEFAULT_START_DATE
    )

    # Compute indicators
    enriched = compute_sector_indicators(ohlcv)

    enriched["benchmark"] = "NIFTY50"

    enriched.to_csv(OUTPUT_PATH, index=False)

    csv_to_mongo(
        OUTPUT_PATH,
        "benchmark_indicators"
    )


if __name__ == "__main__":
    run_benchmark_pipeline()
