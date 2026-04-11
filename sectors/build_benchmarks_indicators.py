import pandas as pd

from sectors.sector_indicators import (
    fetch_sector_ohlcv,
    compute_sector_indicators
)

from configs.data_sources import DEFAULT_START_DATE
from utils.mongo_writer import df_to_mongo


BENCHMARK_INDEX = "NSEI"


def run_benchmark_pipeline():

    # Fetch benchmark OHLCV
    ohlcv = fetch_sector_ohlcv(
        sector_index=BENCHMARK_INDEX,
        start=DEFAULT_START_DATE
    )

    if ohlcv is None or ohlcv.empty:
        raise RuntimeError("Benchmark OHLCV data is empty")

    # Compute indicators
    enriched = compute_sector_indicators(ohlcv)

    enriched["benchmark"] = "NIFTY50"

    # Store directly in MongoDB
    inserted = df_to_mongo(
        enriched,
        "benchmark_indicators"
    )

    print(f"Benchmark indicators stored in MongoDB ({inserted} records).")


if __name__ == "__main__":
    run_benchmark_pipeline()
