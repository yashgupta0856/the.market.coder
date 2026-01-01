import pandas as pd

from sectors.sector_indicators import (
    fetch_sector_ohlcv,
    compute_sector_indicators
)

from configs.data_sources import DEFAULT_START_DATE


BENCHMARK_INDEX = "NSEI"
OUTPUT_PATH = "data/processed/benchmark_indicators.csv"


def run_benchmark_pipeline():
    # Fetch benchmark OHLCV (same logic as sector)
    ohlcv = fetch_sector_ohlcv(
        sector_index=BENCHMARK_INDEX,
        start=DEFAULT_START_DATE
    )

    # Compute indicators using SAME engine
    enriched = compute_sector_indicators(ohlcv)

    # Rename column for clarity
    enriched["benchmark"] = "NIFTY50"

    enriched.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    run_benchmark_pipeline()
