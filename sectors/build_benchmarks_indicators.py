import pandas as pd

from sectors.sector_indicators import (
    fetch_sector_ohlcv,
    compute_sector_indicators
)

from configs.data_sources import DEFAULT_START_DATE
from utils.mongo import get_collection


BENCHMARK_INDEX = "NSEI"


def df_to_mongo(df: pd.DataFrame, collection_name: str):
    """
    Direct DataFrame → MongoDB insert.
    Replaces old csv_to_mongo().
    """

    if df.empty:
        return 0

    col = get_collection(collection_name)

    # Clear old benchmark data
    col.delete_many({})

    records = df.to_dict(orient="records")

    col.insert_many(records)

    return len(records)


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
