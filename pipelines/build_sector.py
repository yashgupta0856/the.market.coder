import pandas as pd

from sectors.sector_indicators import (
    fetch_sector_ohlcv,
    compute_sector_indicators
)

from sectors.sector_strength import (
    compute_raw_rs,
    normalize_and_score
)

from sectors.sector_regime import classify_sector_regimes
from sectors.build_benchmarks_indicators import run_benchmark_pipeline

from configs.data_sources import DEFAULT_START_DATE
from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo


SECTORS = [
    "CNXAUTO", "CNXIT", "CNXFMCG", "CNXFIN",
    "CNXPHARMA", "CNXMETAL", "CNXENERGY",
    "CNXINFRA", "CNXREALTY", "CNXMEDIA",
    "CNXPSUBANK", "CNXSERVICE"
]


def run_phase4():
    print("Running Benchmark Pipeline")
    run_benchmark_pipeline()

    #  Sector Indicators 
    frames = []

    for sector in SECTORS:
        ohlcv = fetch_sector_ohlcv(sector, start=DEFAULT_START_DATE)
        enriched = compute_sector_indicators(ohlcv)
        frames.append(enriched)

    sector_df = pd.concat(frames, ignore_index=True)

    # Drop non-trading-day rows (yfinance returns date with NaN close on weekends/holidays)
    sector_df = sector_df.dropna(subset=["close"])

    df_to_mongo(sector_df, "sector_indicators")

    #  Sector Strength 
    latest_date = sector_df["date"].max()
    sector_latest = sector_df[sector_df["date"] == latest_date]

    benchmark_col = get_collection("benchmark_indicators")
    benchmark_df = pd.DataFrame(
        list(benchmark_col.find({}, {"_id": 0}))
    )

    benchmark_df["date"] = pd.to_datetime(benchmark_df["date"])
    benchmark_df = benchmark_df.dropna(subset=["close"])
    benchmark_latest = benchmark_df[
        benchmark_df["date"] == latest_date
    ]

    rs_raw = compute_raw_rs(sector_latest, benchmark_latest)
    rs_final = normalize_and_score(rs_raw)

    df_to_mongo(rs_final, "sector_strength")

    #  Sector Regime 
    regime_df = classify_sector_regimes(rs_final)
    df_to_mongo(regime_df, "sector_regime")

    print("Phase 4 completed successfully")


if __name__ == "__main__":
    run_phase4()
