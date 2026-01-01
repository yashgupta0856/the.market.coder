import pandas as pd

from sectors.sector_indicators import (
    fetch_sector_ohlcv,
    compute_sector_indicators
)

from sectors.sector_strength import (
    load_sector_and_benchmark,
    compute_raw_rs,
    normalize_and_score
)

from sectors.sector_regime import classify_sector_regimes
from sectors.build_benchmarks_indicators import run_benchmark_pipeline

from configs.data_sources import DEFAULT_START_DATE


# CONFIG

SECTORS = [
    "CNXAUTO", "CNXIT", "CNXFMCG", "CNXFIN",
    "CNXPHARMA", "CNXMETAL", "CNXENERGY",
    "CNXINFRA", "CNXREALTY", "CNXMEDIA",
    "CNXPSUBANK", "CNXSERVICE"
]

SECTOR_INDICATORS_PATH = "data/processed/sector_indicators.csv"
SECTOR_STRENGTH_PATH   = "data/processed/sector_strength.csv"
SECTOR_REGIME_PATH     = "data/processed/sector_regime.csv"
BENCHMARK_PATH         = "data/processed/benchmark_indicators.csv"



# PHASE 4.2 — Sector Indicators


def run_phase4_2():
    frames = []

    for sector in SECTORS:
        ohlcv = fetch_sector_ohlcv(sector, start=DEFAULT_START_DATE)
        enriched = compute_sector_indicators(ohlcv)
        frames.append(enriched)

    final_df = pd.concat(frames, ignore_index=True)
    final_df.to_csv(SECTOR_INDICATORS_PATH, index=False)

    return final_df



# PHASE 4.3 — Sector Strength


def run_phase4_3():
    sector_latest, benchmark_latest = load_sector_and_benchmark(
        SECTOR_INDICATORS_PATH,
        BENCHMARK_PATH
    )

    rs_raw   = compute_raw_rs(sector_latest, benchmark_latest)
    rs_final = normalize_and_score(rs_raw)

    rs_final.to_csv(SECTOR_STRENGTH_PATH, index=False)
    return rs_final



# PHASE 4 — MASTER PIPELINE


def run_phase4():
    print("Running Benchmark Indicator Pipeline")
    run_benchmark_pipeline()

    print("Running Phase 4.2 — Sector Indicator Computation")
    run_phase4_2()

    print("Running Phase 4.3 — Sector Relative Strength")
    rs_df = run_phase4_3()

    print("Running Phase 4.4 — Sector Regime Classification")
    regime_df = classify_sector_regimes(rs_df)
    regime_df.to_csv(SECTOR_REGIME_PATH, index=False)

    print("Phase 4 completed successfully")


if __name__ == "__main__":
    run_phase4()