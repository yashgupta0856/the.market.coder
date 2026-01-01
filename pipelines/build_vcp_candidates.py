import pandas as pd
from scanners.vcp_scanner import scan_universe


INPUT_PATH = "data/processed/indicators/equity_indicators.csv"
OUTPUT_PATH = "data/processed/vcp_candidates.csv"


def run_phase3():
    df = pd.read_csv(INPUT_PATH, parse_dates=["date"])

    results = scan_universe(df)

    results.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    run_phase3()
