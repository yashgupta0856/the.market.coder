import json
import pandas as pd
from pathlib import Path

from analytics.monte_carlo import run_monte_carlo

FINAL_STOCKS_PATH = "output/final_stock_scores.csv"
PRICE_DATA_PATH = "data/processed/ohlcv_equities.csv"
OUTPUT_PATH = "data/processed/monte_carlo.json"


def build_monte_carlo():
    ranked = pd.read_csv(FINAL_STOCKS_PATH)
    prices = pd.read_csv(PRICE_DATA_PATH, parse_dates=["date"])

    output = {}

    for symbol in ranked["symbol"].unique():
        df = prices[prices["symbol"] == symbol].sort_values("date")

        if len(df) < 60:
            continue

        output[symbol] = run_monte_carlo(df["close"])

    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print("Monte Carlo simulations generated.")


if __name__ == "__main__":
    build_monte_carlo()