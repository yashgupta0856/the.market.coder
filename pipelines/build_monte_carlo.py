import pandas as pd

from analytics.monte_carlo import run_monte_carlo
from utils.mongo import get_collection   # assumes you already created this

FINAL_STOCKS_PATH = "output/final_stock_scores.csv"
PRICE_DATA_PATH = "data/processed/ohlcv_equities.csv"


def build_monte_carlo():
    ranked = pd.read_csv(FINAL_STOCKS_PATH)
    prices = pd.read_csv(PRICE_DATA_PATH, parse_dates=["date"])

    mc_col = get_collection("monte_carlo")

    # clean old data (important to avoid duplicates)
    mc_col.delete_many({})

    docs = []

    for symbol in ranked["symbol"].unique():
        df = prices[prices["symbol"] == symbol].sort_values("date")

        if len(df) < 60:
            continue

        result = run_monte_carlo(df["close"])

        docs.append({
            "symbol": symbol,
            "metrics": result["metrics"],
            "paths": result["paths"]
        })

    if docs:
        mc_col.insert_many(docs)

    print(f"Monte Carlo simulations stored in MongoDB ({len(docs)} symbols).")


if __name__ == "__main__":
    build_monte_carlo()
