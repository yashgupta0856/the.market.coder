import pandas as pd

from analytics.monte_carlo import run_monte_carlo
from utils.mongo import get_collection

FINAL_STOCKS_PATH = "output/final_stock_scores.csv"


def build_monte_carlo():
    # ranked symbols still come from CSV
    ranked = pd.read_csv(FINAL_STOCKS_PATH)

    # MongoDB collections
    price_col = get_collection("ohlcv_equities")   # <-- your existing price data
    mc_col = get_collection("monte_carlo")

    # clean old Monte Carlo results
    mc_col.delete_many({})

    docs = []

    for symbol in ranked["symbol"].unique():
        # fetch price data for symbol from MongoDB
        cursor = price_col.find(
            {"symbol": symbol},
            {"_id": 0, "symbol": 1, "date": 1, "close": 1}
        )

        data = list(cursor)
        if len(data) < 60:
            continue

        df = pd.DataFrame(data)

        # ensure proper types
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

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
