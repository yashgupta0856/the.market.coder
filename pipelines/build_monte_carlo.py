import pandas as pd

from analytics.monte_carlo import run_monte_carlo
from utils.mongo import get_collection


def build_monte_carlo():

    # ===============================
    # 1️⃣ LOAD RANKED STOCKS FROM MONGO
    # ===============================

    ranked_col = get_collection("final_stock_scores")
    price_col = get_collection("ohlcv_equities")
    mc_col = get_collection("monte_carlo")

    ranked_docs = list(
        ranked_col.find({}, {"_id": 0, "symbol": 1})
    )

    if not ranked_docs:
        raise RuntimeError(
            "final_stock_scores collection is empty. "
            "Run Phase 5 pipeline first."
        )

    symbols = list({doc["symbol"] for doc in ranked_docs})

    # ===============================
    # 2️⃣ CLEAR OLD MONTE CARLO DATA
    # ===============================

    mc_col.delete_many({})

    docs = []


    for symbol in symbols:

        cursor = price_col.find(
            {"symbol": symbol},
            {"_id": 0, "date": 1, "close": 1}
        )

        data = list(cursor)

        if len(data) < 60:
            continue

        df = pd.DataFrame(data)

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

    print(
        f"Monte Carlo simulations stored in MongoDB "
        f"({len(docs)} symbols)."
    )


if __name__ == "__main__":
    build_monte_carlo()
