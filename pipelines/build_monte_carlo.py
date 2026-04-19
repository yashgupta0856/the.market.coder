import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from analytics.monte_carlo import run_monte_carlo
from utils.mongo import get_collection


def build_monte_carlo(max_workers=8):

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
    # 2️⃣ BATCH FETCH ALL PRICE DATA
    # ===============================

    all_prices = pd.DataFrame(
        list(price_col.find(
            {"symbol": {"$in": symbols}},
            {"_id": 0, "date": 1, "close": 1, "symbol": 1}
        ))
    )

    if all_prices.empty:
        print("No price data found for ranked symbols.")
        return

    all_prices["date"] = pd.to_datetime(all_prices["date"])
    all_prices = all_prices.sort_values(["symbol", "date"])
    price_frames = {
        symbol: symbol_df
        for symbol, symbol_df in all_prices.groupby("symbol")
    }

    # ===============================
    # 3️⃣ PARALLEL MONTE CARLO
    # ===============================

    mc_col.delete_many({})

    def _run_mc(symbol):
        symbol_prices = price_frames.get(symbol)
        if symbol_prices is None:
            return None
        if len(symbol_prices) < 60:
            return None
        result = run_monte_carlo(symbol_prices["close"])
        return {
            "symbol": symbol,
            "metrics": result["metrics"],
            "paths": result["paths"]
        }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_run_mc, symbols))

    docs = [d for d in results if d is not None]

    if docs:
        mc_col.insert_many(docs)

    print(
        f"Monte Carlo simulations stored in MongoDB "
        f"({len(docs)} symbols)."
    )


if __name__ == "__main__":
    build_monte_carlo()
