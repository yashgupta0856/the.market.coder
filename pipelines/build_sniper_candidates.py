import pandas as pd
from utils.mongo import get_collection
from scanners.sniper_scanner import scan_universe_sniper


def run_sniper_pipeline():
    """
    Runs Sniper scanner and stores results in MongoDB
    """

    ohlcv_col = get_collection("ohlcv_equities")
    sniper_col = get_collection("sniper_candidates")

    # Load OHLCV from MongoDB
    df = pd.DataFrame(
        list(
            ohlcv_col.find(
                {},
                {"_id": 0, "date": 1, "symbol": 1, "close": 1, "volume": 1}
            )
        )
    )

    if df.empty:
        raise RuntimeError("OHLCV collection is empty")

    df["date"] = pd.to_datetime(df["date"])

    # Run sniper scanner
    sniper_df = scan_universe_sniper(df)

    # Clear old results
    sniper_col.delete_many({})

    # Insert new results
    if not sniper_df.empty:
        sniper_col.insert_many(
            sniper_df.to_dict(orient="records")
        )

    print(f"Sniper scan complete → {sniper_df['sniper_candidate'].sum()} stocks")


if __name__ == "__main__":
    run_sniper_pipeline()
