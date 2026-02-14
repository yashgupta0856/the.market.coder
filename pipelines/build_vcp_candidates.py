import pandas as pd

from scanners.vcp_scanner import scan_universe
from utils.mongo import get_collection


def df_to_mongo(df, collection_name, clear_existing=True):
    col = get_collection(collection_name)

    if clear_existing:
        col.delete_many({})

    if not df.empty:
        col.insert_many(df.to_dict(orient="records"))


def run_phase3():
    col = get_collection("equity_indicators")
    df = pd.DataFrame(list(col.find({}, {"_id": 0})))

    if df.empty:
        raise RuntimeError("equity_indicators collection is empty")

    df["date"] = pd.to_datetime(df["date"])

    results = scan_universe(df)

    print("Storing vcp_candidates in MongoDB...")
    df_to_mongo(results, "vcp_candidates")


if __name__ == "__main__":
    run_phase3()
