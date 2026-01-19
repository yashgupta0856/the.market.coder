import pandas as pd

from scanners.vcp_scanner import scan_universe
from utils.mongo import get_collection
from utils.mongo_loader import csv_to_mongo


OUTPUT_PATH = "data/processed/vcp_candidates.csv"


def run_phase3():
    # READ FROM MONGODB
    col = get_collection("equity_indicators")
    df = pd.DataFrame(list(col.find({}, {"_id": 0})))
    df["date"] = pd.to_datetime(df["date"])

    # RUN VCP SCAN
    results = scan_universe(df)

    # WRITE CSV (temporary)
    results.to_csv(OUTPUT_PATH, index=False)

    # WRITE MONGODB
    csv_to_mongo(
        OUTPUT_PATH,
        "vcp_candidates"
    )


if __name__ == "__main__":
    run_phase3()
