import pandas as pd

from scanners.vcp_scanner import scan_universe
from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo


def run_phase3():
    col = get_collection("equity_indicators")
    df = pd.DataFrame(list(col.find({}, {"_id": 0})))

    if df.empty:
        raise RuntimeError("equity_indicators collection is empty")

    df["date"] = pd.to_datetime(df["date"])

    results = scan_universe(df)

    # Summary stats
    total = len(results)
    candidates = results["vcp_candidate"].sum()
    triggers = results["is_breakout"].sum() if "is_breakout" in results.columns else 0

    quality_counts = results["vcp_quality"].value_counts().to_dict() if "vcp_quality" in results.columns else {}

    print(f"VCP Scan Results:")
    print(f"  Total symbols scanned: {total}")
    print(f"  VCP candidates:        {candidates}")
    print(f"  Active breakouts:      {triggers}")

    if quality_counts:
        print(f"  Quality breakdown:")
        for level in ["trigger", "textbook", "strong", "emerging"]:
            if level in quality_counts:
                print(f"    {level:>10}: {quality_counts[level]}")

    print("Storing vcp_candidates in MongoDB...")
    df_to_mongo(results, "vcp_candidates")


if __name__ == "__main__":
    run_phase3()
