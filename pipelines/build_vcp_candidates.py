from scanners.vcp_scanner import scan_universe
from utils.mongo_writer import df_to_mongo
from utils.symbol_loader import load_symbol_frames


VCP_HISTORY_BARS = 252
VCP_PROJECTION = {
    "symbol": 1,
    "date": 1,
    "close": 1,
    "high": 1,
    "low": 1,
    "volume": 1,
    "sma_50": 1,
    "sma_200": 1,
    "reg_slope_100": 1,
    "atr_14": 1,
}


def run_phase3():
    symbol_frames = load_symbol_frames(
        "equity_indicators",
        projection=VCP_PROJECTION,
        limit_per_symbol=VCP_HISTORY_BARS,
        max_workers=8,
    )

    if not symbol_frames:
        raise RuntimeError("equity_indicators collection is empty")

    results = scan_universe(symbol_frames)

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
