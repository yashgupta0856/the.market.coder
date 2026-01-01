import pandas as pd
from pathlib import Path



FINAL_STOCKS_PATH = "output/final_stock_scores.csv"
VCP_CANDIDATES_PATH = "data/processed/vcp_candidates.csv"


def get_ranked_vcp_stocks(limit=20):
    if not Path(FINAL_STOCKS_PATH).exists():
        return []

    df = pd.read_csv(FINAL_STOCKS_PATH)

    # Defensive checks
    required_cols = {"symbol", "rank", "close"}
    if not required_cols.issubset(df.columns):
        return []

    df = df.sort_values("rank").head(limit)

    return df[["symbol", "close", "rank"]].to_dict(orient="records")





def get_sector_wise_vcp_counts(limit=5):
    if not Path(FINAL_STOCKS_PATH).exists():
        return []

    df = pd.read_csv(FINAL_STOCKS_PATH)

    required = {"sector_index", "vcp_candidate"}
    if not required.issubset(df.columns):
        return []

    vcp_df = df[df["vcp_candidate"] == True]

    counts = (
        vcp_df.groupby("sector_index")
        .size()
        .reset_index(name="vcp_count")
        .sort_values("vcp_count", ascending=False)
        .head(limit)
    )

    return counts.to_dict(orient="records")








def get_remaining_vcp_symbols():
    if not Path(VCP_CANDIDATES_PATH).exists() or not Path(FINAL_STOCKS_PATH).exists():
        return []

    # Load VCP candidates
    vcp_df = pd.read_csv(VCP_CANDIDATES_PATH)

    if not {"symbol", "vcp_candidate"}.issubset(vcp_df.columns):
        return []

    # Step 1: All VCP = True symbols
    vcp_true_symbols = set(
        vcp_df[vcp_df["vcp_candidate"] == True]["symbol"]
    )

    # Load final selected stocks
    final_df = pd.read_csv(FINAL_STOCKS_PATH)

    if "symbol" not in final_df.columns:
        return []

    # Step 2: Final selected symbols
    final_symbols = set(final_df["symbol"])

    # Step 3: Remaining VCP symbols
    remaining_symbols = vcp_true_symbols - final_symbols

    # Return as sorted list (or dicts if needed later)
    return sorted(list(remaining_symbols))
