import pandas as pd
from features.stock_filtering import filter_vcp_with_sector
from models.stock_scoring_model import compute_stock_score
from sectors.sector_rotation import build_sector_rotation
from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo



# Mongo Helpers


def mongo_to_df(collection_name: str) -> pd.DataFrame:
    col = get_collection(collection_name)
    data = list(col.find({}, {"_id": 0}))
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)



# PHASE 5.1 — STOCK + SECTOR FUSION


def run_phase5_1() -> pd.DataFrame:
    print("Reading required collections from MongoDB...")

    stocks_col = get_collection("equity_indicators")
    vcp = mongo_to_df("vcp_candidates")
    sector_regime = mongo_to_df("sector_regime")
    mapping = mongo_to_df("stock_sector_mapping")

    # Find latest date via a single targeted query (instead of loading all rows)
    latest_doc = stocks_col.find_one(
        {}, {"date": 1, "_id": 0}, sort=[("date", -1)]
    )

    if not latest_doc:
        raise RuntimeError("equity_indicators collection is empty")

    latest_date = latest_doc["date"]

    # Query ONLY the latest date rows (~2000 instead of 200K+)
    stocks_latest = pd.DataFrame(
        list(stocks_col.find({"date": latest_date}, {"_id": 0}))
    )

    if stocks_latest.empty:
        raise RuntimeError("No data found for latest date")

    stocks_latest["date"] = pd.to_datetime(stocks_latest["date"])

    # Merge VCP flag
    stocks_latest = stocks_latest.merge(
        vcp, on="symbol", how="left"
    )
    stocks_latest["vcp_candidate"] = (
        stocks_latest["vcp_candidate"].fillna(False)
    )

    # Merge stock → sector mapping
    stocks_latest = stocks_latest.merge(
        mapping, on="symbol", how="left"
    )

    # Merge sector regime
    stocks_latest = stocks_latest.merge(
        sector_regime,
        on="sector_index",
        how="left"
    )

    print("Storing stock_sector_fused in MongoDB...")
    df_to_mongo(stocks_latest, "stock_sector_fused")

    return stocks_latest



# PHASE 5.2 — VCP + SECTOR FILTERING


def run_phase5_2(fused_df: pd.DataFrame) -> pd.DataFrame:
    filtered_df = filter_vcp_with_sector(fused_df)

    print("Storing vcp_sector_filtered in MongoDB...")
    df_to_mongo(filtered_df, "vcp_sector_filtered")

    return filtered_df



# SECTOR ROTATION BOOST


def apply_sector_rotation_boost(stock_df: pd.DataFrame) -> pd.DataFrame:
    rotation_df = mongo_to_df("sector_rotation")

    if rotation_df.empty:
        return stock_df

    if not {"sector_index", "rotation_rank"}.issubset(rotation_df.columns):
        return stock_df

    max_rank = rotation_df["rotation_rank"].max()

    rotation_df["rotation_bonus"] = (
        1 - (rotation_df["rotation_rank"] / max_rank)
    ).clip(0.0, 1.0)

    df = stock_df.merge(
        rotation_df[["sector_index", "rotation_bonus"]],
        on="sector_index",
        how="left"
    )

    df["rotation_bonus"] = df["rotation_bonus"].fillna(0.0)

    df["stock_score"] = df["stock_score"] + (
        0.15 * df["rotation_bonus"]
    )

    return df



# PHASE 5.3 — STOCK SCORING & RANKING


def run_phase5_3(filtered_df: pd.DataFrame) -> pd.DataFrame:
    ranked_df = compute_stock_score(filtered_df)

    ranked_df = apply_sector_rotation_boost(ranked_df)

    ranked_df = ranked_df.sort_values(
        "stock_score",
        ascending=False
    ).reset_index(drop=True)

    ranked_df["rank"] = ranked_df.index + 1

    print("Storing final_stock_scores in MongoDB...")
    df_to_mongo(ranked_df, "final_stock_scores")

    return ranked_df



# MASTER ENTRY POINT

def run_phase5_pipeline():
    print("Starting Phase 5.1 — Stock Sector Fusion")
    fused_df = run_phase5_1()

    print("Building Sector Rotation")
    rotation_df = build_sector_rotation(window_col="roc_63")

    df_to_mongo(rotation_df, "sector_rotation")

    print("Starting Phase 5.2 — VCP Sector Filtering")
    filtered_df = run_phase5_2(fused_df)

    print("Starting Phase 5.3 — Stock Scoring + Rotation Boost")
    final_df = run_phase5_3(filtered_df)

    print("Phase 5 pipeline completed successfully")

    return final_df


if __name__ == "__main__":
    run_phase5_pipeline()
