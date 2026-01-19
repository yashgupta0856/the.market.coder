import pandas as pd
from pathlib import Path

from features.stock_sector_fusion import fuse_stock_with_sector
from features.stock_filtering import filter_vcp_with_sector
from models.stock_scoring_model import compute_stock_score
from sectors.sector_rotation import build_sector_rotation

from utils.mongo import get_collection
from utils.mongo_loader import csv_to_mongo


# PATH CONFIGURATION (TEMP CSVs KEPT)

STOCK_INDICATORS_PATH = "data/processed/indicators/equity_indicators.csv"
VCP_PATH              = "data/processed/vcp_candidates.csv"
SECTOR_REGIME_PATH    = "data/processed/sector_regime.csv"
STOCK_SECTOR_MAP_PATH = "data/reference/stock_sector_mapping.csv"

SECTOR_ROTATION_PATH  = "data/processed/sector_rotation.csv"

FUSED_OUTPUT_PATH     = "data/processed/stock_sector_fused.csv"
FILTERED_OUTPUT_PATH  = "data/processed/vcp_sector_filtered.csv"
FINAL_OUTPUT_PATH     = "output/final_stock_scores.csv"


# ---------- HELPERS (Mongo → CSV bridge) ----------

def mongo_to_csv(collection_name: str, output_path: str):
    col = get_collection(collection_name)
    df = pd.DataFrame(list(col.find({}, {"_id": 0})))
    df.to_csv(output_path, index=False)
    return df


# ---------- PHASE 5.1 — STOCK + SECTOR FUSION ----------

def run_phase5_1() -> pd.DataFrame:
    # READ FROM MONGODB
    mongo_to_csv("equity_indicators", STOCK_INDICATORS_PATH)
    mongo_to_csv("vcp_candidates", VCP_PATH)
    mongo_to_csv("sector_regime", SECTOR_REGIME_PATH)
    mongo_to_csv("stock_sector_mapping", STOCK_SECTOR_MAP_PATH)

    fused_df = fuse_stock_with_sector(
        stock_indicators_path=STOCK_INDICATORS_PATH,
        vcp_path=VCP_PATH,
        sector_regime_path=SECTOR_REGIME_PATH,
        stock_sector_map_path=STOCK_SECTOR_MAP_PATH,
    )

    fused_df.to_csv(FUSED_OUTPUT_PATH, index=False)

    csv_to_mongo(
        FUSED_OUTPUT_PATH,
        "stock_sector_fused"
    )

    return fused_df


# ---------- PHASE 5.2 — VCP + SECTOR FILTERING ----------

def run_phase5_2(fused_df: pd.DataFrame) -> pd.DataFrame:
    filtered_df = filter_vcp_with_sector(fused_df)
    filtered_df.to_csv(FILTERED_OUTPUT_PATH, index=False)

    csv_to_mongo(
        FILTERED_OUTPUT_PATH,
        "vcp_sector_filtered"
    )

    return filtered_df


# ---------- SECTOR ROTATION BOOST ----------

def apply_sector_rotation_boost(stock_df: pd.DataFrame) -> pd.DataFrame:
    if not Path(SECTOR_ROTATION_PATH).exists():
        return stock_df

    rot = pd.read_csv(SECTOR_ROTATION_PATH)

    if not {"sector_index", "rotation_rank"}.issubset(rot.columns):
        return stock_df

    max_rank = rot["rotation_rank"].max()

    rot["rotation_bonus"] = 1 - (rot["rotation_rank"] / max_rank)
    rot["rotation_bonus"] = rot["rotation_bonus"].clip(0.0, 1.0)

    df = stock_df.merge(
        rot[["sector_index", "rotation_bonus"]],
        on="sector_index",
        how="left"
    )

    df["rotation_bonus"] = df["rotation_bonus"].fillna(0.0)

    df["stock_score"] = df["stock_score"] + (0.15 * df["rotation_bonus"])

    return df


# ---------- PHASE 5.3 — STOCK SCORING & RANKING ----------

def run_phase5_3(filtered_df: pd.DataFrame) -> pd.DataFrame:
    ranked_df = compute_stock_score(filtered_df)

    ranked_df = apply_sector_rotation_boost(ranked_df)

    ranked_df = ranked_df.sort_values("stock_score", ascending=False)
    ranked_df["rank"] = range(1, len(ranked_df) + 1)

    ranked_df.to_csv(FINAL_OUTPUT_PATH, index=False)

    csv_to_mongo(
        FINAL_OUTPUT_PATH,
        "final_stock_scores"
    )

    return ranked_df


# ---------- PIPELINE ENTRY POINT ----------

def run_phase5_pipeline():
    print("Starting Phase 5.1 — Stock Sector Fusion")
    fused_df = run_phase5_1()

    print("Building Sector Rotation (from stock breadth)")
    rotation_df = build_sector_rotation(window_col="roc_63")
    rotation_df.to_csv(SECTOR_ROTATION_PATH, index=False)

    csv_to_mongo(
        SECTOR_ROTATION_PATH,
        "sector_rotation"
    )

    print("Starting Phase 5.2 — VCP Sector Filtering")
    filtered_df = run_phase5_2(fused_df)

    print("Starting Phase 5.3 — Stock Scoring + Rotation Boost")
    final_df = run_phase5_3(filtered_df)

    print("Phase 5 pipeline completed successfully")
    return final_df


if __name__ == "__main__":
    run_phase5_pipeline()
