import pandas as pd
from pathlib import Path


SECTOR_RS_PATH = "data/processed/sector_strength.csv"
SECTOR_REGIME_PATH = "data/processed/sector_regime.csv"


def get_top_sectors_by_regime():
    if not Path(SECTOR_RS_PATH).exists() or not Path(SECTOR_REGIME_PATH).exists():
        return {
            "leading": [],
            "improving": [],
            "weakening": [],
            "lagging": [],
        }

    rs_df = pd.read_csv(SECTOR_RS_PATH)
    regime_df = pd.read_csv(SECTOR_REGIME_PATH)

    # Merge strength + regime
    df = rs_df.merge(
        regime_df[["sector_index", "sector_regime"]],
        on="sector_index",
        how="left",
    )

    result = {
        "leading": [],
        "improving": [],
        "weakening": [],
        "lagging": [],
    }

    regime_config = {
        "LEADING": ("leading", 5),
        "IMPROVING": ("improving", 5),
        "WEAKENING": ("weakening", 5),
        "LAGGING": ("lagging", 5),
    }

    for regime, (key, limit) in regime_config.items():
        subset = (
            df[df["sector_regime"] == regime]
            .sort_values("rs_score", ascending=False)
            .head(limit)
        )

        # IMPORTANT: sector_index is the display identifier
        result[key] = subset[
            ["sector_index", "rs_score"]
        ].to_dict(orient="records")

    return result
