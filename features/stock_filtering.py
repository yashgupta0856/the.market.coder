import pandas as pd


ALLOWED_SECTOR_REGIMES = {"LEADING", "IMPROVING"}


def filter_vcp_with_sector(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["vcp_candidate", "sector_regime"]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    filtered = df[
        (df["vcp_candidate"] == True) &
        (df["sector_regime"].isin(ALLOWED_SECTOR_REGIMES))
    ].copy()

    return filtered
