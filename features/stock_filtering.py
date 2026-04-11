import pandas as pd


ALLOWED_SECTOR_REGIMES = {"LEADING", "IMPROVING"}


def filter_vcp_with_sector(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["vcp_candidate", "sector_regime"]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Base filtering: VCP flag + Sector Regime
    mask = (df["vcp_candidate"] == True) & (df["sector_regime"].isin(ALLOWED_SECTOR_REGIMES))

    # Optional Fundamental Filtering (P2)
    # Only applied if the database contains the newly enriched fundamental columns
    if "market_cap" in df.columns:
        # Minimum 1,000 Cr (10 Billion INR) market cap to filter out micro-caps
        mask &= (df["market_cap"].isna()) | (df["market_cap"] >= 10_000_000_000)

    if "revenue_growth" in df.columns:
        # Require positive revenue growth explicitly, but allow NA if fetch failed
        mask &= (df["revenue_growth"].isna()) | (df["revenue_growth"] > 0)
        
    if "trailing_eps" in df.columns and "forward_eps" in df.columns:
        # Require positive earnings (either trailing or forward)
        mask &= (df["trailing_eps"].isna()) | (df["trailing_eps"] > 0) | (df["forward_eps"] > 0)

    filtered = df[mask].copy()

    return filtered
