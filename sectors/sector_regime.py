import math
import pandas as pd


def classify_sector_regimes(sector_strength_df: pd.DataFrame) -> pd.DataFrame:
    df = sector_strength_df.copy()

    n = len(df)
    if n == 0:
        raise ValueError("Empty sector strength dataframe")

    q1 = math.ceil(0.25 * n)
    q2 = math.ceil(0.50 * n)
    q3 = math.ceil(0.75 * n)

    def regime_from_rank(rank):
        if rank <= q1:
            return "LEADING"
        elif rank <= q2:
            return "IMPROVING"
        elif rank <= q3:
            return "WEAKENING"
        else:
            return "LAGGING"

    df["sector_regime"] = df["rs_rank"].apply(regime_from_rank)

    return df
