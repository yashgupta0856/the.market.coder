import pandas as pd

MAPPING_PATH = "data/reference/stock_sector_mapping.csv"


def load_sector_mapping():
    df = pd.read_csv(MAPPING_PATH)

    required_cols = ["symbol", "sector", "sector_index"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    if df["symbol"].duplicated().any():
        raise ValueError("Duplicate symbols in sector mapping")

    return df


def get_sector_for_symbol(symbol, mapping_df):
    row = mapping_df[mapping_df["symbol"] == symbol]
    if row.empty:
        return None
    return row.iloc[0]["sector"]


def get_sector_index_for_symbol(symbol, mapping_df):
    row = mapping_df[mapping_df["symbol"] == symbol]
    if row.empty:
        return None
    return row.iloc[0]["sector_index"]