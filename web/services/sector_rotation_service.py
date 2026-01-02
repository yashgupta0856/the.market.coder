import pandas as pd
from pathlib import Path

ROTATION_PATH = "data/processed/sector_rotation.csv"

def get_top_rotating_sectors(limit=10):
    if not Path(ROTATION_PATH).exists():
        return []

    df = pd.read_csv(ROTATION_PATH)
    return df.sort_values("rotation_rank").head(limit).to_dict(orient="records")
