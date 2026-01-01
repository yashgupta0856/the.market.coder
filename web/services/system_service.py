from pathlib import Path
from datetime import datetime
import pandas as pd


UNIVERSE_PATH = "data/processed/equity_universe.csv"
VCP_PATH = "data/processed/vcp_candidates.csv"
FINAL_OUTPUT_PATH = "output/final_stock_scores.csv"


def get_system_stats():
    stats = {}

    # Universe size
    if Path(UNIVERSE_PATH).exists():
        universe_df = pd.read_csv(UNIVERSE_PATH)
        stats["universe_size"] = len(universe_df)
    else:
        stats["universe_size"] = 0

    # VCP candidates
    if Path(VCP_PATH).exists():
        vcp_df = pd.read_csv(VCP_PATH,usecols=['vcp_candidate'])
        stats["vcp_candidates"] = len(vcp_df[vcp_df["vcp_candidate"] == True])
    else:
        stats["vcp_candidates"] = 0

    # Final ranked stocks
    if Path(FINAL_OUTPUT_PATH).exists():
        ranked_df = pd.read_csv(FINAL_OUTPUT_PATH)
        stats["final_ranked"] = len(ranked_df)

        last_modified = Path(FINAL_OUTPUT_PATH).stat().st_mtime
        stats["last_run"] = datetime.fromtimestamp(last_modified).strftime(
            "%Y-%m-%d %H:%M IST"
        )
    else:
        stats["final_ranked"] = 0
        stats["last_run"] = "N/A"

    return stats
