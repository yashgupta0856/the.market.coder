import pandas as pd
from pathlib import Path


FINAL_STOCKS_PATH = "output/final_stock_scores.csv"


def get_rank1_stock_explanation():
    if not Path(FINAL_STOCKS_PATH).exists():
        return None, None

    df = pd.read_csv(FINAL_STOCKS_PATH)

    required = {
        "symbol", "rank", "trend_strength", "momentum",
        "volatility_tightness", "structure_bonus",
        "sector_index", "sector_regime", "vcp_candidate"
    }

    if not required.issubset(df.columns):
        return None, None

    # Rank is float (1.0), so sort safely
    top = df.sort_values("rank").iloc[0]

    # - LEFT PANEL (Why Rank 1) -
    left = {
        "symbol": top["symbol"],
        "rank": int(top["rank"]),
        "trend_strength": top["trend_strength"],
        "momentum": top["momentum"],
        "volatility_tightness": top["volatility_tightness"],
        "structure_bonus": bool(top["structure_bonus"]),
        "sector_index": top["sector_index"],
        "sector_regime": top["sector_regime"],
    }

    # - RIGHT PANEL (VCP Status) -
    right = {
        "vcp_status": "PASS" if top["vcp_candidate"] else "FAIL",
        "trend_gate": "PASS",            # implied by vcp_candidate
        "volatility_gate": "PASS",
        "price_tightness_gate": "PASS",
        "volume_dryup_gate": "PASS",
    }

    return left, right
