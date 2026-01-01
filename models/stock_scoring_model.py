import pandas as pd


def compute_stock_score(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "symbol", "close", "sma_200", "roc_63", "atr_14", "sector_regime"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    scored = df.copy()

    # Feature engineering
    scored["trend_strength"] = (
        (scored["close"] - scored["sma_200"]) / scored["sma_200"]
    )

    scored["momentum"] = scored["roc_63"]

    scored["volatility_tightness"] = 1 - (scored["atr_14"] / scored["close"])

    # VCP-only universe → constant bonus
    scored["structure_bonus"] = 1.0

    # Composite score
    scored["stock_score"] = (
        0.35 * scored["trend_strength"] +
        0.35 * scored["momentum"] +
        0.20 * scored["volatility_tightness"] +
        0.10 * scored["structure_bonus"]
    )

    # Ranking
    scored["rank"] = scored["stock_score"].rank(
        ascending=False, method="first"
    )

    scored = scored.sort_values("rank")

    return scored
