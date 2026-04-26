import numpy as np
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

    # Fundamental factor: normalise 0–100 score to 0–1, default 0.5 (neutral)
    if "fundamental_score" in scored.columns:
        scored["fundamental_factor"] = (
            scored["fundamental_score"].fillna(50) / 100
        ).clip(0, 1)
    else:
        scored["fundamental_factor"] = 0.5

    # Composite score
    scored["stock_score"] = (
        0.35 * scored["trend_strength"] +
        0.35 * scored["momentum"] +
        0.20 * scored["volatility_tightness"] +
        0.10 * scored["fundamental_factor"]
    )

    # Ranking
    scored["rank"] = scored["stock_score"].rank(
        ascending=False, method="first"
    )

    scored = scored.sort_values("rank")

    return scored





def clamp(x, low=0.0, high=1.0):
    return max(low, min(high, x))


def compute_sniper_score(symbol_df: pd.DataFrame):
    """
    symbol_df = symbol-level OHLCV dataframe with ema_20, ema_50 columns.
    Must have at least 60 rows.
    """
    symbol_df = symbol_df.sort_values("date")

    if len(symbol_df) < 60:
        return None

    latest = symbol_df.iloc[-1]

    #  MOMENTUM (40%) 
    close_20 = symbol_df.iloc[-21]["close"]
    momentum_20 = (latest["close"] / close_20) - 1
    momentum_score = clamp(momentum_20 / 0.15)  # 15% = strong move

    #  TREND (30%) 
    trend_conditions = [
        latest["close"] > latest["ema_20"],
        latest["close"] > latest["ema_50"],
        latest["ema_20"] > latest["ema_50"],
    ]
    trend_score = sum(trend_conditions) / 3

    #  VOLUME (30%) 
    recent_vol = symbol_df["volume"].iloc[-5:].mean()
    base_vol = symbol_df["volume"].iloc[-30:].mean()

    if base_vol <= 0:
        return None

    volume_ratio = recent_vol / base_vol
    volume_score = clamp((volume_ratio - 1) / 0.5)

    #  FINAL SCORE 
    final_score = (
        0.4 * momentum_score +
        0.3 * trend_score +
        0.3 * volume_score
    )

    return round(final_score * 100, 2)