import pandas as pd


def trend_gate(symbol_df):
    required_cols = ["close", "sma_50", "sma_200", "reg_slope_100"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    latest = symbol_df.iloc[-1]

    # IMAGE SCANNER STYLE TREND
    if not (
        latest["close"] > latest["sma_50"] and
        latest["sma_50"] > latest["sma_200"]
    ):
        return False

    # Allow slightly negative but flat slope
    if latest["reg_slope_100"] < -0.001:
        return False

    # Price must be reasonably close to 52-week high
    if len(symbol_df) >= 252:
        recent_high = symbol_df["close"].rolling(252).max().iloc[-1]
        if latest["close"] < 0.75 * recent_high:
            return False

    return True


def volatility_contraction_gate(symbol_df):
    required_cols = ["atr_14", "close"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < 10:
        return False

    latest = symbol_df.iloc[-1]

    # ATR contraction vs 10 days ago (image logic)
    atr_10_days_ago = symbol_df["atr_14"].iloc[-10]
    if latest["atr_14"] >= atr_10_days_ago:
        return False

    # ATR normalized by price
    if (latest["atr_14"] / latest["close"]) > 0.08:
        return False

    return True


def price_tightness_gate(symbol_df, lookback=15):
    required_cols = ["close", "high", "low"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < lookback:
        return False

    recent = symbol_df.iloc[-lookback:]

    # Slightly loosened tightness
    close_range_pct = (
        recent["close"].max() - recent["close"].min()
    ) / recent["close"].iloc[-1]

    if close_range_pct > 0.08:
        return False

    # Allow occasional wide day
    daily_range_pct = (
        (recent["high"] - recent["low"]) / recent["close"]
    )

    if (daily_range_pct > 0.05).sum() > 2:
        return False

    return True


def volume_dryup_gate(
    symbol_df,
    recent_window=15,
    prior_window=30,
    max_vol_expansion=1.5,
    max_distribution_ratio=0.40
):
    required_cols = ["volume", "close"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < recent_window + prior_window:
        return True  # IMPORTANT: do NOT kill early

    recent = symbol_df.iloc[-recent_window:]
    prior = symbol_df.iloc[-(recent_window + prior_window):-recent_window]

    avg_vol_recent = recent["volume"].mean()
    avg_vol_prior = prior["volume"].mean()

    # Allow volume expansion (no hard dry-up)
    if avg_vol_recent > max_vol_expansion * avg_vol_prior:
        return False

    # Distribution days check (soft)
    prev_close = recent["close"].shift(1)
    down_days = recent["close"] < prev_close
    high_volume = recent["volume"] > (1.5 * avg_vol_recent)

    distribution_days = (down_days & high_volume).sum()
    distribution_ratio = distribution_days / recent_window

    if distribution_ratio > max_distribution_ratio:
        return False

    return True


def is_vcp_candidate(df: pd.DataFrame) -> bool:
    if not trend_gate(df):
        return False

    if not volatility_contraction_gate(df):
        return False

    if not price_tightness_gate(df):
        return False

    # Volume is now a SOFT filter
    if not volume_dryup_gate(df):
        return False

    return True


def scan_universe(indicator_df: pd.DataFrame) -> pd.DataFrame:
    results = []

    for symbol, symbol_df in indicator_df.groupby("symbol"):
        symbol_df = symbol_df.sort_values("date")

        try:
            is_vcp = is_vcp_candidate(symbol_df)
        except Exception:
            is_vcp = False

        results.append({
            "symbol": symbol,
            "vcp_candidate": is_vcp
        })

    return pd.DataFrame(results)