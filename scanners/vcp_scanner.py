import pandas as pd



def trend_gate(symbol_df):
    required_cols = ["close", "sma_50", "sma_200", "reg_slope_100"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    latest = symbol_df.iloc[-1]

    # Core trend structure
    if not (
        latest["close"] > latest["sma_50"] and
        latest["close"] > latest["sma_200"] and
        latest["sma_50"] > latest["sma_200"]
    ):
        return False

    # Allow flat but non-negative long-term slope
    if latest["reg_slope_100"] < -0.0001:
        return False

    # Price location filter (B+ alignment with website)
    # Needs ~1 year of data
    if len(symbol_df) >= 252:
        recent_high = symbol_df["close"].rolling(252).max().iloc[-1]
        if latest["close"] < 0.8 * recent_high:
            return False

    return True




def volatility_contraction_gate(symbol_df):
    required_cols = ["atr_14", "atr_100", "range_compression"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Need at least 10 days for short-term ATR comparison
    if len(symbol_df) < 10:
        return False

    latest = symbol_df.iloc[-1]

    # Defensive checks
    if latest["atr_100"] <= 0:
        return False

    atr_short_term = symbol_df["atr_14"].iloc[-10]
    atr_ratio_long = latest["atr_14"] / latest["atr_100"]

    # Hybrid volatility contraction condition (B+)
    if not (
        latest["atr_14"] < atr_short_term   # short-term contraction (website-style)
        or atr_ratio_long < 0.95             # long-term contraction (institutional)
    ):
        return False

    # Range compression (loosened)
    if latest["range_compression"] >= 1.1:
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

    close_range_pct = (
        recent["close"].max() - recent["close"].min()
    ) / recent["close"].iloc[-1]

    if close_range_pct >= 0.06:
        return False

    daily_range_pct = (
        (recent["high"] - recent["low"]) / recent["close"]
    )

    if (daily_range_pct > 0.03).any():
        return False

    return True



def volume_dryup_gate(symbol_df, recent_window=15, prior_window=30):
    required_cols = ["volume", "close"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < recent_window + prior_window:
        return False

    recent = symbol_df.iloc[-recent_window:]
    prior = symbol_df.iloc[-(recent_window + prior_window):-recent_window]

    avg_vol_recent = recent["volume"].mean()
    avg_vol_prior = prior["volume"].mean()

    # Rule VU1
    if avg_vol_recent >= 1.1 * avg_vol_prior:
        return False


    # Rule VU2 — distribution days
    prev_close = recent["close"].shift(1)
    down_days = recent["close"] < prev_close
    high_volume = recent["volume"] > (1.5 * avg_vol_recent)

    if (down_days & high_volume).any():
        return False

    return True





def is_vcp_candidate(df: pd.DataFrame) -> bool:
    #Gate 1
    if not trend_gate(df):
        return False

    #Gate 2
    if not volatility_contraction_gate(df):
        return False

    #Gate 3
    if not price_tightness_gate(df):
        return False
    
    #Gate 4
    # commented because to lower restruction on vcp scanner
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

