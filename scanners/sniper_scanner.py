import pandas as pd


def wma(series: pd.Series, period: int) -> pd.Series:
    """
    Weighted Moving Average
    """
    weights = range(1, period + 1)
    return series.rolling(period).apply(
        lambda x: (x * weights).sum() / sum(weights),
        raw=True
    )


def resample_and_wma(df, timeframe: str, period: int):
    """
    timeframe: 'W' or 'M'
    """
    resampled = (
        df
        .set_index("date")
        .resample(timeframe)
        .last()
        .dropna()
    )
    return wma(resampled["close"], period)


def is_sniper_candidate(symbol_df: pd.DataFrame) -> bool:
    required_cols = ["date", "close", "volume"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < 60:
        return False

    df = symbol_df.sort_values("date").copy()

    
    # DAILY WMAs
    
    df["wma_1"] = wma(df["close"], 1)
    df["wma_12"] = wma(df["close"], 12)
    df["wma_20"] = wma(df["close"], 20)

    latest = df.iloc[-1]

    
    # WEEKLY / MONTHLY WMAs
    
    weekly_wma_6 = resample_and_wma(df, "W", 6)
    weekly_wma_12 = resample_and_wma(df, "W", 12)

    monthly_wma_2 = resample_and_wma(df, "M", 2)
    monthly_wma_4 = resample_and_wma(df, "M", 4)

    if len(weekly_wma_12) < 1 or len(monthly_wma_4) < 1:
        return False

    # Latest higher-timeframe values
    w_wma_6 = weekly_wma_6.iloc[-1]
    w_wma_12 = weekly_wma_12.iloc[-1]
    m_wma_2 = monthly_wma_2.iloc[-1]
    m_wma_4 = monthly_wma_4.iloc[-1]

    
    # SNIPER CONDITIONS
    

    # 1. Daily WMA(1) > Monthly WMA(2) + 1
    if latest["wma_1"] <= m_wma_2 + 1:
        return False

    # 2. Monthly WMA(2) > Monthly WMA(4) + 2
    if m_wma_2 <= m_wma_4 + 2:
        return False

    # 3. Daily WMA(1) > Weekly WMA(6) + 2
    if latest["wma_1"] <= w_wma_6 + 2:
        return False

    # 4. Weekly WMA(6) > Weekly WMA(12) + 2
    if w_wma_6 <= w_wma_12 + 2:
        return False

    # 5. Daily WMA(1) > 4 days ago WMA(12) + 2
    if df["wma_12"].iloc[-5] is None:
        return False
    if latest["wma_1"] <= df["wma_12"].iloc[-5] + 2:
        return False

    # 6. Daily WMA(1) > 2 days ago WMA(20) + 2
    if df["wma_20"].iloc[-3] is None:
        return False
    if latest["wma_1"] <= df["wma_20"].iloc[-3] + 2:
        return False

    
    # PRICE FILTER
    
    if not (25 <= latest["close"] <= 500):
        return False

    
    # WEEKLY VOLUME FILTER
    
    weekly_volume = (
        df
        .set_index("date")
        .resample("W")["volume"]
        .sum()
    )

    if weekly_volume.iloc[-1] <= 85_000:
        return False

    return True


def scan_universe_sniper(indicator_df: pd.DataFrame, max_workers=8) -> pd.DataFrame:
    from concurrent.futures import ThreadPoolExecutor

    def _process_symbol(args):
        symbol, symbol_df = args
        try:
            is_sniper = is_sniper_candidate(symbol_df)
        except Exception:
            is_sniper = False

        return {
            "symbol": symbol,
            "sniper_candidate": is_sniper
        }

    groups = list(indicator_df.groupby("symbol"))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_process_symbol, groups))

    return pd.DataFrame(results)