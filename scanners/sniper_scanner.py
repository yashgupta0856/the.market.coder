import numpy as np
import pandas as pd


def wma(series: pd.Series, period: int) -> pd.Series:
    """
    Weighted Moving Average — optimised with np.dot.
    """
    weights = np.arange(1, period + 1, dtype=float)
    total_weight = weights.sum()
    return series.rolling(period).apply(
        lambda x: np.dot(x, weights) / total_weight,
        raw=True,
    )


def resample_close(df, timeframe: str):
    """
    Resample daily data to weekly ('W') or monthly ('ME') and return
    the last close per period.  Resampled ONCE per timeframe.
    """
    return (
        df.set_index("date")["close"]
        .resample(timeframe)
        .last()
        .dropna()
    )


def is_sniper_candidate(symbol_df: pd.DataFrame) -> bool:
    required_cols = ["date", "close", "volume"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < 60:
        return False

    df = symbol_df.sort_values("date").copy()

    # ── DAILY WMAs ────────────────────────────────────────────────────
    df["wma_12"] = wma(df["close"], 12)
    df["wma_20"] = wma(df["close"], 20)

    latest = df.iloc[-1]
    latest_close = latest["close"]

    # ── WEEKLY / MONTHLY — resample ONCE per timeframe ────────────────
    weekly_close = resample_close(df, "W")
    monthly_close = resample_close(df, "ME")

    if len(weekly_close) < 12 or len(monthly_close) < 4:
        return False

    w_wma_6 = wma(weekly_close, 6).iloc[-1]
    w_wma_12 = wma(weekly_close, 12).iloc[-1]
    m_wma_2 = wma(monthly_close, 2).iloc[-1]
    m_wma_4 = wma(monthly_close, 4).iloc[-1]

    # ── SNIPER CONDITIONS (percentage-based thresholds) ───────────────
    # Replaced absolute ₹ offsets with percentage multipliers so the
    # conditions scale properly across all price levels.

    # 1. Close > Monthly WMA(2) × 1.005   (was: + 1)
    if latest_close <= m_wma_2 * 1.005:
        return False

    # 2. Monthly WMA(2) > Monthly WMA(4) × 1.01   (was: + 2)
    if m_wma_2 <= m_wma_4 * 1.01:
        return False

    # 3. Close > Weekly WMA(6) × 1.01   (was: + 2)
    if latest_close <= w_wma_6 * 1.01:
        return False

    # 4. Weekly WMA(6) > Weekly WMA(12) × 1.01   (was: + 2)
    if w_wma_6 <= w_wma_12 * 1.01:
        return False

    # 5. Close > 4-days-ago WMA(12) × 1.01   (was: + 2)
    wma12_4d_ago = df["wma_12"].iloc[-5]
    if wma12_4d_ago is None or pd.isna(wma12_4d_ago):
        return False
    if latest_close <= wma12_4d_ago * 1.01:
        return False

    # 6. Close > 2-days-ago WMA(20) × 1.01   (was: + 2)
    wma20_2d_ago = df["wma_20"].iloc[-3]
    if wma20_2d_ago is None or pd.isna(wma20_2d_ago):
        return False
    if latest_close <= wma20_2d_ago * 1.01:
        return False

    # ── PRICE FILTER ──────────────────────────────────────────────────
    if not (25 <= latest_close <= 500):
        return False

    # ── WEEKLY VOLUME FILTER ──────────────────────────────────────────
    weekly_volume = (
        df.set_index("date")
        .resample("W")["volume"]
        .sum()
    )

    if weekly_volume.iloc[-1] <= 85_000:
        return False

    return True


def scan_universe_sniper(indicator_df, max_workers=8) -> pd.DataFrame:
    from concurrent.futures import ThreadPoolExecutor

    if isinstance(indicator_df, pd.DataFrame):
        groups = list(indicator_df.groupby("symbol"))
    elif isinstance(indicator_df, dict):
        groups = list(indicator_df.items())
    else:
        groups = list(indicator_df)

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

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_process_symbol, groups))

    return pd.DataFrame(results)
