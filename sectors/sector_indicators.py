import yfinance as yf
import pandas as pd


def fetch_sector_ohlcv(sector_index, start=None, end=None):

    # SPECIAL CASE: Financial Services
    if sector_index == "CNXFIN":
        ticker = "NIFTY_FIN_SERVICE.NS"
    else:
        ticker = f"^{sector_index}"

    df = yf.download(
        ticker,
        start=start,
        end=end,
        auto_adjust=False,
        progress=False
    )

    if df.empty:
        raise ValueError(f"No data for sector index {sector_index}")

    df = df.reset_index()

    # Handle MultiIndex columns from yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df.columns = [c.lower() for c in df.columns]
    df["sector_index"] = sector_index

    return df




from indicators.moving_averages import sma, ema
from indicators.momentum import roc
from indicators.volatility import  atr, range_compression
from indicators.trend import linear_regression_slope


def compute_sector_indicators(df):
    df = df.sort_values("date").copy()

    df["sma_50"] = sma(df["close"], 50)
    df["sma_200"] = sma(df["close"], 200)

    df["ema_50"] = ema(df["close"], 50)

    df["roc_63"] = roc(df["close"], 63)

    df["atr_14"] = atr(df["high"], df["low"], df["close"], 14)
    df["atr_100"] = atr(df["high"], df["low"], df["close"], 100)

    df["range_compression"] = range_compression(
        df["high"], df["low"], df["close"]
    )

    df["reg_slope_100"] = linear_regression_slope(df["close"], 100)

    return df
