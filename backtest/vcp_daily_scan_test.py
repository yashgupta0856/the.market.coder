import pandas as pd
from datetime import date

from backtest.config import (
    BACKTEST_START_DATE,
    BACKTEST_END_DATE
)

from scanners.vcp_scanner import is_vcp_candidate
from indicators.moving_averages import sma
from indicators.volatility import atr, range_compression
from indicators.trend import linear_regression_slope


DATA_PATH = "data/processed"


def load_ohlcv():
    df = pd.read_csv(
        f"{DATA_PATH}/ohlcv_equities.csv",
        parse_dates=["date"]
    )
    df["date"] = df["date"].dt.tz_localize(None).dt.normalize()
    return df


def compute_indicators(symbol_df):
    df = symbol_df.sort_values("date").copy()

    df["sma_50"] = sma(df["close"], 50)
    df["sma_200"] = sma(df["close"], 200)
    df["atr_14"] = atr(df["high"], df["low"], df["close"], 14)
    df["atr_100"] = atr(df["high"], df["low"], df["close"], 100)
    df["range_compression"] = range_compression(
        df["high"], df["low"], df["close"]
    )
    df["reg_slope_100"] = linear_regression_slope(df["close"], 100)

    return df


def run_daily_vcp_scan():
    ohlcv = load_ohlcv()
    symbols = ohlcv["symbol"].unique().tolist()

    trading_days = ohlcv["date"].drop_duplicates().sort_values()
    trading_days = trading_days[
        (trading_days >= pd.Timestamp(BACKTEST_START_DATE)) &
        (trading_days <= pd.Timestamp(BACKTEST_END_DATE))
    ]

    for ts in trading_days:
        current_date = ts.date()
        vcp_stocks = []

        for symbol in symbols:
            history = ohlcv[
                (ohlcv["symbol"] == symbol) &
                (ohlcv["date"] <= ts)
            ]

            if len(history) < 200:
                continue

            enriched = compute_indicators(history)

            try:
                if is_vcp_candidate(enriched):
                    vcp_stocks.append(symbol)
            except Exception:
                continue

        print(
            f"{current_date} → "
            f"{len(vcp_stocks)} VCP stocks → {vcp_stocks}"
        )


if __name__ == "__main__":
    run_daily_vcp_scan()
