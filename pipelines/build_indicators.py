import pandas as pd

from indicators.moving_averages import sma, ema
from indicators.momentum import roc
from indicators.volatility import true_range, atr, rolling_std, range_compression
from indicators.trend import linear_regression_slope

from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo




# Indicator Computation


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").copy()

    df["sma_20"] = sma(df["close"], 20)
    df["sma_50"] = sma(df["close"], 50)
    df["sma_100"] = sma(df["close"], 100)
    df["sma_200"] = sma(df["close"], 200)

    df["ema_20"] = ema(df["close"], 20)
    df["ema_50"] = ema(df["close"], 50)

    df["roc_63"] = roc(df["close"], 63)
    df["roc_126"] = roc(df["close"], 126)

    df["tr"] = true_range(df["high"], df["low"], df["close"])
    df["atr_14"] = atr(df["high"], df["low"], df["close"], 14)
    df["atr_100"] = atr(df["high"], df["low"], df["close"], 100)

    df["std_20"] = rolling_std(df["close"], 20)
    df["std_100"] = rolling_std(df["close"], 100)

    df["range_compression"] = range_compression(
        df["high"], df["low"], df["close"]
    )

    df["reg_slope_50"] = linear_regression_slope(df["close"], 50)
    df["reg_slope_100"] = linear_regression_slope(df["close"], 100)

    return df



# Phase 2 Runner


def run_phase2():
    col = get_collection("ohlcv_equities")
    ohlcv = pd.DataFrame(list(col.find({}, {"_id": 0})))

    if ohlcv.empty:
        raise RuntimeError("ohlcv_equities collection is empty")

    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    frames = []

    for symbol, symbol_df in ohlcv.groupby("symbol"):
        enriched = compute_indicators(symbol_df)
        frames.append(enriched)

    final_df = pd.concat(frames, ignore_index=True)

    print("Storing equity_indicators in MongoDB...")
    df_to_mongo(final_df, "equity_indicators")


if __name__ == "__main__":
    run_phase2()
