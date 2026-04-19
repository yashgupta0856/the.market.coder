import pandas as pd

from indicators.moving_averages import sma, ema
from indicators.momentum import roc
from indicators.volatility import true_range, atr, rolling_std, range_compression
from indicators.trend import linear_regression_slope

from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo, upsert_df_to_mongo


ROLLING_OVERLAP = 199
RAW_OHLCV_PROJECTION = {
    "_id": 0,
    "symbol": 1,
    "date": 1,
    "open": 1,
    "high": 1,
    "low": 1,
    "close": 1,
    "volume": 1,
}
INDICATOR_STATE_PROJECTION = {
    "_id": 0,
    "close": 1,
    "ema_20": 1,
    "ema_50": 1,
    "atr_14": 1,
    "atr_100": 1,
}




# Indicator Computation


def _continue_ema(
    series: pd.Series,
    window: int,
    prev_value,
) -> pd.Series:
    if prev_value is None or pd.isna(prev_value):
        return ema(series, window)

    alpha = 2 / (window + 1)
    current = float(prev_value)
    values = []

    for raw_value in series.astype(float):
        current = alpha * raw_value + ((1 - alpha) * current)
        values.append(current)

    return pd.Series(values, index=series.index, dtype=float)


def _true_range_with_prev_close(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    prev_close,
) -> pd.Series:
    if prev_close is None or pd.isna(prev_close):
        return true_range(high, low, close)

    prev_close_series = close.shift(1)
    prev_close_series.iloc[0] = prev_close

    return pd.concat(
        [
            high - low,
            (high - prev_close_series).abs(),
            (low - prev_close_series).abs(),
        ],
        axis=1,
    ).max(axis=1)


def _continue_atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    window: int,
    prev_atr,
    prev_close,
) -> tuple[pd.Series, pd.Series]:
    tr_series = _true_range_with_prev_close(high, low, close, prev_close)

    if prev_atr is None or pd.isna(prev_atr):
        return tr_series, atr(high, low, close, window)

    alpha = 2 / (window + 1)
    current = float(prev_atr)
    values = []

    for tr_value in tr_series.astype(float):
        current = alpha * tr_value + ((1 - alpha) * current)
        values.append(current)

    atr_series = pd.Series(values, index=tr_series.index, dtype=float)
    return tr_series, atr_series


def compute_indicators(
    df: pd.DataFrame,
    prev_state: dict | None = None,
) -> pd.DataFrame:
    df = df.sort_values("date").copy()

    df["sma_20"] = sma(df["close"], 20)
    df["sma_50"] = sma(df["close"], 50)
    df["sma_100"] = sma(df["close"], 100)
    df["sma_200"] = sma(df["close"], 200)

    df["roc_63"] = roc(df["close"], 63)
    df["roc_126"] = roc(df["close"], 126)

    df["std_20"] = rolling_std(df["close"], 20)
    df["std_100"] = rolling_std(df["close"], 100)

    df["range_compression"] = range_compression(
        df["high"], df["low"], df["close"]
    )

    df["reg_slope_50"] = linear_regression_slope(df["close"], 50)
    df["reg_slope_100"] = linear_regression_slope(df["close"], 100)

    prev_close = prev_state.get("close") if prev_state else None
    df["ema_20"] = _continue_ema(
        df["close"],
        20,
        prev_state.get("ema_20") if prev_state else None,
    )
    df["ema_50"] = _continue_ema(
        df["close"],
        50,
        prev_state.get("ema_50") if prev_state else None,
    )

    df["tr"], df["atr_14"] = _continue_atr(
        df["high"],
        df["low"],
        df["close"],
        14,
        prev_state.get("atr_14") if prev_state else None,
        prev_close,
    )
    _, df["atr_100"] = _continue_atr(
        df["high"],
        df["low"],
        df["close"],
        100,
        prev_state.get("atr_100") if prev_state else None,
        prev_close,
    )

    return df


def get_latest_dates(collection_name: str) -> dict[str, pd.Timestamp]:
    col = get_collection(collection_name)

    pipeline = [
        {"$group": {"_id": "$symbol", "last_date": {"$max": "$date"}}},
    ]

    return {
        doc["_id"]: pd.to_datetime(doc["last_date"])
        for doc in col.aggregate(pipeline)
    }


def load_symbol_ohlcv_window(
    symbol: str,
    last_indicator_date: pd.Timestamp | None,
) -> tuple[pd.DataFrame, pd.Timestamp | None, dict | None]:
    ohlcv_col = get_collection("ohlcv_equities")
    indicators_col = get_collection("equity_indicators")

    if last_indicator_date is None:
        docs = list(
            ohlcv_col.find({"symbol": symbol}, RAW_OHLCV_PROJECTION)
            .sort("date", 1)
        )
        if not docs:
            return pd.DataFrame(), None, None

        df = pd.DataFrame(docs)
        df["date"] = pd.to_datetime(df["date"])
        return df, None, None

    overlap_docs = list(
        ohlcv_col.find(
            {"symbol": symbol, "date": {"$lte": last_indicator_date}},
            RAW_OHLCV_PROJECTION,
        )
        .sort("date", -1)
        .limit(ROLLING_OVERLAP)
    )
    new_docs = list(
        ohlcv_col.find(
            {"symbol": symbol, "date": {"$gt": last_indicator_date}},
            RAW_OHLCV_PROJECTION,
        )
        .sort("date", 1)
    )

    if not new_docs:
        return pd.DataFrame(), None, None

    overlap_docs.reverse()
    window_docs = overlap_docs + new_docs

    df = pd.DataFrame(window_docs)
    df["date"] = pd.to_datetime(df["date"])

    first_new_date = pd.to_datetime(new_docs[0]["date"])
    prev_state = None

    if overlap_docs:
        window_start_date = pd.to_datetime(overlap_docs[0]["date"])
        prev_state = indicators_col.find_one(
            {"symbol": symbol, "date": {"$lt": window_start_date}},
            INDICATOR_STATE_PROJECTION,
            sort=[("date", -1)],
        )

    return df, first_new_date, prev_state


def run_phase2_full(max_workers=8):
    from concurrent.futures import ThreadPoolExecutor

    col = get_collection("ohlcv_equities")
    symbols = col.distinct("symbol")

    if not symbols:
        raise RuntimeError("ohlcv_equities collection is empty")

    print(f"Full Phase 2 rebuild: computing indicators for {len(symbols)} symbols...")

    def _process_symbol(symbol):
        docs = list(
            col.find(
                {"symbol": symbol},
                {"_id": 0},
            ).sort("date", 1)
        )
        if not docs:
            return None

        symbol_df = pd.DataFrame(docs)
        symbol_df["date"] = pd.to_datetime(symbol_df["date"])
        return compute_indicators(symbol_df)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        frames = [
            frame
            for frame in executor.map(_process_symbol, symbols)
            if frame is not None and not frame.empty
        ]

    if not frames:
        raise RuntimeError("No valid indicator data computed")

    final_df = pd.concat(frames, ignore_index=True)

    print(f"Storing {len(final_df)} equity_indicators in MongoDB...")
    df_to_mongo(final_df, "equity_indicators")

    return len(final_df)



# Phase 2 Runner


def run_phase2(incremental=True, max_workers=8):
    from concurrent.futures import ThreadPoolExecutor

    if not incremental:
        run_phase2_full(max_workers=max_workers)
        return

    ohlcv_last_dates = get_latest_dates("ohlcv_equities")
    if not ohlcv_last_dates:
        raise RuntimeError("ohlcv_equities collection is empty")

    indicator_last_dates = get_latest_dates("equity_indicators")

    if not indicator_last_dates:
        print("equity_indicators is empty; running full Phase 2 rebuild...")
        run_phase2_full(max_workers=max_workers)
        return

    symbols_to_process = sorted(
        symbol
        for symbol, raw_last_date in ohlcv_last_dates.items()
        if (
            symbol not in indicator_last_dates
            or raw_last_date > indicator_last_dates[symbol]
        )
    )

    if not symbols_to_process:
        print("equity_indicators is already up to date")
        return

    print(
        f"Incremental Phase 2: recomputing {len(symbols_to_process)} symbols "
        f"with windowed overlap..."
    )

    def _process_symbol(symbol: str):
        last_indicator_date = indicator_last_dates.get(symbol)
        window_df, first_new_date, prev_state = load_symbol_ohlcv_window(
            symbol,
            last_indicator_date,
        )

        if window_df.empty:
            return None

        enriched = compute_indicators(window_df, prev_state=prev_state)

        if first_new_date is None:
            return enriched

        return enriched[enriched["date"] >= first_new_date].copy()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        frames = [
            frame
            for frame in executor.map(_process_symbol, symbols_to_process)
            if frame is not None and not frame.empty
        ]

    if not frames:
        print("No indicator rows required updates")
        return

    final_df = pd.concat(frames, ignore_index=True)

    print(f"Upserting {len(final_df)} equity_indicators rows into MongoDB...")
    upsert_df_to_mongo(
        final_df,
        "equity_indicators",
        key_fields=["symbol", "date"],
    )


if __name__ == "__main__":
    run_phase2()
