import pandas as pd
from utils.mongo import get_collection


# =====================================================
# LOADERS
# =====================================================

def load_stock_sector_fused():
    col = get_collection("stock_sector_fused")

    data = list(col.find({}, {"_id": 0}))

    if not data:
        raise RuntimeError(
            "stock_sector_fused collection is empty. "
            "Run Phase 5.1 before sector rotation."
        )

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    return df


def load_benchmark_indicators():
    col = get_collection("benchmark_indicators")

    data = list(col.find({}, {"_id": 0}))

    if not data:
        raise RuntimeError(
            "benchmark_indicators collection is empty."
        )

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    return df


# =====================================================
# SECTOR BREADTH
# =====================================================

def compute_sector_breadth(window_col="roc_63", fused_df=None):

    df = fused_df if fused_df is not None else load_stock_sector_fused()

    required = {"symbol", "sector_index", window_col}
    if not required.issubset(df.columns):
        raise ValueError("Missing required columns for sector breadth")

    latest = (
        df.sort_values("date")
          .groupby("symbol", as_index=False)
          .tail(1)
    )

    latest["positive"] = latest[window_col] > 0

    return (
        latest.groupby("sector_index")["positive"]
        .mean()
        .reset_index(name="breadth")
    )


# =====================================================
# CAPITAL WEIGHTED RETURN (FIXED)
# =====================================================

def compute_sector_capital_weighted_return(window_col="roc_63", fused_df=None):

    df = fused_df if fused_df is not None else load_stock_sector_fused()

    required = {"symbol", "sector_index", "close", "volume", window_col}
    if not required.issubset(df.columns):
        raise ValueError("Missing required columns for capital weighted return")

    latest = (
        df.sort_values("date")
          .groupby("symbol", as_index=False)
          .tail(1)
    )

    # Proxy capital weight (price * volume)
    latest["capital_weight"] = latest["close"] * latest["volume"]

    grouped = latest.groupby("sector_index")

    weighted_return = (
        grouped.apply(
            lambda x: (
                (x[window_col] * x["capital_weight"]).sum()
                / x["capital_weight"].sum()
                if x["capital_weight"].sum() != 0 else 0
            )
        )
        .reset_index(name="capital_weighted_return")
    )

    return weighted_return


# =====================================================
# RELATIVE STRENGTH VS MARKET
# =====================================================

def compute_sector_relative_strength(window_col="roc_63", fused_df=None, bench_df=None):

    stock_df = fused_df if fused_df is not None else load_stock_sector_fused()
    bench_df = bench_df if bench_df is not None else load_benchmark_indicators()

    latest_stock = (
        stock_df.sort_values("date")
        .groupby("symbol", as_index=False)
        .tail(1)
    )

    sector_return = (
        latest_stock.groupby("sector_index")[window_col]
        .mean()
        .reset_index(name="sector_return")
    )

    latest_benchmark = bench_df.sort_values("date").iloc[-1]

    if window_col not in latest_benchmark:
        raise ValueError(f"{window_col} not found in benchmark data")

    market_return = latest_benchmark[window_col]

    sector_return["relative_strength"] = (
        sector_return["sector_return"] - market_return
    )

    return sector_return[["sector_index", "relative_strength"]]


# =====================================================
# Z-SCORE HELPER
# =====================================================

def zscore(series: pd.Series):
    return (series - series.mean()) / series.std(ddof=0)


# =====================================================
# MASTER ROTATION ENGINE
# =====================================================

def build_sector_rotation(window_col="roc_63"):
    """
    Builds sector rotation ranking using:
    - breadth
    - capital weighted return
    - relative strength

    Loads shared data ONCE and passes to all sub-functions.
    """

    # Load shared data once (was loaded 3× before)
    fused_df = load_stock_sector_fused()
    bench_df = load_benchmark_indicators()

    breadth = compute_sector_breadth(window_col, fused_df=fused_df)
    cap_ret = compute_sector_capital_weighted_return(window_col, fused_df=fused_df)
    rel     = compute_sector_relative_strength(window_col, fused_df=fused_df, bench_df=bench_df)

    df = (
        breadth
        .merge(cap_ret, on="sector_index")
        .merge(rel, on="sector_index")
    )

    # Z-scores
    df["z_breadth"]  = zscore(df["breadth"])
    df["z_capital"]  = zscore(df["capital_weighted_return"])
    df["z_relative"] = zscore(df["relative_strength"])

    # Composite rotation score
    df["rotation_score"] = (
        0.4 * df["z_breadth"] +
        0.4 * df["z_capital"] +
        0.2 * df["z_relative"]
    )

    df = df.sort_values("rotation_score", ascending=False)
    df["rotation_rank"] = range(1, len(df) + 1)

    return df[
        [
            "sector_index",
            "breadth",
            "capital_weighted_return",
            "relative_strength",
            "rotation_score",
            "rotation_rank",
        ]
    ]