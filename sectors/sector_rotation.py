import pandas as pd
from pathlib import Path


INDICATORS_PATH = "data/processed/stock_sector_fused.csv"
BENCHMARK_PATH  = "data/processed/benchmark_indicators.csv"



# Sector Breadth


def compute_sector_breadth(window_col="roc_63"):
    """
    Sector breadth = % of stocks with positive returns
    """
    if not Path(INDICATORS_PATH).exists():
        raise FileNotFoundError(
            "stock_sector_fused.csv not found. "
            "Run Phase 5.1 before sector rotation."
        )

    df = pd.read_csv(INDICATORS_PATH)

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



# Capital Impact


def compute_sector_capital_impact(window_col="roc_63"):
    if not Path(INDICATORS_PATH).exists():
        raise FileNotFoundError(
            "stock_sector_fused.csv not found. "
            "Run Phase 5.1 before sector rotation."
        )

    df = pd.read_csv(INDICATORS_PATH)

    required = {"symbol", "sector_index", "close", "volume", window_col}
    if not required.issubset(df.columns):
        raise ValueError("Missing required columns for capital impact")

    latest = (
        df.sort_values("date")
          .groupby("symbol", as_index=False)
          .tail(1)
    )

    latest["capital_weight"] = latest["close"] * latest["volume"]
    latest["impact"] = latest[window_col] * latest["capital_weight"]

    return (
        latest.groupby("sector_index")["impact"]
        .sum()
        .reset_index(name="capital_impact")
    )



# Relative Strength vs Market


def compute_sector_relative_strength(window_col="roc_63"):
    if not Path(INDICATORS_PATH).exists():
        raise FileNotFoundError("stock_sector_fused.csv not found")

    if not Path(BENCHMARK_PATH).exists():
        raise FileNotFoundError("benchmark_indicators.csv not found")

    stock_df = pd.read_csv(INDICATORS_PATH)
    bench_df = pd.read_csv(BENCHMARK_PATH)

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

    market_return = bench_df.sort_values("date").iloc[-1][window_col]

    sector_return["relative_strength"] = (
        sector_return["sector_return"] - market_return
    )

    return sector_return[["sector_index", "relative_strength"]]



# Z-score helper


def zscore(series: pd.Series):
    return (series - series.mean()) / series.std(ddof=0)



# MASTER ROTATION ENGINE


def build_sector_rotation(window_col="roc_63"):
    """
    Builds sector rotation ranking using:
    - breadth
    - capital impact
    - relative strength
    """

    breadth = compute_sector_breadth(window_col)
    impact  = compute_sector_capital_impact(window_col)
    rel     = compute_sector_relative_strength(window_col)

    df = (
        breadth
        .merge(impact, on="sector_index")
        .merge(rel, on="sector_index")
    )

    df["z_breadth"]  = zscore(df["breadth"])
    df["z_capital"]  = zscore(df["capital_impact"])
    df["z_relative"] = zscore(df["relative_strength"])

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
            "capital_impact",
            "relative_strength",
            "rotation_score",
            "rotation_rank",
        ]
    ]