import pandas as pd


def load_sector_and_benchmark(sector_path, benchmark_path):
    sector_df = pd.read_csv(sector_path, parse_dates=["date"])
    benchmark_df = pd.read_csv(benchmark_path, parse_dates=["date"])

    latest_date = sector_df["date"].max()

    sector_latest = sector_df[sector_df["date"] == latest_date]
    benchmark_latest = benchmark_df[benchmark_df["date"] == latest_date]

    return sector_latest, benchmark_latest



def compute_raw_rs(sector_latest, benchmark_latest):
    bm = benchmark_latest.iloc[0]

    records = []

    for _, row in sector_latest.iterrows():
        rs_momentum = row["roc_63"] - bm["roc_63"]
        rs_slope = row["reg_slope_100"] - bm["reg_slope_100"]
        trend_confirm = int(row["close"] > row["sma_200"])

        records.append({
            "sector_index": row["sector_index"],
            "rs_momentum": rs_momentum,
            "rs_slope": rs_slope,
            "trend_confirm": trend_confirm,
        })

    return pd.DataFrame(records)




def normalize_and_score(rs_df):
    rs_df["z_momentum"] = (
        (rs_df["rs_momentum"] - rs_df["rs_momentum"].mean()) /
        rs_df["rs_momentum"].std()
    )

    rs_df["z_slope"] = (
        (rs_df["rs_slope"] - rs_df["rs_slope"].mean()) /
        rs_df["rs_slope"].std()
    )

    rs_df["rs_score"] = (
        0.5 * rs_df["z_momentum"] +
        0.3 * rs_df["z_slope"] +
        0.2 * rs_df["trend_confirm"]
    )

    rs_df["rs_rank"] = rs_df["rs_score"].rank(ascending=False)

    return rs_df.sort_values("rs_rank")
