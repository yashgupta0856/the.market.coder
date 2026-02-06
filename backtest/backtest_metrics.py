import pandas as pd

from backtest.vcp_stateful_backtest import run_stateful_backtest


def compute_metrics(trades):
    if not trades:
        print("No trades to analyze.")
        return

    df = pd.DataFrame(trades)

    # Basic derived fields
    df["return_pct"] = df["total_return"] * 100
    df["win"] = df["total_return"] > 0

    total_trades = len(df)
    wins = df["win"].sum()
    losses = total_trades - wins

    win_ratio = wins / total_trades * 100
    avg_return = df["return_pct"].mean()
    median_return = df["return_pct"].median()

    best_trade = df["return_pct"].max()
    worst_trade = df["return_pct"].min()

    print("\n================ BACKTEST SUMMARY ================")
    print(f"Total Trades       : {total_trades}")
    print(f"Wins               : {wins}")
    print(f"Losses             : {losses}")
    print(f"Win Ratio (%)      : {win_ratio:.2f}")
    print(f"Average Return (%) : {avg_return:.2f}")
    print(f"Median Return (%)  : {median_return:.2f}")
    print(f"Best Trade (%)     : {best_trade:.2f}")
    print(f"Worst Trade (%)    : {worst_trade:.2f}")
    print("=========\n")

    print("Sample Trades:")
    print(
        df[
            [
                "symbol",
                "entry_date",
                "exit_date",
                "days_held",
                "return_pct"
            ]
        ]
        .sort_values("entry_date")
        .head(10)
        .to_string(index=False)
    )

    return df


if __name__ == "__main__":
    trades = run_stateful_backtest()
    compute_metrics(trades)
