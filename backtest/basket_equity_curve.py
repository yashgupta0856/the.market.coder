import os
import json
import pandas as pd
import matplotlib.pyplot as plt

from backtest.vcp_stateful_backtest import run_stateful_backtest
from backtest.config import (
    BACKTEST_START_DATE,
    BACKTEST_END_DATE,
    HOLDING_DAYS
)



# Output paths (web-consumable)

STATIC_DIR = "web/static"
EQUITY_CURVE_IMG = "vcp_equity_curve.png"
SUMMARY_JSON = "vcp_backtest_summary.json"



# Equity curve construction

def build_equity_curve(trades):
    """
    Build basket-level equity curve using equal-weighted daily returns
    derived from trade-level results (smoothed per-trade).
    """

    if not trades:
        raise ValueError("No trades available to build equity curve.")

    records = []

    for trade in trades:
        entry_date = pd.to_datetime(trade["entry_date"])
        days = trade["days_held"]

        # Smoothed daily return (consistent with current backtest engine)
        daily_return = (1 + trade["total_return"]) ** (1 / days) - 1

        current_date = entry_date
        for _ in range(days):
            records.append({
                "date": current_date,
                "symbol": trade["symbol"],
                "daily_return": daily_return
            })
            current_date += pd.Timedelta(days=1)

    df = pd.DataFrame(records)

    basket_daily_return = (
        df.groupby("date")["daily_return"]
        .mean()
        .sort_index()
    )

    equity_curve = (1 + basket_daily_return).cumprod()

    equity_df = pd.DataFrame({
        "basket_return": basket_daily_return,
        "equity": equity_curve
    })

    return equity_df



# Console summary (developer / logs)

def print_equity_summary(equity_df):
    start_equity = equity_df["equity"].iloc[0]
    end_equity = equity_df["equity"].iloc[-1]
    total_return_pct = (end_equity - 1) * 100

    print("\n================ EQUITY CURVE SUMMARY ================")
    print(f"Start Equity        : {start_equity:.4f}")
    print(f"End Equity          : {end_equity:.4f}")
    print(f"Total Return (%)    : {total_return_pct:.2f}")
    print("=====================================================\n")

    print("Sample Equity Curve:")
    print(equity_df.head(10).to_string())



# Save equity curve image for web UI

def save_equity_curve_plot(equity_df):
    os.makedirs(STATIC_DIR, exist_ok=True)
    output_path = os.path.join(STATIC_DIR, EQUITY_CURVE_IMG)

    plt.figure(figsize=(12, 6))
    plt.plot(equity_df.index, equity_df["equity"])
    plt.title("QuantFusion — VCP Basket Equity Curve")
    plt.xlabel("Date")
    plt.ylabel("Equity (Normalized)")
    plt.grid(True)
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()

    print(f"Equity curve image saved to: {output_path}")



# Save JSON summary for FastAPI dashboard

def save_backtest_summary(equity_df, trades):
    total_trades = len(trades)
    wins = sum(t["total_return"] > 0 for t in trades)

    summary = {
        "start_date": str(BACKTEST_START_DATE),
        "end_date": str(BACKTEST_END_DATE),
        "holding_days": HOLDING_DAYS,
        "total_trades": total_trades,
        "win_ratio_pct": round((wins / total_trades) * 100, 2) if total_trades else 0,
        "average_return_pct": round(
            (sum(t["total_return"] for t in trades) / total_trades) * 100, 2
        ) if total_trades else 0,
        "equity_start": round(equity_df["equity"].iloc[0], 4),
        "equity_end": round(equity_df["equity"].iloc[-1], 4),
        "total_strategy_return_pct": round(
            (equity_df["equity"].iloc[-1] - 1) * 100, 2
        )
    }

    output_path = os.path.join(STATIC_DIR, SUMMARY_JSON)

    with open(output_path, "w") as f:
        json.dump(summary, f, indent=4)

    print(f"Backtest summary saved to: {output_path}")



# End-to-end runner

def run_basket_equity_curve():
    """
    End-to-end execution:
    - Runs stateful backtest
    - Builds basket equity curve
    - Prints console summary
    - Saves equity curve image
    - Saves JSON summary for web
    """

    trades = run_stateful_backtest()
    equity_df = build_equity_curve(trades)

    print_equity_summary(equity_df)
    save_equity_curve_plot(equity_df)
    save_backtest_summary(equity_df, trades)

    return equity_df


if __name__ == "__main__":
    run_basket_equity_curve()
