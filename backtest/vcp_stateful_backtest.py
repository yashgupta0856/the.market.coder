import pandas as pd

from backtest.config import (
    BACKTEST_START_DATE,
    BACKTEST_END_DATE,
    HOLDING_DAYS
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


def run_stateful_backtest():
    ohlcv = load_ohlcv()
    symbols = ohlcv["symbol"].unique().tolist()

    trading_days = ohlcv["date"].drop_duplicates().sort_values()

    entry_days = trading_days[
        (trading_days >= pd.Timestamp(BACKTEST_START_DATE)) &
        (trading_days <= pd.Timestamp(BACKTEST_END_DATE))
    ]

    future_days = trading_days[
        trading_days > pd.Timestamp(BACKTEST_END_DATE)
    ]

    active_trades = {}
    closed_trades = []

    # ======================================================
    # PHASE A — ENTRY + TRACKING
    # ======================================================
    for ts in entry_days:
        current_date = ts.date()
        print(f"\n===== {current_date} =====")

        todays_vcp = []

        # Detect VCP stocks
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
                    todays_vcp.append(symbol)
            except Exception:
                continue

        print(f"VCP stocks today: {todays_vcp}")

        # Open new trades
        for symbol in todays_vcp:
            if symbol in active_trades:
                continue

            today_row = ohlcv[
                (ohlcv["symbol"] == symbol) &
                (ohlcv["date"] == ts)
            ]

            if today_row.empty:
                continue

            entry_close = today_row.iloc[0]["close"]

            active_trades[symbol] = {
                "symbol": symbol,
                "entry_date": current_date,
                "entry_close": entry_close,
                "days_held": 0,
                "last_close": entry_close
            }

            print(f"OPEN → {symbol} @ {entry_close:.2f}")

        # Update trades
        to_close = []

        for symbol, trade in active_trades.items():
            today_row = ohlcv[
                (ohlcv["symbol"] == symbol) &
                (ohlcv["date"] == ts)
            ]

            if today_row.empty:
                continue

            close_price = today_row.iloc[0]["close"]
            trade["days_held"] += 1
            trade["last_close"] = close_price

            ret = (close_price / trade["entry_close"]) - 1

            print(
                f"TRACK {symbol} | "
                f"Day {trade['days_held']} | "
                f"Return {ret:.2%}"
            )

            if trade["days_held"] >= HOLDING_DAYS:
                trade["exit_date"] = current_date
                trade["exit_close"] = close_price
                trade["total_return"] = ret

                closed_trades.append(trade)
                to_close.append(symbol)

                print(f"CLOSE → {symbol} | Return {ret:.2%}")

        for symbol in to_close:
            del active_trades[symbol]

    # ======================================================
    # PHASE B — EXTEND ONLY EXISTING TRADES
    # ======================================================
    for ts in future_days:
        if not active_trades:
            break

        current_date = ts.date()
        print(f"\n===== {current_date} (EXTENSION) =====")

        to_close = []

        for symbol, trade in active_trades.items():
            today_row = ohlcv[
                (ohlcv["symbol"] == symbol) &
                (ohlcv["date"] == ts)
            ]

            if today_row.empty:
                continue

            close_price = today_row.iloc[0]["close"]
            trade["days_held"] += 1
            trade["last_close"] = close_price

            ret = (close_price / trade["entry_close"]) - 1

            print(
                f"TRACK {symbol} | "
                f"Day {trade['days_held']} | "
                f"Return {ret:.2%}"
            )

            if trade["days_held"] >= HOLDING_DAYS:
                trade["exit_date"] = current_date
                trade["exit_close"] = close_price
                trade["total_return"] = ret

                closed_trades.append(trade)
                to_close.append(symbol)

                print(f"CLOSE → {symbol} | Return {ret:.2%}")

        for symbol in to_close:
            del active_trades[symbol]

    print("\n===== BACKTEST COMPLETE =====")
    print(f"Closed trades: {len(closed_trades)}")

    return closed_trades


if __name__ == "__main__":
    run_stateful_backtest()
