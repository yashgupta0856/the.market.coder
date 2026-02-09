import pandas as pd

from utils.mongo import get_collection
from scanners.sniper_scanner import scan_universe_sniper
from indicators.moving_averages import ema


def clamp(x, low=0.0, high=1.0):
    return max(low, min(high, x))


def compute_sniper_score(symbol_df: pd.DataFrame) -> float | None:
    """
    Compute Sniper score for ONE symbol dataframe
    """

    symbol_df = symbol_df.sort_values("date")

    if len(symbol_df) < 60:
        return None

    latest = symbol_df.iloc[-1]

    
    # 1️ MOMENTUM (40%)
    
    close_20 = symbol_df.iloc[-21]["close"]
    momentum_20 = (latest["close"] / close_20) - 1
    momentum_score = clamp(momentum_20 / 0.15)  # 15% move = strong

    
    # 2️ TREND ALIGNMENT (30%)
    
    trend_conditions = [
        latest["close"] > latest["ema_20"],
        latest["close"] > latest["ema_50"],
        latest["ema_20"] > latest["ema_50"],
    ]
    trend_score = sum(trend_conditions) / 3

    
    # 3️ VOLUME STRENGTH (30%)
    
    recent_vol = symbol_df["volume"].iloc[-5:].mean()
    base_vol = symbol_df["volume"].iloc[-30:].mean()

    if base_vol <= 0:
        return None

    volume_ratio = recent_vol / base_vol
    volume_score = clamp((volume_ratio - 1) / 0.5)  # +50% = strong

    
    # FINAL SCORE
    
    final_score = (
        0.4 * momentum_score +
        0.3 * trend_score +
        0.3 * volume_score
    )

    return round(final_score * 100, 2)


def run_sniper_pipeline():
    """
    Runs Sniper scanner + ranking and stores results in MongoDB
    """

    ohlcv_col = get_collection("ohlcv_equities")
    sniper_col = get_collection("sniper_candidates")
    ranked_col = get_collection("sniper_ranked")

    
    # LOAD OHLCV
    
    df = pd.DataFrame(
        list(
            ohlcv_col.find(
                {},
                {
                    "_id": 0,
                    "date": 1,
                    "symbol": 1,
                    "close": 1,
                    "volume": 1,
                }
            )
        )
    )

    if df.empty:
        raise RuntimeError("OHLCV collection is empty")

    df["date"] = pd.to_datetime(df["date"])

    
    # RUN SNIPER SCANNER
    
    sniper_df = scan_universe_sniper(df)

    sniper_col.delete_many({})
    ranked_col.delete_many({})

    if sniper_df.empty:
        print("No sniper candidates found")
        return

    sniper_col.insert_many(
        sniper_df.to_dict(orient="records")
    )

    
    # RANK SNIPER CANDIDATES
    
    ranked_records = []

    for symbol in sniper_df.loc[
        sniper_df["sniper_candidate"] == True, "symbol"
    ]:
        symbol_df = df[df["symbol"] == symbol].copy()

        # Indicators
        symbol_df["ema_20"] = ema(symbol_df["close"], 20)
        symbol_df["ema_50"] = ema(symbol_df["close"], 50)

        score = compute_sniper_score(symbol_df)

        if score is None:
            continue

        ranked_records.append({
            "symbol": symbol,
            "sniper_score": score
        })

    if not ranked_records:
        print("No sniper stocks qualified for ranking")
        return

    ranked_df = (
        pd.DataFrame(ranked_records)
        .sort_values("sniper_score", ascending=False)
        .reset_index(drop=True)
    )

    ranked_df["rank"] = ranked_df.index + 1

    ranked_col.insert_many(
        ranked_df.to_dict(orient="records")
    )

    print(
        f"Sniper scan complete → "
        f"{sniper_df['sniper_candidate'].sum()} candidates | "
        f"{len(ranked_df)} ranked"
    )


if __name__ == "__main__":
    run_sniper_pipeline()