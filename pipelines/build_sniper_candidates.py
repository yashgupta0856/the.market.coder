import pandas as pd

from utils.mongo_writer import df_to_mongo
from scanners.sniper_scanner import scan_universe_sniper
from indicators.moving_averages import ema
from models.stock_scoring_model import compute_sniper_score
from utils.symbol_loader import load_symbol_frames


SNIPER_HISTORY_BARS = 160
SNIPER_PROJECTION = {
    "symbol": 1,
    "date": 1,
    "close": 1,
    "volume": 1,
}



def run_sniper_pipeline():
    symbol_frames = load_symbol_frames(
        "ohlcv_equities",
        projection=SNIPER_PROJECTION,
        limit_per_symbol=SNIPER_HISTORY_BARS,
        max_workers=8,
    )

    if not symbol_frames:
        raise RuntimeError("ohlcv_equities collection is empty")

    sniper_df = scan_universe_sniper(symbol_frames)

    df_to_mongo(sniper_df, "sniper_candidates")

    ranked_records = []

    for symbol in sniper_df.loc[
        sniper_df["sniper_candidate"] == True, "symbol"
    ]:
        symbol_df = symbol_frames[symbol].copy()

        symbol_df["ema_20"] = ema(symbol_df["close"], 20)
        symbol_df["ema_50"] = ema(symbol_df["close"], 50)

        score = compute_sniper_score(symbol_df)

        if score is not None:
            ranked_records.append({
                "symbol": symbol,
                "sniper_score": score
            })

    if not ranked_records:
        return

    ranked_df = (
        pd.DataFrame(ranked_records)
        .sort_values("sniper_score", ascending=False)
        .reset_index(drop=True)
    )

    ranked_df["rank"] = ranked_df.index + 1

    df_to_mongo(ranked_df, "sniper_ranked")

    print("Sniper pipeline completed.")


if __name__ == "__main__":
    run_sniper_pipeline()
