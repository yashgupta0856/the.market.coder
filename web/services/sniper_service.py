from utils.mongo import get_collection


def get_sniper_stocks(limit=2000):
    sniper_col = get_collection("sniper_candidates")
    ohlcv_col = get_collection("ohlcv_equities")

    sniper_symbols = [
        doc["symbol"]
        for doc in sniper_col.find(
            {"sniper_candidate": True},
            {"_id": 0, "symbol": 1}
        ).limit(limit)
    ]

    if not sniper_symbols:
        return []

    result = []

    for symbol in sniper_symbols:
        latest = ohlcv_col.find_one(
            {"symbol": symbol},
            sort=[("date", -1)],
            projection={"_id": 0, "close": 1, "date": 1}
        )

        if not latest:
            continue

        result.append({
            "symbol": symbol,
            "price": round(latest["close"], 2),
            "date": latest["date"]
        })

    return result