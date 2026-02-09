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



def get_ranked_sniper_stocks(limit=2000):
    ranked_col = get_collection("sniper_ranked")
    ohlcv_col = get_collection("ohlcv_equities")

    ranked = list(
        ranked_col.find(
            {},
            {"_id": 0, "symbol": 1, "rank": 1, "sniper_score": 1}
        )
        .sort("rank", 1)
        .limit(limit)
    )

    if not ranked:
        return []

    symbols = [r["symbol"] for r in ranked]

    #  Get latest close per symbol
    price_docs = ohlcv_col.aggregate([
        {"$match": {"symbol": {"$in": symbols}}},
        {"$sort": {"date": -1}},
        {
            "$group": {
                "_id": "$symbol",
                "close": {"$first": "$close"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "symbol": "$_id",
                "close": 1
            }
        }
    ])

    price_map = {
        doc["symbol"]: round(doc["close"], 2)
        for doc in price_docs
    }

    for r in ranked:
        r["price"] = price_map.get(r["symbol"])

    return ranked

