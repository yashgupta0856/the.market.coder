from utils.mongo import get_collection


def get_ranked_vcp_stocks(limit=20):
    col = get_collection("final_stock_scores")

    cursor = (
        col.find(
            {},
            {
                "_id": 0, 
                "symbol": 1, 
                "close": 1, 
                "rank": 1,
                "vcp_quality": 1,
                "contraction_count": 1,
                "granular_sector": 1,
                "granular_subsector": 1,
                "fundamental_grade": 1,
                "fundamental_score": 1
            }
        )
        .sort("rank", 1)
        .limit(limit)
    )

    return list(cursor)


def get_sector_wise_vcp_counts(limit=5):
    col = get_collection("final_stock_scores")

    pipeline = [
        {"$match": {"vcp_candidate": True}},
        {
            "$group": {
                "_id": "$granular_sector",
                "vcp_count": {"$sum": 1}
            }
        },
        {"$sort": {"vcp_count": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "granular_sector": "$_id",
                "vcp_count": 1
            }
        }
    ]

    return list(col.aggregate(pipeline))


def get_remaining_vcp_symbols():
    vcp_col = get_collection("vcp_candidates")
    final_col = get_collection("final_stock_scores")

    vcp_true_symbols = {
        doc["symbol"]
        for doc in vcp_col.find(
            {"vcp_candidate": True},
            {"_id": 0, "symbol": 1}
        )
    }

    final_symbols = {
        doc["symbol"]
        for doc in final_col.find(
            {},
            {"_id": 0, "symbol": 1}
        )
    }

    return sorted(list(vcp_true_symbols - final_symbols))
