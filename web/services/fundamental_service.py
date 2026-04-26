"""
Fundamental analysis dashboard service.

Reads from the ``equity_fundamentals`` MongoDB collection (populated
by Phase 1.5) and returns data suitable for the web dashboard.
"""

from utils.mongo import get_collection


def get_top_fundamental_stocks(limit: int = 10) -> list[dict]:
    """
    Return the top stocks by fundamental_score.

    Each record contains: symbol, fundamental_score, fundamental_grade,
    trailing_pe, roe, revenue_growth, market_cap.
    """
    col = get_collection("equity_fundamentals")

    cursor = (
        col.find(
            {"fundamental_score": {"$exists": True}},
            {
                "_id": 0,
                "symbol": 1,
                "fundamental_score": 1,
                "fundamental_grade": 1,
                "trailing_pe": 1,
                "roe": 1,
                "revenue_growth": 1,
                "market_cap": 1,
            },
        )
        .sort("fundamental_score", -1)
        .limit(limit)
    )

    return list(cursor)


def get_fundamental_for_symbol(symbol: str) -> dict | None:
    """
    Return fundamental data for a single symbol.
    Used for the Fund. popup on the dashboard.
    """
    col = get_collection("equity_fundamentals")

    doc = col.find_one(
        {"symbol": symbol},
        {"_id": 0, "fetched_at": 0},
    )

    return doc


def get_fundamental_overview() -> dict:
    """
    Return aggregate fundamental stats for the universe.
    Used for summary cards on the dashboard.
    """
    col = get_collection("equity_fundamentals")

    pipeline = [
        {"$match": {"fundamental_score": {"$exists": True}}},
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "avg_score": {"$avg": "$fundamental_score"},
                "avg_pe": {"$avg": "$trailing_pe"},
                "avg_roe": {"$avg": "$roe"},
                "avg_growth": {"$avg": "$revenue_growth"},
            }
        },
    ]

    result = list(col.aggregate(pipeline))

    if not result:
        return {
            "total": 0,
            "avg_score": 0,
            "avg_pe": 0,
            "avg_roe": 0,
            "avg_growth": 0,
        }

    doc = result[0]
    doc.pop("_id", None)
    return doc


def get_grade_distribution() -> dict:
    """
    Return count of stocks per fundamental grade (A/B/C/D/F).
    """
    col = get_collection("equity_fundamentals")

    pipeline = [
        {"$match": {"fundamental_grade": {"$exists": True}}},
        {
            "$group": {
                "_id": "$fundamental_grade",
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    result = list(col.aggregate(pipeline))

    distribution = {grade: 0 for grade in ["A", "B", "C", "D", "F"]}
    for doc in result:
        if doc["_id"] in distribution:
            distribution[doc["_id"]] = doc["count"]

    return distribution
