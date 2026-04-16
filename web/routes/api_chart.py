from fastapi import APIRouter
from utils.mongo import get_collection

router = APIRouter(prefix="/api/chart", tags=["Chart"])


def normalize_date(d):
    if isinstance(d, str):
        return d[:10]
    return d.strftime("%Y-%m-%d")


@router.get("/{symbol}")
def get_chart_data(symbol: str, limit: int = 150):
    col = get_collection("ohlcv_equities")

    cursor = (
        col.find(
            {"symbol": symbol},
            {
                "_id": 0,
                "date": 1,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
            }
        )
        .sort("date", -1)
        .limit(limit)
    )

    data = list(cursor)
    if not data:
        return {"status": "not_found"}

    # Deduplicate by date (data is already sorted descending, so keep the first we see)
    seen_dates = set()
    unique_data = []
    for d in data:
        dt_str = normalize_date(d["date"])
        if dt_str not in seen_dates:
            seen_dates.add(dt_str)
            unique_data.append(d)

    # Reverse to make chronological for LightweightCharts
    unique_data.reverse()

    candles = [
        {
            "time": normalize_date(d["date"]),
            "open": float(d["open"]),
            "high": float(d["high"]),
            "low": float(d["low"]),
            "close": float(d["close"]),
        }
        for d in unique_data
    ]

    volume = [
        {
            "time": normalize_date(d["date"]),
            "value": float(d["volume"]),
            "color": "#26a69a" if d["close"] >= d["open"] else "#ef5350",
        }
        for d in unique_data
    ]

    return {
        "status": "success",
        "symbol": symbol,
        "candles": candles,
        "volume": volume,
    }
