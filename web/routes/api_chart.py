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

    data.reverse()

    candles = [
        {
            "time": normalize_date(d["date"]),
            "open": float(d["open"]),
            "high": float(d["high"]),
            "low": float(d["low"]),
            "close": float(d["close"]),
        }
        for d in data
    ]

    volume = [
        {
            "time": normalize_date(d["date"]),
            "value": float(d["volume"]),
            "color": "#26a69a" if d["close"] >= d["open"] else "#ef5350",
        }
        for d in data
    ]

    return {
        "status": "success",
        "symbol": symbol,
        "candles": candles,
        "volume": volume,
    }
