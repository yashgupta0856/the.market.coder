import math

from fastapi import APIRouter
from web.services.fundamental_service import get_fundamental_for_symbol

router = APIRouter(prefix="/api/fundamentals", tags=["Fundamentals"])


def _sanitize(doc: dict) -> dict:
    """Replace NaN / Infinity floats with None so JSON is valid."""
    clean = {}
    for k, v in doc.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            clean[k] = None
        else:
            clean[k] = v
    return clean


@router.get("/{symbol}")
def fundamentals(symbol: str):
    data = get_fundamental_for_symbol(symbol)

    if not data:
        return {"status": "not_found"}

    return {
        "status": "success",
        "symbol": symbol,
        "metrics": _sanitize(data),
    }
