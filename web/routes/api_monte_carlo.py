from fastapi import APIRouter
from web.services.monte_carlo_service import get_monte_carlo_for_symbol

router = APIRouter(prefix="/api/monte-carlo", tags=["Monte Carlo"])


@router.get("/{symbol}")
def monte_carlo(symbol: str):
    data = get_monte_carlo_for_symbol(symbol)

    if not data:
        return {"status": "not_found"}

    return {
        "status": "success",
        "symbol": symbol,
        "metrics": data["metrics"],
        "paths": data["paths"],
    }