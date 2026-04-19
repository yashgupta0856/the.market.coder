from fastapi import APIRouter

from web.services.sniper_service import get_ranked_sniper_stocks

router = APIRouter(prefix="/api/sniper", tags=["Sniper"])


@router.get("/ranked")
def get_ranked_sniper(limit: int = 2000):
    return {
        "status": "success",
        "rows": get_ranked_sniper_stocks(limit),
    }
