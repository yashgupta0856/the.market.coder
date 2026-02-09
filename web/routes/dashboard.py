from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.services.system_service import get_system_stats
from web.services.sector_service import get_top_sectors_by_regime
from web.services.stock_service import (
    get_ranked_vcp_stocks,
    get_sector_wise_vcp_counts,
    get_remaining_vcp_symbols
)
from web.services.explainability_service import get_rank1_stock_explanation
from web.services.sector_rotation_service import get_top_rotating_sectors
from web.services.monte_carlo_service import get_monte_carlo_for_symbol
from web.services.sniper_service import get_sniper_stocks,get_ranked_sniper_stocks

from fastapi.responses import RedirectResponse
from web.services.access_control import user_has_active_access

router = APIRouter()
templates = Jinja2Templates(directory="web/templates")


# Homepage
@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "home.html",
        {"request": request}
    )


# Dashboard
@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):

    email = request.session.get("user_email")
    if not email:
        return RedirectResponse("/login", status_code=302)

    if not user_has_active_access(email):
        return RedirectResponse("/renew", status_code=302)



    system_stats = get_system_stats()
    sector_cards = get_top_sectors_by_regime()
    ranked_stocks = get_ranked_vcp_stocks(limit=100)
    rank1_left, rank1_right = get_rank1_stock_explanation()
    sector_vcp_counts = get_sector_wise_vcp_counts(limit=10)
    remaining_vcp_stocks = get_remaining_vcp_symbols()
    rotating_sectors = get_top_rotating_sectors(limit=10)
    sniper_stocks = get_sniper_stocks(limit=2000)
    sniper_ranked = get_ranked_sniper_stocks(limit=2000)

    mc_data = None
    if rank1_left:
        mc_data = get_monte_carlo_for_symbol(rank1_left["symbol"])

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "system_stats": system_stats,
            "sector_cards": sector_cards,
            "ranked_stocks": ranked_stocks,
            "rank1_left": rank1_left,
            "rank1_right": rank1_right,
            "sector_vcp_counts": sector_vcp_counts,
            "remaining_vcp_stocks": remaining_vcp_stocks,
            "rotating_sectors": rotating_sectors,
            "mc_data": mc_data,   
            "sniper_stocks":sniper_stocks,
            "sniper_ranked":sniper_ranked
        }
    )