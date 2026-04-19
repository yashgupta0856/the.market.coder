from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from concurrent.futures import ThreadPoolExecutor

from web.services.system_service import get_system_stats
from web.services.sector_service import get_top_sectors_by_regime
from web.services.stock_service import (
    get_ranked_vcp_stocks,
    get_sector_wise_vcp_counts,
    get_remaining_vcp_symbols
)
from web.services.explainability_service import get_rank1_stock_explanation
from web.services.sector_rotation_service import get_top_rotating_sectors

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

    # Run all independent service calls in parallel
    with ThreadPoolExecutor(max_workers=7) as pool:
        f_stats = pool.submit(get_system_stats)
        f_sectors = pool.submit(get_top_sectors_by_regime)
        f_ranked = pool.submit(get_ranked_vcp_stocks, 100)
        f_explain = pool.submit(get_rank1_stock_explanation)
        f_vcp_counts = pool.submit(get_sector_wise_vcp_counts, 10)
        f_remaining = pool.submit(get_remaining_vcp_symbols)
        f_rotating = pool.submit(get_top_rotating_sectors, 10)

    system_stats = f_stats.result()
    sector_cards = f_sectors.result()
    ranked_stocks = f_ranked.result()
    rank1_left, rank1_right = f_explain.result()
    sector_vcp_counts = f_vcp_counts.result()
    remaining_vcp_stocks = f_remaining.result()
    rotating_sectors = f_rotating.result()

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
        }
    )
