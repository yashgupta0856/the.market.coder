from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
import os
import time

from utils.logger import setup_logging, get_logger

# Initialize structured logging before anything else
setup_logging()
logger = get_logger("app")

from web.routes.dashboard import router as dashboard_router
from web.routes.community import router as community_router
from web.routes.api_monte_carlo import router as mc_router
from web.routes.api_fundamentals import router as fund_router
from web.routes.auth import router as auth_router
from web.routes.news import router as news_router
from web.routes.api_sniper import router as sniper_router

from web.routes import api_chart 

app = FastAPI(title="the.market.coder — VCP Market Intelligence")
templates = Jinja2Templates(directory="web/templates")


# SESSION MIDDLEWARE — crash if secret is missing


SECRET_KEY = os.getenv("SESSION_SECRET")
if not SECRET_KEY:
    raise RuntimeError("SESSION_SECRET not found in environment variables. Refusing to start with an insecure default.")

app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="the.market.coder_session",
    max_age=60 * 60 * 24 * 7  # 7 days
)


# REQUEST LOGGING MIDDLEWARE


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration_ms = round((time.time() - start) * 1000)
    logger.info(f"{request.method} {request.url.path} → {response.status_code} ({duration_ms}ms)")
    return response


# STATIC FILES


app.mount("/static", StaticFiles(directory="web/static"), name="static")


# CUSTOM ERROR HANDLERS


@app.exception_handler(404)
async def not_found_handler(request: Request, exc: StarletteHTTPException):
    return templates.TemplateResponse(
        "404.html",
        {"request": request},
        status_code=404
    )


@app.exception_handler(500)
async def server_error_handler(request: Request, exc: StarletteHTTPException):
    logger.error(f"500 Internal Server Error on {request.method} {request.url.path}")
    return templates.TemplateResponse(
        "500.html",
        {"request": request},
        status_code=500
    )


# ROUTES


app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(community_router)
app.include_router(mc_router)
app.include_router(fund_router)
app.include_router(news_router)
app.include_router(sniper_router)
app.include_router(api_chart.router)

logger.info("the.market.coder application started successfully")
