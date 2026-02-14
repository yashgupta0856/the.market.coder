from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
import os

from web.routes.dashboard import router as dashboard_router
from web.routes.community import router as community_router
from web.routes.api_monte_carlo import router as mc_router
from web.routes.auth import router as auth_router
from web.routes.news import router as news_router

from web.routes import api_chart 

app = FastAPI(title="the.market.coder — VCP Market Intelligence")


# SESSION MIDDLEWARE


SECRET_KEY = os.getenv("SESSION_SECRET", "the.market.coder-secret")

app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="the.market.coder_session",
    max_age=60 * 60 * 24 * 7  # 7 days
)


# STATIC FILES


app.mount("/static", StaticFiles(directory="web/static"), name="static")


# ROUTES


app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(community_router)
app.include_router(mc_router)
app.include_router(news_router)
app.include_router(api_chart.router)