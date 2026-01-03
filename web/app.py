from fastapi import FastAPI
from web.routes.dashboard import router as dashboard_router
from fastapi.staticfiles import StaticFiles
from web.routes.community import router as community_router
from web.routes.api_monte_carlo import router as mc_router
from web.db.database import Base, engine

Base.metadata.create_all(bind=engine)

app = FastAPI(title="QuantFusion — VCP Market Intelligence")
app.mount("/static",StaticFiles(directory="web/static"),name="static")
app.include_router(dashboard_router)
app.include_router(community_router)
app.include_router(mc_router)
