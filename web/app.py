from fastapi import FastAPI
from web.routes.dashboard import router as dashboard_router
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="QuantFusion — VCP Market Intelligence")
app.mount("/static",StaticFiles(directory="web/static"),name="static")
app.include_router(dashboard_router)
