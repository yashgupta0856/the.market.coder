from fastapi import APIRouter, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
import os
import shutil

from web.services.community_service import (
    create_post,
    fetch_all_posts
)

router = APIRouter(tags=["Community"])
templates = Jinja2Templates(directory="web/templates")

UPLOAD_DIR = "web/static/community_uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.get("/community", response_class=HTMLResponse)
def community_page(request: Request):
    posts = fetch_all_posts()
    return templates.TemplateResponse(
        "community.html",
        {"request": request, "posts": posts}
    )


@router.post("/api/community/create")
async def create_community_post(
    symbol: str = Form(...),
    entry: str = Form(None),
    stop_loss: str = Form(None),
    target: str = Form(None),
    commentary: str = Form(None),
    image: UploadFile = File(None),
):
    image_path = None

    if image:
        filename = f"{datetime.utcnow().timestamp()}_{image.filename}"
        file_path = os.path.join(UPLOAD_DIR, filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(image.file, buffer)
        image_path = f"/static/community_uploads/{filename}"

    create_post({
        "symbol": symbol,
        "entry": entry,
        "stop_loss": stop_loss,
        "target": target,
        "commentary": commentary,
        "image_path": image_path,
    })

    return {"status": "success", "symbol": symbol}
