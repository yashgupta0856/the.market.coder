from fastapi import APIRouter, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.services.community_service import (
    create_post,
    fetch_all_posts
)

from utils.cloudinary_uploader import upload_image

router = APIRouter(tags=["Community"])
templates = Jinja2Templates(directory="web/templates")



# COMMUNITY PAGE


@router.get("/community", response_class=HTMLResponse)
def community_page(request: Request):
    posts = fetch_all_posts()
    return templates.TemplateResponse(
        "community.html",
        {
            "request": request,
            "posts": posts
        }
    )



# CREATE COMMUNITY POST


@router.post("/api/community/create")
async def create_community_post(
    symbol: str = Form(...),
    entry: str = Form(None),
    stop_loss: str = Form(None),
    target: str = Form(None),
    commentary: str = Form(None),
    image: UploadFile = File(None),
):
    image_url = None

    # Upload image to Cloudinary
    if image:
        image_url = upload_image(image.file)

    create_post({
        "symbol": symbol,
        "entry": entry,
        "stop_loss": stop_loss,
        "target": target,
        "commentary": commentary,
        "image_path": image_url,   # Cloud URL
    })

    return {
        "status": "success",
        "symbol": symbol
    }
