from fastapi import APIRouter, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from web.services.community_service import (
    create_post,
    fetch_all_posts
)

from utils.cloudinary_uploader import upload_image

router = APIRouter(tags=["Community"])
templates = Jinja2Templates(directory="web/templates")

from fastapi.responses import RedirectResponse
from web.services.access_control import user_has_active_access


# COMMUNITY PAGE (READ ACCESS — LOGIN + PAYMENT REQUIRED)

@router.get("/community", response_class=HTMLResponse)
def community_page(request: Request):
    email = request.session.get("user_email")
    if not email:
        return RedirectResponse("/login", status_code=302)

    if not user_has_active_access(email):
        return RedirectResponse("/renew", status_code=302)

    posts = fetch_all_posts()

    return templates.TemplateResponse(
        "community.html",
        {
            "request": request,
            "posts": posts,
            "user_email": email
        }
    )



# CREATE COMMUNITY POST (NO AUTH — SYSTEM / OWNER ONLY)


@router.post("/api/community/create")
async def create_community_post(
    symbol: str = Form(...),
    entry: str = Form(None),
    stop_loss: str = Form(None),
    target: str = Form(None),
    commentary: str = Form(None),
    image: UploadFile = File(None),
):

    """
    IMPORTANT:
    - This endpoint is intentionally UNPROTECTED.
    - Community posts are curated and added only by the system owner.
    - Do NOT add login / admin checks here.
    """

    image_url = None

    if image:
        image_url = upload_image(image.file)

    create_post({
        "symbol": symbol,
        "entry": entry,
        "stop_loss": stop_loss,
        "target": target,
        "commentary": commentary,
        "image_path": image_url,
        "author": "the.market.coder"
    })

    return {
        "status": "success",
        "symbol": symbol
    }