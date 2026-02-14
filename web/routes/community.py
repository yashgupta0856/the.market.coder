from fastapi import APIRouter, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime

from utils.mongo import get_collection
from utils.cloudinary_uploader import upload_image
from web.services.access_control import user_has_active_access
from web.services.role_guard import role_required

router = APIRouter(tags=["Community"])
templates = Jinja2Templates(directory="web/templates")



# COMMUNITY PAGE (LOGIN + ACTIVE ACCESS REQUIRED)


@router.get("/community", response_class=HTMLResponse)
def community_page(request: Request):

    email = request.session.get("user_email")

    # Must be logged in
    if not email:
        return RedirectResponse("/login", status_code=302)

    # Must have active subscription
    if not user_has_active_access(email):
        return RedirectResponse("/renew", status_code=302)

    col = get_collection("community_posts")

    posts = list(
        col.find({}, {"_id": 0})
        .sort("created_at", -1)
    )

    return templates.TemplateResponse(
        "community.html",
        {
            "request": request,
            "posts": posts,
            "user_email": email
        }
    )



# CREATE COMMUNITY POST (ADMIN ONLY)


@router.post("/api/community/create")
async def create_community_post(
    request: Request,
    symbol: str = Form(...),
    entry: str = Form(None),
    stop_loss: str = Form(None),
    target: str = Form(None),
    commentary: str = Form(None),
    image: UploadFile = File(None),
):
    """
    ADMIN ONLY:
    Creates new community research post.
    """

    #  Role-based protection
    check = role_required(request, "admin")
    if check:
        return check

    image_url = None

    if image:
        image_url = upload_image(image.file)

    col = get_collection("community_posts")

    col.insert_one({
        "symbol": symbol,
        "entry": entry,
        "stop_loss": stop_loss,
        "target": target,
        "commentary": commentary,
        "image_path": image_url,
        "author": "the.market.coder",
        "created_at": datetime.utcnow()
    })

    return {
        "status": "success",
        "symbol": symbol
    }



# ADMIN COMMUNITY CREATE PAGE (ADMIN ONLY)


@router.get("/admin/community", response_class=HTMLResponse)
def create_community_page(request: Request):

    check = role_required(request, "admin")
    if check:
        return check

    return templates.TemplateResponse(
        "create_community.html",
        {"request": request}
    )
