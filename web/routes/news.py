from fastapi import APIRouter, Request, Form, UploadFile, File, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from web.services.news_service import (
    create_news_article,
    fetch_news_paginated,
    fetch_single_news
)

from web.services.role_guard import role_required
from utils.cloudinary_uploader import upload_image

router = APIRouter()
templates = Jinja2Templates(directory="web/templates")



# NEWS LIST PAGE (PUBLIC)


@router.get("/news", response_class=HTMLResponse)
def news_page(request: Request, page: int = Query(1)):

    per_page = 6
    news, total = fetch_news_paginated(page, per_page)
    total_pages = max(1, -(-total // per_page))
    page = max(1, min(page, total_pages))

    return templates.TemplateResponse(
        "news.html",
        {
            "request": request,
            "news": news,
            "page": page,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages
        }
    )



# SINGLE ARTICLE PAGE (PUBLIC)


@router.get("/news/{slug}", response_class=HTMLResponse)
def news_detail(request: Request, slug: str):

    article = fetch_single_news(slug)

    if not article:
        return RedirectResponse("/news", status_code=302)

    return templates.TemplateResponse(
        "news_detail.html",
        {"request": request, "article": article}
    )



# ADMIN CREATE PAGE (ADMIN ONLY)


@router.get("/admin/news", response_class=HTMLResponse)
def create_news_page(request: Request):

    # 🔐 Role Protection
    check = role_required(request, "admin")
    if check:
        return check

    return templates.TemplateResponse(
        "create_news.html",
        {"request": request}
    )



# ADMIN CREATE NEWS POST (ADMIN ONLY)


@router.post("/admin/news")
async def create_news(
    request: Request,
    title: str = Form(...),
    summary: str = Form(...),
    content: str = Form(...),
    category: str = Form(...),
    image: UploadFile = File(None)
):

    # 🔐 Role Protection
    check = role_required(request, "admin")
    if check:
        return check

    image_url = None

    if image:
        image_url = upload_image(image.file)

    slug = create_news_article({
        "title": title,
        "summary": summary,
        "content": content,
        "image_url": image_url,
        "category": category
    })

    return RedirectResponse(f"/news/{slug}", status_code=302)
