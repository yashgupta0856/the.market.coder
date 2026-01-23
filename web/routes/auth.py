from datetime import datetime
from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from utils.mongo import get_collection
from utils.auth import hash_password, verify_password

router = APIRouter()
templates = Jinja2Templates(directory="web/templates")


@router.get("/signup")
def signup_page(request: Request):
    return templates.TemplateResponse("signup.html", {"request": request})


@router.post("/signup")
def signup_user(
    request: Request,
    email: str = Form(...),
    password: str = Form(...)
):
    users = get_collection("users")

    if users.find_one({"email": email}):
        return templates.TemplateResponse(
            "signup.html",
            {"request": request, "error": "Email already registered"}
        )

    users.insert_one({
        "email": email,
        "password": hash_password(password),
        "role": "user",
        "has_community_access": False,
        "community_expires_at": None,
        "created_at": datetime.utcnow(),
        "last_login": None
    })

    return RedirectResponse("/login", status_code=302)


@router.get("/login")
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@router.post("/login")
def login_user(
    request: Request,
    email: str = Form(...),
    password: str = Form(...)
):
    users = get_collection("users")
    user = users.find_one({"email": email})

    if not user or not verify_password(password, user["password"]):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid credentials"}
        )

    users.update_one(
        {"email": email},
        {"$set": {"last_login": datetime.utcnow()}}
    )

    request.session["user_email"] = email
    return RedirectResponse("/dashboard", status_code=302)