from fastapi import APIRouter, Request, Form, File, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from utils.mongo import get_collection
from utils.auth import hash_password, verify_password

from datetime import datetime, timedelta, timezone
from utils.cloudinary_uploader import upload_image


router = APIRouter()
templates = Jinja2Templates(directory="web/templates")


@router.get("/signup")
def signup_page(request: Request):
    return templates.TemplateResponse("signup.html", {"request": request})



@router.post("/signup")
async def signup_user(
    request: Request,
    name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    tradingview_id: str = Form(None),
    payment_proof: UploadFile = File(...)
):
    users = get_collection("users")

    if users.find_one({"email": email}):
        return templates.TemplateResponse(
            "signup.html",
            {"request": request, "error": "Email already registered"}
        )

    image_url = upload_image(payment_proof.file)

    expires_at = datetime.now(timezone.utc) + timedelta(weeks=4)

    users.insert_one({
        "name": name,
        "email": email,
        "password": hash_password(password),
        "role":"user",
        "tradingview_id": tradingview_id,
        "access_expires_at": expires_at,
        "created_at": datetime.now(timezone.utc),
        "last_login": None
    })

    payments = get_collection("payment_requests")
    payments.insert_one({
        "email": email,
        "image_url": image_url,
        "created_at": datetime.now(timezone.utc),
        "source": "signup"
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
        {"$set": {"last_login": datetime.now(timezone.utc)}}
    )

    request.session["user_email"] = email
    request.session["user_name"] = user.get("name", "User")

    return RedirectResponse("/dashboard", status_code=302)


@router.get("/logout")
def logout_user(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)



@router.get("/renew")
def renew_page(request: Request):
    email = request.session.get("user_email")
    if not email:
        return RedirectResponse("/login", status_code=302)

    return templates.TemplateResponse(
        "renew.html",
        {"request": request}
    )


@router.post("/renew")
async def renew_access(
    request: Request,
    payment_proof: UploadFile = File(...)
):
    email = request.session.get("user_email")
    if not email:
        return RedirectResponse("/login", status_code=302)

    image_url = upload_image(payment_proof.file)

    expires_at = datetime.now(timezone.utc) + timedelta(minutes=60)

    users = get_collection("users")
    users.update_one(
        {"email": email},
        {"$set": {"access_expires_at": expires_at}}
    )

    payments = get_collection("payment_requests")
    payments.insert_one({
        "email": email,
        "image_url": image_url,
        "created_at": datetime.now(timezone.utc),
        "source": "renew"
    })

    return RedirectResponse("/dashboard", status_code=302)
