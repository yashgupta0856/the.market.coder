from fastapi import APIRouter, Request, Form, File, UploadFile, BackgroundTasks
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from utils.mongo import get_collection
from utils.auth import hash_password, verify_password

from datetime import datetime, timedelta, timezone
from utils.cloudinary_uploader import upload_image
from utils.email import send_password_reset_email
import uuid


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
    # Password validation
    if len(password) < 8:
        return templates.TemplateResponse(
            "signup.html",
            {"request": request, "error": "Password must be at least 8 characters"}
        )

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

    expires_at = datetime.now(timezone.utc) + timedelta(weeks=4)

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



# FORGOT PASSWORD


@router.get("/forgot-password")
def forgot_password_page(request: Request):
    return templates.TemplateResponse(
        "forgot_password.html",
        {"request": request}
    )


@router.post("/forgot-password")
def forgot_password_submit(
    request: Request,
    background_tasks: BackgroundTasks,
    email: str = Form(...),
):
    users = get_collection("users")
    user = users.find_one({"email": email})

    # Always show success to prevent email enumeration
    success_msg = "If an account with that email exists, a password reset link has been sent."

    if user:
        token = str(uuid.uuid4())
        resets = get_collection("password_resets")

        resets.insert_one({
            "email": email,
            "token": token,
            "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
            "used": False,
            "created_at": datetime.now(timezone.utc)
        })

        # Build reset link using the request's base URL
        reset_link = f"{request.base_url}reset-password?token={token}"

        background_tasks.add_task(
            send_password_reset_email,
            to_email=email,
            reset_link=reset_link
        )

    return templates.TemplateResponse(
        "forgot_password.html",
        {"request": request, "success": success_msg}
    )


@router.get("/reset-password")
def reset_password_page(request: Request, token: str = ""):
    if not token:
        return templates.TemplateResponse(
            "reset_password.html",
            {"request": request, "valid_token": False}
        )

    resets = get_collection("password_resets")
    record = resets.find_one({
        "token": token,
        "used": False,
        "expires_at": {"$gt": datetime.now(timezone.utc)}
    })

    return templates.TemplateResponse(
        "reset_password.html",
        {
            "request": request,
            "valid_token": record is not None,
            "token": token
        }
    )


@router.post("/reset-password")
def reset_password_submit(
    request: Request,
    token: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
):
    # Validate passwords match
    if password != confirm_password:
        return templates.TemplateResponse(
            "reset_password.html",
            {"request": request, "valid_token": True, "token": token,
             "error": "Passwords do not match"}
        )

    # Validate password length
    if len(password) < 8:
        return templates.TemplateResponse(
            "reset_password.html",
            {"request": request, "valid_token": True, "token": token,
             "error": "Password must be at least 8 characters"}
        )

    resets = get_collection("password_resets")
    record = resets.find_one({
        "token": token,
        "used": False,
        "expires_at": {"$gt": datetime.now(timezone.utc)}
    })

    if not record:
        return templates.TemplateResponse(
            "reset_password.html",
            {"request": request, "valid_token": False,
             "error": "This reset link is invalid or has expired."}
        )

    # Update password
    users = get_collection("users")
    users.update_one(
        {"email": record["email"]},
        {"$set": {"password": hash_password(password)}}
    )

    # Mark token as used
    resets.update_one(
        {"_id": record["_id"]},
        {"$set": {"used": True}}
    )

    return RedirectResponse("/login", status_code=302)
