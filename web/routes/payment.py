# from fastapi import APIRouter, Request, UploadFile, File
# from fastapi.responses import HTMLResponse, RedirectResponse
# from fastapi.templating import Jinja2Templates

# from utils.cloudinary_uploader import upload_image
# from web.services.payment_service import (
#     create_payment_request,
#     get_latest_payment_request
# )
# from web.services.access_control import check_community_access

# router = APIRouter()
# templates = Jinja2Templates(directory="web/templates")


# @router.get("/payment-page", response_class=HTMLResponse)
# def payment_page(request: Request):
#     email = request.session.get("user_email")

#     if not email:
#         return RedirectResponse("/login", status_code=302)

#     #  RULE 1: ACCESS ALWAYS WINS
#     if check_community_access(email):
#         return RedirectResponse("/community", status_code=302)

#     #  RULE 2: ONLY SHOW "WAIT 1 HOUR" IF PAYMENT IS *CURRENTLY* PENDING
#     pending = False
#     latest = get_latest_payment_request(email)

#     if latest and latest.get("status") == "PENDING":
#         pending = True

#     return templates.TemplateResponse(
#         "payment.html",
#         {
#             "request": request,
#             "pending": pending
#         }
#     )


# @router.post("/payment/upload")
# async def upload_payment_proof(
#     request: Request,
#     image: UploadFile = File(...)
# ):
#     email = request.session.get("user_email")

#     if not email:
#         return RedirectResponse("/login", status_code=302)

#     image_url = upload_image(image.file)

#     create_payment_request(
#         email=email,
#         image_url=image_url
#     )

#     return RedirectResponse("/community", status_code=302)