from datetime import datetime, timezone
from fastapi import Request
from fastapi.responses import RedirectResponse
from utils.mongo import get_collection


def require_community_access(request: Request):
    email = request.session.get("user_email")

    if not email:
        return RedirectResponse("/login", status_code=302)

    users = get_collection("users")

    user = users.find_one(
        {"email": email},
        {
            "_id": 0,
            #"has_community_access": 1,
            #"community_expires_at": 1
        }
    )

    if not user or not user.get(#"has_community_access"):
        return RedirectResponse("/payment-page", status_code=302)

    expiry = user.get(#"community_expires_at")
    if not expiry:
        return RedirectResponse("/payment-page", status_code=302)

    if expiry < datetime.now(timezone.utc):
        users.update_one(
            {"email": email},
            {"$set": {#"has_community_access": False}}
        )
        return RedirectResponse("/payment-page", status_code=302)

    return None