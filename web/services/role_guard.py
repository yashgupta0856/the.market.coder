from fastapi.responses import RedirectResponse
from utils.mongo import get_collection


def role_required(request, required_role: str):

    email = request.session.get("user_email")

    if not email:
        return RedirectResponse("/login", status_code=302)

    users = get_collection("users")
    user = users.find_one({"email": email})

    if not user:
        return RedirectResponse("/login", status_code=302)

    if user.get("role") != required_role:
        return RedirectResponse("/", status_code=302)

    return None  # allowed
