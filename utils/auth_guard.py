from fastapi import Request
from fastapi.responses import RedirectResponse


def require_login(request: Request):
    """
    Enforces login using server-side session.
    """
    user_email = request.session.get("user_email")

    if not user_email:
        return RedirectResponse(
            url="/login",
            status_code=302
        )

    return None
