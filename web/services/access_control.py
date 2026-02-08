from datetime import datetime, timezone
from utils.mongo import get_collection

def user_has_active_access(email: str) -> bool:
    users = get_collection("users")
    user = users.find_one({"email": email})

    if not user:
        return False

    expires = user.get("access_expires_at")
    if not expires:
        return False

    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)

    return datetime.now(timezone.utc) <= expires
