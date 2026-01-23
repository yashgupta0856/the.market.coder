from datetime import datetime, timedelta, timezone
from typing import Optional
from utils.mongo import get_collection


users_col = get_collection("users")


def create_user(email: str, role: str = "user") -> dict:
    user = {
        "email": email,
        "role": role,
        "plan": "free",
        "access_expires_at": None,
        "has_community_access": False,
        "community_expires_at": None,
        "created_at": datetime.now(timezone.utc),
        "last_login": None,
    }

    users_col.insert_one(user)
    return user


def get_user_by_email(email: str) -> Optional[dict]:
    return users_col.find_one(
        {"email": email},
        {"_id": 0}
    )


def user_has_active_access(user: dict) -> bool:
    if not user:
        return False

    if user.get("role") == "admin":
        return True

    if user.get("plan") != "premium":
        return False

    expires = user.get("access_expires_at")
    if not expires:
        return False

    return datetime.now(timezone.utc) <= expires


def grant_premium_access(email: str, days: int = 30):
    expires_at = datetime.now(timezone.utc) + timedelta(days=days)

    users_col.update_one(
        {"email": email},
        {
            "$set": {
                "plan": "premium",
                "access_expires_at": expires_at
            }
        }
    )


def update_last_login(email: str):
    users_col.update_one(
        {"email": email},
        {"$set": {"last_login": datetime.now(timezone.utc)}}
    )


def grant_community_access(email: str, months: int = 2):
    expiry = datetime.now(timezone.utc) + timedelta(days=30 * months)

    users_col.update_one(
        {"email": email},
        {
            "$set": {
                "has_community_access": True,
                "community_expires_at": expiry
            }
        }
    )