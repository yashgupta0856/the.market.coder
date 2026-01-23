from datetime import datetime, timezone
from utils.mongo import get_collection


def check_community_access(email: str) -> bool:
    users = get_collection("users")

    user = users.find_one(
        {"email": email},
        {
            "_id": 0,
            "has_community_access": 1,
            "community_expires_at": 1
        }
    )

    if not user:
        return False

    if not user.get("has_community_access"):
        return False

    expires_at = user.get("community_expires_at")

    if not expires_at:
        return False

    #  FORCE UTC-AWARE COMPARISON
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)

    if expires_at < now:
        #  Auto-revoke access
        users.update_one(
            {"email": email},
            {
                "$set": {
                    "has_community_access": False,
                    "community_expires_at": None
                }
            }
        )

        #  Expire stale payment requests
        payments = get_collection("payment_requests")
        payments.update_many(
            {"email": email, "status": "PENDING"},
            {"$set": {"status": "EXPIRED"}}
        )

        return False

    return True