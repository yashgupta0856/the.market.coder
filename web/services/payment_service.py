from datetime import datetime
from utils.mongo import get_collection


def create_payment_request(email: str, image_url: str):
    col = get_collection("payment_requests")

    col.insert_one({
        "email": email,
        "image_url": image_url,
        "status": "PENDING",
        "created_at": datetime.utcnow()
    })


def get_latest_payment_request(email: str):
    col = get_collection("payment_requests")

    return col.find_one(
        {"email": email},
        sort=[("created_at", -1)],
        projection={"_id": 0}
    )
