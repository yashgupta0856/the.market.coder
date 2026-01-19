from datetime import datetime
from utils.mongo import get_collection


def create_post(data: dict):
    col = get_collection("community_posts")
    data["created_at"] = datetime.utcnow()
    col.insert_one(data)


def fetch_all_posts():
    col = get_collection("community_posts")
    return list(
        col.find({}, {"_id": 0}).sort("created_at", -1)
    )
