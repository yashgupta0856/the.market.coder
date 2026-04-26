from datetime import datetime, timezone
from utils.mongo import get_collection
import re


def generate_slug(title: str) -> str:
    slug = title.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug)
    return slug


def create_news_article(data: dict):
    col = get_collection("market_news")

    slug = generate_slug(data["title"])

    article = {
        "slug": slug,
        "title": data["title"],
        "summary": data.get("summary"),
        "content": data.get("content"),
        "image_url": data.get("image_url"),
        "author": "the.market.coder",
        "category": data.get("category", "market"),
        "published": True,
        "created_at": datetime.now(timezone.utc)
    }

    col.insert_one(article)

    return slug


def fetch_all_news():
    col = get_collection("market_news")
    return list(
        col.find({"published": True}, {"_id": 0})
        .sort("created_at", -1)
    )


def fetch_news_paginated(page: int = 1, per_page: int = 9):
    col = get_collection("market_news")
    total = col.count_documents({"published": True})

    posts = list(
        col.find({"published": True}, {"_id": 0})
        .sort("created_at", -1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    return posts, total


def fetch_single_news(slug: str):
    col = get_collection("market_news")
    return col.find_one(
        {"slug": slug, "published": True},
        {"_id": 0}
    )
