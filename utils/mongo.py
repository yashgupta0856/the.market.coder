# utils/mongo.py

import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")

if not MONGODB_URI:
    raise ValueError("MONGODB_URI not found in .env")

_client = MongoClient(MONGODB_URI)
_db = _client.get_default_database()  # quantfusion


def get_db():
    return _db


def get_collection(name: str):
    return _db[name]
