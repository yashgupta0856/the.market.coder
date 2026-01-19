# utils/mongo.py

import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")

if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI not found in environment variables")

_client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=30000,
    connectTimeoutMS=20000,
    socketTimeoutMS=20000,
    tls=True,
    retryWrites=True,
)

_db = _client.get_database()  # uses db name from URI (quantfusion)


def get_db():
    return _db


def get_collection(name: str):
    return _db[name]
