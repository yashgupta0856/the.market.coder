import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_URI_SECONDARY = os.getenv("MONGODB_URI_SECONDARY")

if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI not found in environment variables")

_client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=30000,
    connectTimeoutMS=30000,
    socketTimeoutMS=30000,
    tls=True,
    retryWrites=True,
)
_db = _client.get_database()  

_client2 = None
_db2 = None

if MONGODB_URI_SECONDARY:
    _client2 = MongoClient(
        MONGODB_URI_SECONDARY,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
        tls=True,
        retryWrites=True,
    )
    _db2 = _client2.get_database("quantfusion_analytics")

SECONDARY_COLLECTIONS = {
    "equity_indicators", "benchmark_indicators",
    "vcp_candidates", "sniper_candidates", "sniper_ranked",
    "sector_indicators", "sector_strength", "sector_regime",
    "stock_sector_fused", "vcp_sector_filtered", "sector_rotation",
    "final_stock_scores", "monte_carlo"
}

def get_db():
    return _db

def get_collection(name: str):
    if _db2 is not None and name in SECONDARY_COLLECTIONS:
        return _db2[name]
    return _db[name]
