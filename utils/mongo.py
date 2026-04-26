import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_URI_SECONDARY = os.getenv("MONGODB_URI_SECONDARY")

if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI not found in environment variables")

# ── Primary client (raw data + user collections) ──────────────────────
_client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=30000,
    connectTimeoutMS=30000,
    socketTimeoutMS=30000,
    tls=True,
    retryWrites=True,
    maxPoolSize=20,
    minPoolSize=5,
    maxIdleTimeMS=60000,
    compressors=["zstd", "snappy"],
)
_db = _client.get_database()

# ── Secondary client (computed analytics — dashboard reads) ───────────
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
        maxPoolSize=20,
        minPoolSize=5,
        maxIdleTimeMS=60000,
        compressors=["zstd", "snappy"],
    )
    _db2 = _client2.get_database("quantfusion_analytics")

# Collections routed to the secondary (analytics) database.
# Keeps heavy pipeline writes off the primary and gives the
# dashboard its own read pool.
SECONDARY_COLLECTIONS = {
    "equity_indicators", "benchmark_indicators",
    "equity_fundamentals",
    "vcp_candidates", "sniper_candidates", "sniper_ranked",
    "sector_indicators", "sector_strength", "sector_regime",
    "stock_sector_fused", "vcp_sector_filtered", "sector_rotation",
    "final_stock_scores", "monte_carlo",
    "ohlcv_latest",  # lightweight price cache for dashboard reads
}


def get_db():
    return _db


def get_secondary_db():
    """Return the secondary (analytics) database, falling back to primary."""
    return _db2 if _db2 is not None else _db


def get_collection(name: str):
    if _db2 is not None and name in SECONDARY_COLLECTIONS:
        return _db2[name]
    return _db[name]


def sync_latest_prices_to_secondary():
    """
    Copy the latest-date OHLCV snapshot to the secondary database so
    dashboard services never need to hit the primary during reads.
    """
    if _db2 is None:
        return

    primary_col = _db["ohlcv_equities"]
    latest_doc = primary_col.find_one(
        {}, {"date": 1, "_id": 0}, sort=[("date", -1)]
    )

    if not latest_doc:
        return

    docs = list(
        primary_col.find(
            {"date": latest_doc["date"]},
            {"_id": 0, "symbol": 1, "date": 1, "close": 1, "volume": 1},
        )
    )

    if docs:
        target = _db2["ohlcv_latest"]
        target.drop()
        target.insert_many(docs, ordered=False)
        target.create_index([("symbol", 1)], background=True)
        print(f"Synced {len(docs)} latest prices to secondary DB")
