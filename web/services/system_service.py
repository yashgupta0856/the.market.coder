from datetime import datetime, timezone
from utils.mongo import get_collection


def get_system_stats():
    stats = {}

    # Universe size
    universe_col = get_collection("equity_universe")
    stats["universe_size"] = universe_col.count_documents({})

    # VCP candidates
    vcp_col = get_collection("vcp_candidates")
    stats["vcp_candidates"] = vcp_col.count_documents(
        {"vcp_candidate": True}
    )

    # Final ranked stocks
    final_col = get_collection("final_stock_scores")
    stats["final_ranked"] = final_col.count_documents({})

    # Last run timestamp (based on latest document insertion)
    last_doc = final_col.find_one(sort=[("_id", -1)])

    if last_doc:
        stats["last_run"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )
    else:
        stats["last_run"] = "N/A"

    return stats
