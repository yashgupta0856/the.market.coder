from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from utils.mongo import get_collection


def get_system_stats():
    """
    Fetch dashboard system stats using parallel queries and fast
    estimated counts where exact values are not required.
    """

    def _count_universe():
        return get_collection("equity_universe").estimated_document_count()

    def _count_vcp():
        # Needs a filter, so must use count_documents
        return get_collection("vcp_candidates").count_documents(
            {"vcp_candidate": True}
        )

    def _count_final():
        return get_collection("final_stock_scores").estimated_document_count()

    def _get_last_run():
        col = get_collection("final_stock_scores")
        last_doc = col.find_one(sort=[("_id", -1)])
        if last_doc:
            return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return "N/A"

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_uni = pool.submit(_count_universe)
        f_vcp = pool.submit(_count_vcp)
        f_final = pool.submit(_count_final)
        f_run = pool.submit(_get_last_run)

    return {
        "universe_size": f_uni.result(),
        "vcp_candidates": f_vcp.result(),
        "final_ranked": f_final.result(),
        "last_run": f_run.result(),
    }
