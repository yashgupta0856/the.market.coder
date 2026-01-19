from utils.mongo import get_collection


def get_top_sectors_by_regime():
    rs_col = get_collection("sector_strength")
    regime_col = get_collection("sector_regime")

    # Build sector_index -> regime map
    regime_map = {
        doc["sector_index"]: doc["sector_regime"]
        for doc in regime_col.find(
            {},
            {"_id": 0, "sector_index": 1, "sector_regime": 1}
        )
    }

    result = {
        "leading": [],
        "improving": [],
        "weakening": [],
        "lagging": [],
    }

    regime_config = {
        "LEADING": ("leading", 5),
        "IMPROVING": ("improving", 5),
        "WEAKENING": ("weakening", 5),
        "LAGGING": ("lagging", 5),
    }

    # Fetch sector strength data
    rs_docs = list(
        rs_col.find(
            {},
            {"_id": 0, "sector_index": 1, "rs_score": 1}
        )
    )

    # Attach regime to each record
    for doc in rs_docs:
        doc["sector_regime"] = regime_map.get(doc["sector_index"])

    # Split & rank by regime
    for regime, (key, limit) in regime_config.items():
        subset = [
            {
                "sector_index": d["sector_index"],
                "rs_score": d["rs_score"],
            }
            for d in rs_docs
            if d.get("sector_regime") == regime
        ]

        subset = sorted(
            subset,
            key=lambda x: x["rs_score"],
            reverse=True
        )[:limit]

        result[key] = subset

    return result
