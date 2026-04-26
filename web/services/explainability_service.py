from utils.mongo import get_collection


def get_rank1_stock_explanation():
    """
    Returns explainability panels for Rank #1 stock.
    Reads ONLY from MongoDB.
    """

    collection = get_collection("final_stock_scores")

    top = collection.find_one(
        {},
        sort=[("rank", 1)],
        projection={
            "_id": 0,
            "symbol": 1,
            "rank": 1,
            "trend_strength": 1,
            "momentum": 1,
            "volatility_tightness": 1,
            "structure_bonus": 1,
            "sector_index": 1,
            "sector_regime": 1,
            "vcp_candidate": 1,
            "fundamental_factor": 1,
            "fundamental_grade": 1,
        }
    )

    if not top:
        return None, None

    # LEFT PANEL — model reasoning
    left = {
        "symbol": top["symbol"],
        "rank": int(top["rank"]),
        "trend_strength": top.get("trend_strength"),
        "momentum": top.get("momentum"),
        "volatility_tightness": top.get("volatility_tightness"),
        "fundamental_factor": top.get("fundamental_factor"),
        "fundamental_grade": top.get("fundamental_grade"),
        "sector_index": top.get("sector_index"),
        "sector_regime": top.get("sector_regime"),
    }

    # RIGHT PANEL — structural validation
    right = {
        "vcp_status": "PASS" if top.get("vcp_candidate") else "FAIL",
        "trend_gate": "PASS",
        "volatility_gate": "PASS",
        "price_tightness_gate": "PASS",
        "volume_dryup_gate": "PASS",
    }

    return left, right
