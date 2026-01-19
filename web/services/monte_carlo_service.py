from utils.mongo import get_collection


def get_monte_carlo_for_symbol(symbol: str):
    mc_col = get_collection("monte_carlo")

    doc = mc_col.find_one(
        {"symbol": symbol},
        {"_id": 0}   
    )

    return doc
