from utils.mongo import get_collection


def get_top_rotating_sectors(limit=10):
    """
    Fetch top rotating sectors ordered by rotation_rank (ascending).
    Reads ONLY from MongoDB.
    """

    collection = get_collection("sector_rotation")

    cursor = (
        collection
        .find(
            {},
            {
                "_id": 0,
                "sector_index": 1,
                "rotation_rank": 1
            }
        )
        .sort("rotation_rank", 1)
        .limit(limit)
    )

    return list(cursor)
