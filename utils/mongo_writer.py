from pymongo import UpdateOne

from utils.mongo import get_collection


def df_to_mongo(
    df,
    collection_name: str,
    clear_existing: bool = True,
    batch_size: int = 5000,
):
    """
    Write a DataFrame to a MongoDB collection.

    Uses larger batch sizes and unordered inserts for maximum throughput.
    When clear_existing=True, the old data is deleted just before the first
    batch is inserted to minimise the empty-collection window.
    """
    if df is None or df.empty:
        return 0

    col = get_collection(collection_name)
    records = df.to_dict(orient="records")

    if clear_existing:
        col.delete_many({})

    for i in range(0, len(records), batch_size):
        col.insert_many(records[i : i + batch_size], ordered=False)

    return len(records)


def upsert_df_to_mongo(
    df,
    collection_name: str,
    key_fields: list[str],
    batch_size: int = 5000,
):
    """
    Upsert a DataFrame into a MongoDB collection keyed by *key_fields*.

    Uses unordered bulk_write for maximum throughput.
    """
    if df is None or df.empty:
        return 0

    col = get_collection(collection_name)
    records = df.to_dict(orient="records")

    operations = [
        UpdateOne(
            {field: record[field] for field in key_fields},
            {"$set": record},
            upsert=True,
        )
        for record in records
    ]

    for i in range(0, len(operations), batch_size):
        col.bulk_write(operations[i : i + batch_size], ordered=False)

    return len(records)
