from utils.mongo import get_collection

def df_to_mongo(
    df,
    collection_name: str,
    clear_existing: bool = True,
    batch_size: int = 1000
):
    if df is None or df.empty:
        return 0

    col = get_collection(collection_name)

    if clear_existing:
        col.delete_many({})

    records = df.to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        col.insert_many(records[i:i + batch_size])

    return len(records)
