# utils/mongo_loader.py

import pandas as pd
from utils.mongo import get_collection


def csv_to_mongo(
    csv_path: str,
    collection_name: str,
    clear_existing: bool = True,
    batch_size: int = 1000
):
    df = pd.read_csv(csv_path)

    if df.empty:
        return 0

    col = get_collection(collection_name)

    if clear_existing:
        col.delete_many({})

    records = df.to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        col.insert_many(records[i:i + batch_size])

    return len(records)
