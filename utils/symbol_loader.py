from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from utils.mongo import get_collection


def load_symbol_frames(
    collection_name: str,
    projection: dict,
    limit_per_symbol: int | None = None,
    max_workers: int = 12,
    date_field: str = "date",
):
    """
    Load per-symbol dataframes from MongoDB.

    When *limit_per_symbol* is set, issues one indexed query per symbol
    (capped to the recent window needed) using parallel threads.
    When no limit is set, performs a single bulk query and groups in Python.
    """
    col = get_collection(collection_name)
    symbols = col.distinct("symbol")

    if not symbols:
        return {}

    projection = {"_id": 0, **projection}

    # ── Fast path: no per-symbol limit → single query ────────────────
    if limit_per_symbol is None:
        all_docs = list(col.find({}, projection).sort(date_field, 1))
        if not all_docs:
            return {}

        df = pd.DataFrame(all_docs)
        if date_field in df.columns:
            df[date_field] = pd.to_datetime(df[date_field])

        return {
            symbol: group.reset_index(drop=True)
            for symbol, group in df.groupby("symbol")
        }

    # ── Capped path: parallel indexed queries per symbol ─────────────
    def _load_symbol(symbol: str):
        cursor = col.find({"symbol": symbol}, projection).sort(date_field, -1)
        cursor = cursor.limit(limit_per_symbol)

        docs = list(cursor)
        if not docs:
            return symbol, None

        docs.reverse()
        frame = pd.DataFrame(docs)

        if date_field in frame.columns:
            frame[date_field] = pd.to_datetime(frame[date_field])

        return symbol, frame

    frames = {}
    worker_count = min(max_workers, len(symbols))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for symbol, frame in executor.map(_load_symbol, symbols):
            if frame is not None and not frame.empty:
                frames[symbol] = frame

    return frames
