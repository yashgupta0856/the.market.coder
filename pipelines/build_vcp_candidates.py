"""
Phase 3 — VCP Candidate Scan + Pivot Anchoring

Each time this phase runs it:

1. Loads existing pivot anchors from ``vcp_pivot_anchors``.
2. Scans the universe — existing candidates use their stored (fixed) pivot
   when evaluating ``is_breakout``; new candidates compute a fresh pivot.
3. Writes brand-new pivots to ``vcp_pivot_anchors`` using $setOnInsert
   semantics (existing anchors are NEVER overwritten).
4. Deletes anchors for stocks that have exited the VCP pattern so they
   can receive a fresh pivot if they re-qualify later.
5. Stores the final scan results to ``vcp_candidates``.
"""

import pandas as pd
from datetime import datetime, timezone

from scanners.vcp_scanner import scan_universe
from utils.mongo import get_collection
from utils.mongo_writer import df_to_mongo, set_on_insert_df_to_mongo
from utils.symbol_loader import load_symbol_frames


VCP_HISTORY_BARS = 252
VCP_PROJECTION = {
    "symbol": 1,
    "date": 1,
    "close": 1,
    "high": 1,
    "low": 1,
    "volume": 1,
    "sma_50": 1,
    "sma_200": 1,
    "reg_slope_100": 1,
    "atr_14": 1,
}


# ---------------------------------------------------------------------------
# Pivot-anchor helpers
# ---------------------------------------------------------------------------


def _load_existing_anchors() -> dict:
    """
    Return a mapping of {symbol: pivot_price} for all previously anchored
    VCP candidates.  This is used to pass fixed pivots into the scanner.
    """
    col = get_collection("vcp_pivot_anchors")
    return {
        doc["symbol"]: doc["pivot_price"]
        for doc in col.find({}, {"_id": 0, "symbol": 1, "pivot_price": 1})
    }


def _write_new_anchors(results: pd.DataFrame, existing_anchors: dict) -> int:
    """
    Persist pivots for brand-new VCP candidates (symbols not yet in the
    anchors collection).  Uses $setOnInsert so re-running the pipeline
    on the same day never overwrites a pivot that was set earlier.

    Returns the number of new anchors written.
    """
    today_str = datetime.now(timezone.utc).date().isoformat()

    new_candidates = results[
        (results["vcp_candidate"] == True) &
        (~results["symbol"].isin(existing_anchors)) &
        (results["pivot_price"].notna())
    ]

    if new_candidates.empty:
        return 0

    anchor_df = pd.DataFrame({
        "symbol":      new_candidates["symbol"].values,
        "pivot_price": new_candidates["pivot_price"].values,
        "anchored_on": today_str,
    })

    written = set_on_insert_df_to_mongo(
        anchor_df,
        "vcp_pivot_anchors",
        key_fields=["symbol"],
    )

    # Ensure fast lookups
    get_collection("vcp_pivot_anchors").create_index(
        [("symbol", 1)], unique=True, background=True
    )

    return written


def _delete_exited_anchors(results: pd.DataFrame, existing_anchors: dict) -> int:
    """
    Remove anchors for stocks that are no longer VCP candidates.

    When a stock exits the pattern its stored pivot is deleted so that,
    if it re-qualifies in the future, a fresh pivot is established from
    the new consolidation base (not the old one).

    Returns the number of anchors deleted.
    """
    exited = results[results["vcp_candidate"] == False]["symbol"].tolist()
    exited_anchored = [s for s in exited if s in existing_anchors]

    if not exited_anchored:
        return 0

    col = get_collection("vcp_pivot_anchors")
    result = col.delete_many({"symbol": {"$in": exited_anchored}})
    return result.deleted_count


# ---------------------------------------------------------------------------
# Phase runner
# ---------------------------------------------------------------------------


def run_phase3():
    # ── Load indicator data ──────────────────────────────────────────────
    symbol_frames = load_symbol_frames(
        "equity_indicators",
        projection=VCP_PROJECTION,
        limit_per_symbol=VCP_HISTORY_BARS,
        max_workers=8,
    )

    if not symbol_frames:
        raise RuntimeError("equity_indicators collection is empty")

    # ── Load existing pivot anchors (fixed entry prices) ─────────────────
    existing_anchors = _load_existing_anchors()
    print(f"  Loaded {len(existing_anchors)} existing pivot anchors")

    # ── Scan universe (anchored pivots flow into the scanner) ────────────
    results = scan_universe(symbol_frames, existing_anchors=existing_anchors)

    # ── Manage anchor collection ─────────────────────────────────────────
    new_written = _write_new_anchors(results, existing_anchors)
    deleted     = _delete_exited_anchors(results, existing_anchors)

    # ── Summary stats ────────────────────────────────────────────────────
    total      = len(results)
    candidates = results["vcp_candidate"].sum()
    triggers   = results["is_breakout"].sum() if "is_breakout" in results.columns else 0

    quality_counts = (
        results["vcp_quality"].value_counts().to_dict()
        if "vcp_quality" in results.columns else {}
    )

    print(f"VCP Scan Results:")
    print(f"  Total symbols scanned:  {total}")
    print(f"  VCP candidates:         {candidates}")
    print(f"  Active breakouts:       {triggers}")
    print(f"  New pivots anchored:    {new_written}")
    print(f"  Stale anchors deleted:  {deleted}")

    if quality_counts:
        print("  Quality breakdown:")
        for level in ["trigger", "textbook", "strong", "emerging"]:
            if level in quality_counts:
                print(f"    {level:>10}: {quality_counts[level]}")

    print("Storing vcp_candidates in MongoDB...")
    df_to_mongo(results, "vcp_candidates")


if __name__ == "__main__":
    run_phase3()
