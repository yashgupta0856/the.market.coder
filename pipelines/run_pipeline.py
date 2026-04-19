"""
Master pipeline runner.
Runs all phases in correct order with parallel execution where safe.

Dependency graph:
  Phase 1 → Phase 2 → [Phase 3 + Phase 4 + Phase 6]  → Phase 5 → Monte Carlo
                         (VCP)    (Sector)   (Sniper)
"""

from concurrent.futures import ThreadPoolExecutor


def main():
    # ── Phase 1 — Data Ingestion (sequential, everything depends on it) ──
    print("Running Phase 1 — Data Ingestion")
    from pipelines.build_universe import run_phase1
    run_phase1()

    # ── Phase 2 — Indicators (sequential, Phases 3+5 depend on it) ───────
    print("Running Phase 2 — Indicators")
    from pipelines.build_indicators import run_phase2
    run_phase2()

    # ── Phase 3 + 4 + 6 — Independent scanners (PARALLEL) ───────────────
    # Phase 3 (VCP):    reads equity_indicators     → writes vcp_candidates
    # Phase 4 (Sector): downloads from Yahoo        → writes sector_*
    # Phase 6 (Sniper): reads ohlcv_equities        → writes sniper_*
    # All three are independent of each other.
    print("Running Phase 3 + 4 + 6 in parallel (VCP · Sector · Sniper)")
    from pipelines.build_vcp_candidates import run_phase3
    from pipelines.build_sector import run_phase4
    from pipelines.build_sniper_candidates import run_sniper_pipeline

    with ThreadPoolExecutor(max_workers=3) as pool:
        f3 = pool.submit(run_phase3)
        f4 = pool.submit(run_phase4)
        f6 = pool.submit(run_sniper_pipeline)

        # Raise any exceptions from the workers
        f3.result()
        f4.result()
        f6.result()

    # ── Phase 5 — Stock Scoring & Ranking (depends on 3 + 4) ─────────────
    print("Running Phase 5 — Stock Scoring & Ranking")
    from pipelines.build_fusion import run_phase5_pipeline
    run_phase5_pipeline()

    # ── Monte Carlo (depends on 5) ───────────────────────────────────────
    print("Running Monte Carlo Risk Simulation")
    from pipelines.build_monte_carlo import build_monte_carlo
    build_monte_carlo()

    # ── Sync latest prices to secondary DB for fast dashboard reads ──────
    print("Syncing latest prices to secondary DB...")
    from utils.mongo import sync_latest_prices_to_secondary
    sync_latest_prices_to_secondary()

    print("All pipelines completed successfully")


if __name__ == "__main__":
    main()