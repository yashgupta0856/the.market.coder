"""
Master pipeline runner.
Runs all phases in correct order.
"""

def main():
    print("Running Phase 1 — Data Ingestion")
    from pipelines.build_universe import run_phase1
    run_phase1()

    print("Running Phase 2 — Indicators")
    from pipelines.build_indicators import run_phase2
    from sectors.build_benchmarks_indicators import run_benchmark_pipeline
    run_benchmark_pipeline()
    run_phase2()

    print("Running Phase 3 — VCP Scanner")
    from pipelines.build_vcp_candidates import run_phase3
    run_phase3()

    print("Running Phase 4 — Sector Intelligence")
    from pipelines.build_sector import run_phase4
    run_phase4()

    print("Running Phase 5 — Stock Scoring & Ranking")
    from pipelines.build_fusion import run_phase5_pipeline
    run_phase5_pipeline()


    print("Running Phase 6 — Sniper Scanner")
    from pipelines.build_sniper_candidates import run_sniper_pipeline
    run_sniper_pipeline()

    

    print("All pipelines completed successfully")

    print("Running Monte Carlo Risk Simulation")
    from pipelines.build_monte_carlo import build_monte_carlo
    build_monte_carlo()


if __name__ == "__main__":
    main()