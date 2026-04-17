def nse_sector_enrichment_to_csv(output_csv: str, max_workers=10):
    import pandas as pd
    import yfinance as yf
    import re
    import requests
    from io import StringIO
    from concurrent.futures import ThreadPoolExecutor, as_completed

    
    # STEP 1: DOWNLOAD NSE EQUITY UNIVERSE
    
    NSE_EQUITY_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv",
        "Referer": "https://www.nseindia.com"
    }

    response = requests.get(NSE_EQUITY_URL, headers=headers, timeout=10)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text))
    symbols = df["SYMBOL"].tolist()

    
    # STEP 2: GRANULAR SECTOR TAXONOMY
    
    from configs.sector_taxonomy import GRANULAR_MAP

    # NORMALIZATION
    
    def normalize(text):
        if not text:
            return None
        text = text.lower()
        text = re.sub(r"[&/—\-]", " ", text)
        text = re.sub(r"[^a-z0-9 ]", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    # Pre-compute normalized sub-sectors for fuzzy fallback
    fuzzy_map = {}
    for ind, (g_sec, g_subsec, cnx) in GRANULAR_MAP.items():
        norm_sub = normalize(g_subsec)
        if norm_sub and norm_sub not in fuzzy_map:
            fuzzy_map[norm_sub] = (g_sec, g_subsec, cnx)

    
    # STEP 3: ENRICH USING YFINANCE (PARALLEL + RATE-LIMITED)
    

    def _enrich_symbol(symbol):
        """Fetch sector, industry, and fundamentals for a single symbol."""
        import time
        import logging

        # Suppress noisy yfinance HTTP error output
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)

        sector = "Others"
        sector_index = "OTHERS"
        granular_sec = "Others"
        granular_subsec = "Unknown"
        fundamentals = {
            "market_cap": None,
            "revenue_growth": None,
            "trailing_eps": None,
            "forward_eps": None
        }

        max_retries = 3

        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(f"{symbol}.NS")
                info = ticker.info

                sector_val = info.get("sector")
                industry_val = info.get("industry")

                # Extract fundamentals
                fundamentals["market_cap"] = info.get("marketCap")
                fundamentals["revenue_growth"] = info.get("revenueGrowth")
                fundamentals["trailing_eps"] = info.get("trailingEps")
                fundamentals["forward_eps"] = info.get("forwardEps")

                if sector_val and industry_val:
                    sector = sector_val
                    granular_subsec = industry_val

                    if industry_val in GRANULAR_MAP:
                        granular_sec, granular_subsec, sector_index = GRANULAR_MAP[industry_val]
                    else:
                        norm_industry = normalize(industry_val)
                        found = False
                        if norm_industry:
                            for norm_sub, tup in fuzzy_map.items():
                                if norm_sub in norm_industry or norm_industry in norm_sub:
                                    granular_sec, granular_subsec, sector_index = tup
                                    found = True
                                    break
                        if not found:
                            try:
                                from utils.mongo import get_collection
                                unmapped = get_collection("unmapped_industries")
                                unmapped.update_one(
                                    {"industry": industry_val},
                                    {"$inc": {"count": 1}},
                                    upsert=True
                                )
                            except Exception:
                                pass # ignore DB errors in thread
                    break  # success — exit retry loop

            except Exception:
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))  # backoff: 1s, 2s, 3s
                    continue

        return {
            "symbol": symbol,
            "sector": sector,
            "sector_index": sector_index,
            "granular_sector": granular_sec,
            "granular_subsector": granular_subsec,
            **fundamentals
        }


    results = []
    completed = 0
    effective_workers = min(max_workers, 8)  # raised from 5 to 8

    print(f"Enriching {len(symbols)} symbols with sector data "
          f"({effective_workers} parallel threads, with retry)...")

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {
            executor.submit(_enrich_symbol, sym): sym
            for sym in symbols
        }

        for future in as_completed(futures):
            completed += 1
            if completed % 200 == 0:
                print(f"  Sector enrichment progress: {completed}/{len(symbols)}")
            results.append(future.result())

    # Count how many got real sectors vs fallback
    real_sectors = sum(1 for r in results if r["sector"] != "Others")
    print(f"Sector enrichment complete: {len(results)} symbols processed "
          f"({real_sectors} matched, {len(results) - real_sectors} fallback)")

    
    # STEP 4: WRITE CSV
    
    output_df = pd.DataFrame(results)
    if output_csv:
        output_df.to_csv(output_csv, index=False)

    return output_df
