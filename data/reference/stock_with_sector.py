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

    
    # STEP 2: CNX SECTOR → INDUSTRY MAPPING
    
    SECTOR_INDEX_MAP = {
        "CNXAUTO": [
            "Auto Manufacturers", "Auto Parts", "Auto & Truck Dealerships",
            "Recreational Vehicles", "Specialty Automotive Retail",
            "Farm & Heavy Construction Machinery", "Railroads",
            "Trucking", "Marine Shipping", "Airports & Air Services"
        ],
        "CNXENERGY": [
            "Oil & Gas Integrated", "Oil & Gas Refining & Marketing",
            "Oil & Gas E&P", "Oil & Gas Equipment & Services",
            "Thermal Coal", "Utilities—Regulated Electric",
            "Utilities—Renewable", "Utilities—Independent Power Producers",
            "Utilities—Regulated Gas", "Utilities—Regulated Water",
            "Solar", "Wind", "Uranium", "Electrical Equipment & Parts"
        ],
        "CNXREALTY": [
            "Real Estate—Development", "Real Estate Services",
            "Real Estate—Diversified", "REIT—Residential",
            "REIT—Office", "REIT—Industrial", "REIT—Retail",
            "Property Management", "Infrastructure Operations"
        ],
        "CNXINFRA": [
            "Engineering & Construction", "Infrastructure Operations",
            "Building Products & Equipment", "Construction Materials",
            "Heavy Electrical Equipment", "Conglomerates",
            "Industrial Distribution", "Specialty Industrial Machinery",
            "Tools & Accessories", "Waste Management"
        ],
        "CNXPHARMA": [
            "Drug Manufacturers—General",
            "Drug Manufacturers—Specialty & Generic",
            "Biotechnology", "Medical Instruments & Supplies",
            "Medical Devices", "Medical Care Facilities",
            "Healthcare Plans", "Health Information Services",
            "Diagnostics & Research", "Life Sciences Tools & Services"
        ],
        "CNXMETAL": [
            "Steel", "Aluminum", "Copper", "Gold", "Silver",
            "Other Industrial Metals & Mining",
            "Diversified Metals & Mining",
            "Iron & Steel", "Coking Coal",
            "Building Materials", "Construction Materials",
            "Paper & Paper Products", "Forest Products"
        ],
        "CNXMEDIA": [
            "Entertainment", "Broadcasting", "Publishing",
            "Advertising Agencies", "Telecom Services",
            "Electronic Gaming & Multimedia",
            "Internet Content & Information",
            "Digital Media", "Communication Equipment"
        ],
        "CNXFMCG": [
            "Packaged Foods", "Farm Products",
            "Beverages—Non-Alcoholic",
            "Beverages—Wineries & Distilleries",
            "Household & Personal Products",
            "Confectioners", "Tobacco",
            "Specialty Retail", "Grocery Stores",
            "Department Stores", "Luxury Goods",
            "Footwear & Accessories",
            "Textile Manufacturing",
            "Apparel Manufacturing", "Apparel Retail"
        ],
        "CNXFIN": [
            "Banks—Regional", "Banks—Diversified",
            "Credit Services", "Asset Management",
            "Capital Markets", "Insurance—Life",
            "Insurance—Property & Casualty",
            "Insurance Brokers",
            "Financial Data & Stock Exchanges",
            "Mortgage Finance",
            "Financial Conglomerates",
            "Shell Companies",
            "Finacial Services"
        ],
        "CNXIT": [
            "Information Technology Services",
            "Software—Application",
            "Software—Infrastructure",
            "Communication Equipment",
            "Consumer Electronics",
            "Computer Hardware",
            "Semiconductors",
            "IT Consulting & Outsourcing",
            "Cloud Computing",
            "Cybersecurity",
            "Electronic Components",
            "Scientific & Technical Instruments"
        ],
        "NSEBANK":[
            "Regional"
        ]
    }

    
    # NORMALIZATION
    
    def normalize(text):
        if not text:
            return None
        text = text.lower()
        text = re.sub(r"[&/—\-]", " ", text)
        text = re.sub(r"[^a-z0-9 ]", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    normalized_map = {
        idx: [normalize(i) for i in inds]
        for idx, inds in SECTOR_INDEX_MAP.items()
    }

    
    # STEP 3: ENRICH USING YFINANCE (PARALLEL + RATE-LIMITED)
    

    def _enrich_symbol(symbol):
        """Fetch sector, industry, and fundamentals for a single symbol."""
        import time
        import logging

        # Suppress noisy yfinance HTTP error output
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)

        sector = "Others"
        sector_index = "OTHERS"
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
                    norm_industry = normalize(industry_val)

                    for idx, inds in normalized_map.items():
                        for ind in inds:
                            if ind and norm_industry and ind in norm_industry:
                                sector_index = idx
                                break
                        if sector_index != "OTHERS":
                            break
                    break  # success — exit retry loop

            except Exception:
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))  # backoff: 1s, 2s, 3s
                    continue

        return {
            "symbol": symbol,
            "sector": sector,
            "sector_index": sector_index,
            **fundamentals
        }


    results = []
    completed = 0
    effective_workers = min(max_workers, 5)  # cap at 5 to avoid rate limiting

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
