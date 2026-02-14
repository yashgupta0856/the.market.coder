def nse_sector_enrichment_to_csv(output_csv: str):
    import pandas as pd
    import yfinance as yf
    import re
    import requests
    from io import StringIO

    
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

    
    # STEP 3: ENRICH USING YFINANCE
    
    results = []

    for symbol in symbols:
        sector = None
        sector_index = None

        try:
            ticker = yf.Ticker(f"{symbol}.NS")
            info = ticker.info

            sector = info.get("sector")
            industry = info.get("industry")
            norm_industry = normalize(industry)

            for idx, inds in normalized_map.items():
                for ind in inds:
                    if ind and norm_industry and ind in norm_industry:
                        sector_index = idx
                        break
                if sector_index:
                    break

        except Exception:
            pass

        
        # FINAL FALLBACK NORMALIZATION
        
        if not sector:
            sector = "Others"

        if not sector_index:
            sector_index = "OTHERS"

        results.append({
            "symbol": symbol,
            "sector": sector,
            "sector_index": sector_index
        })

    
    # STEP 4: WRITE CSV
    
    output_df = pd.DataFrame(results)
    if output_csv:
        output_df.to_csv(output_csv, index=False)

    return output_df
