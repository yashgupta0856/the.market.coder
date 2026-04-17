# Sector Taxonomy & Mapping Rules

# Full Sector Hierarchy (Display purposes)
SECTOR_TAXONOMY = {
    "Agricultural": {
        "parent_cnx": "CNXFMCG",
        "subsectors": ["Pesticides & Agrochemicals", "Aquaculture", "Fertilizers", "Floriculture", "Agriculture", "Tea & Coffee"]
    },
    "Apparel & Accessories": {
        "parent_cnx": "CNXFMCG",
        "subsectors": ["Diamond & Jewellery", "Watches & Accessories", "Footwear", "Textiles", "Leather"]
    },
    "Automobile & Ancillaries": {
        "parent_cnx": "CNXAUTO",
        "subsectors": ["Auto Ancillary", "Two & Three Wheelers", "Passenger Cars", "Tractors", "Trucks/LCV", "Bearings", "Tyres", "Dealers", "Cycles", "Electric Vehicles"]
    },
    "Banking": {
        "parent_cnx": "CNXFIN",
        "subsectors": ["Bank - Private", "Bank - Public"]
    },
    "Chemicals": {
        "parent_cnx": "CNXPHARMA",
        "subsectors": ["Specialty Chemicals", "Dyes & Pigments", "Petrochemicals", "Chlor-Alkali", "Carbon Black"]
    },
    "Cement & Construction": {
        "parent_cnx": "CNXINFRA",
        "subsectors": ["Cement", "Construction", "Infrastructure", "Roads & Highways"]
    },
    "Consumer Durables": {
        "parent_cnx": "CNXFMCG",
        "subsectors": ["Air Conditioners", "Domestic Appliances", "Electronics", "Consumer Electronics"]
    },
    "Consumer & Retail": {
        "parent_cnx": "CNXFMCG",
        "subsectors": ["Retailing", "E-Commerce", "Department Stores", "Grocery", "FMCG", "Tobacco"]
    },
    "Derived Materials": {
        "parent_cnx": "CNXINFRA",
        "subsectors": ["Abrasives", "Castings/Forgings", "Ceramics", "Fasteners", "Glass", "Laminates", "Packaging", "Paints", "Plastic Products"]
    },
    "Education": {
        "parent_cnx": "CNXSERVICE",
        "subsectors": ["Educational Institutions", "Edtech", "Training"]
    },
    "Energy & Power": {
        "parent_cnx": "CNXENERGY",
        "subsectors": ["Oil & Gas", "Power Generation", "Renewable Energy", "Solar", "Wind", "Coal"]
    },
    "Engineering": {
        "parent_cnx": "CNXINFRA",
        "subsectors": ["Heavy Electrical", "Industrial Machinery", "Compressors", "Pumps", "Equipment"]
    },
    "Finance - NBFC": {
        "parent_cnx": "CNXFIN",
        "subsectors": ["NBFC", "Housing Finance", "Microfinance", "Gold Loans"]
    },
    "Finance - Capital Markets": {
        "parent_cnx": "CNXFIN",
        "subsectors": ["Stock Exchanges", "Brokers", "Asset Management", "Wealth Management"]
    },
    "Insurance": {
        "parent_cnx": "CNXFIN",
        "subsectors": ["Insurance - Life", "Insurance - General", "Insurance Brokers", "Reinsurance"]
    },
    "IT - Software & Services": {
        "parent_cnx": "CNXIT",
        "subsectors": ["IT Services", "Software Products", "Cloud", "SaaS", "Digital"]
    },
    "IT - Hardware & Electronics": {
        "parent_cnx": "CNXIT",
        "subsectors": ["Computer Hardware", "Semiconductors", "Electronic Components", "Cables"]
    },
    "Metals & Mining": {
        "parent_cnx": "CNXMETAL",
        "subsectors": ["Steel", "Aluminium", "Copper", "Zinc", "Iron Ore", "Mining", "Sponge Iron"]
    },
    "Media & Entertainment": {
        "parent_cnx": "CNXMEDIA",
        "subsectors": ["Broadcasting", "Entertainment", "Publishing", "Advertising", "Digital Media"]
    },
    "Pharmaceuticals": {
        "parent_cnx": "CNXPHARMA",
        "subsectors": ["Pharma - Indian", "Pharma - MNC", "API/Bulk Drugs", "CRAMS/CDMO"]
    },
    "Healthcare": {
        "parent_cnx": "CNXPHARMA",
        "subsectors": ["Hospitals", "Diagnostics", "Medical Devices", "Health Information"]
    },
    "Telecom": {
        "parent_cnx": "CNXMEDIA",
        "subsectors": ["Telecom Services", "Telecom Equipment", "ISP", "Tower Infrastructure"]
    },
    "Real Estate": {
        "parent_cnx": "CNXREALTY",
        "subsectors": ["Residential", "Commercial", "REITs", "Property Management"]
    },
    "Textiles": {
        "parent_cnx": "CNXFMCG",
        "subsectors": ["Cotton", "Synthetic", "Yarn", "Fabric", "Garments", "Home Textiles"]
    },
    "Logistics & Transport": {
        "parent_cnx": "CNXAUTO",
        "subsectors": ["Shipping", "Logistics", "Ports", "Airlines", "Railways", "Warehousing"]
    },
    "Paper & Printing": {
        "parent_cnx": "CNXMETAL",
        "subsectors": ["Paper", "Printing", "Stationery"]
    },
    "Electrical & Electronics": {
        "parent_cnx": "CNXENERGY",
        "subsectors": ["Electrical Equipment", "Cables", "Wires", "Switchgears", "Transformers", "Batteries"]
    },
    "Defence": {
        "parent_cnx": "CNXINFRA",
        "subsectors": ["Defence Equipment", "Aerospace", "Ordnance", "Shipbuilding"]
    },
    "Personal Care & FMCG": {
        "parent_cnx": "CNXFMCG",
        "subsectors": ["Household Products", "Personal Care", "Home Care", "Oral Care"]
    },
    "Hotels & Tourism": {
        "parent_cnx": "CNXSERVICE",
        "subsectors": ["Hotels", "Resorts", "Travel", "Tourism", "Restaurants"]
    },
    "Specialty & Diversified": {
        "parent_cnx": "CNXSERVICE",
        "subsectors": ["Conglomerates", "Diversified", "Trading", "Miscellaneous"]
    },
    "Utilities": {
        "parent_cnx": "CNXENERGY",
        "subsectors": ["Water", "Gas Distribution", "Regulated Utilities"]
    },
    "PSU & Government": {
        "parent_cnx": "CNXPSUBANK",
        "subsectors": ["Central PSU", "State PSU", "PSU Banks"]
    },
    "Building Materials": {
        "parent_cnx": "CNXINFRA",
        "subsectors": ["Ceramics", "Tiles", "Plywood", "Laminates", "Sanitaryware", "Pipes"]
    },
    "Biotech & Life Sciences": {
        "parent_cnx": "CNXPHARMA",
        "subsectors": ["Biotechnology", "Life Sciences", "CRO", "Genomics"]
    }
}

# yfinance Industry -> (Granular Sector, Granular Sub-Sector, Parent CNX Index)
GRANULAR_MAP = {
    # Agriculture
    "Agricultural Inputs": ("Agricultural", "Pesticides & Agrochemicals", "CNXFMCG"),
    "Farm Products": ("Agricultural", "Agriculture", "CNXFMCG"),
    
    # Auto
    "Auto Parts": ("Automobile & Ancillaries", "Auto Ancillary", "CNXAUTO"),
    "Auto Manufacturers": ("Automobile & Ancillaries", "Passenger Cars", "CNXAUTO"),
    "Auto & Truck Dealerships": ("Automobile & Ancillaries", "Dealers", "CNXAUTO"),
    "Recreational Vehicles": ("Automobile & Ancillaries", "Two & Three Wheelers", "CNXAUTO"),
    "Electric Vehicle Manufacturers": ("Automobile & Ancillaries", "Electric Vehicles", "CNXAUTO"),
    
    # Banking & Finance
    "Banks—Regional": ("Banking", "Bank - Private", "CNXFIN"),
    "Banks—Diversified": ("Banking", "Bank - Public", "CNXFIN"),
    "Credit Services": ("Finance - NBFC", "NBFC", "CNXFIN"),
    "Mortgage Finance": ("Finance - NBFC", "Housing Finance", "CNXFIN"),
    "Asset Management": ("Finance - Capital Markets", "Asset Management", "CNXFIN"),
    "Capital Markets": ("Finance - Capital Markets", "Brokers", "CNXFIN"),
    "Financial Data & Stock Exchanges": ("Finance - Capital Markets", "Stock Exchanges", "CNXFIN"),
    "Insurance—Life": ("Insurance", "Insurance - Life", "CNXFIN"),
    "Insurance—Property & Casualty": ("Insurance", "Insurance - General", "CNXFIN"),
    
    # Pharma & Healthcare
    "Drug Manufacturers—General": ("Pharmaceuticals", "Pharma - Indian", "CNXPHARMA"),
    "Drug Manufacturers—Specialty & Generic": ("Pharmaceuticals", "Pharma - Indian", "CNXPHARMA"),
    "Biotechnology": ("Biotech & Life Sciences", "Biotechnology", "CNXPHARMA"),
    "Life Sciences Tools & Services": ("Biotech & Life Sciences", "Life Sciences", "CNXPHARMA"),
    "Medical Instruments & Supplies": ("Healthcare", "Medical Devices", "CNXPHARMA"),
    "Medical Devices": ("Healthcare", "Medical Devices", "CNXPHARMA"),
    "Medical Care Facilities": ("Healthcare", "Hospitals", "CNXPHARMA"),
    "Diagnostics & Research": ("Healthcare", "Diagnostics", "CNXPHARMA"),
    
    # IT
    "Information Technology Services": ("IT - Software & Services", "IT Services", "CNXIT"),
    "Software—Application": ("IT - Software & Services", "Software Products", "CNXIT"),
    "Software—Infrastructure": ("IT - Software & Services", "Software Products", "CNXIT"),
    "Computer Hardware": ("IT - Hardware & Electronics", "Computer Hardware", "CNXIT"),
    "Semiconductors": ("IT - Hardware & Electronics", "Semiconductors", "CNXIT"),
    "Electronic Components": ("IT - Hardware & Electronics", "Electronic Components", "CNXIT"),
    
    # Consumer & FMCG
    "Packaged Foods": ("Consumer & Retail", "FMCG", "CNXFMCG"),
    "Beverages—Non-Alcoholic": ("Consumer & Retail", "FMCG", "CNXFMCG"),
    "Beverages—Wineries & Distilleries": ("Consumer & Retail", "FMCG", "CNXFMCG"),
    "Tobacco": ("Consumer & Retail", "Tobacco", "CNXFMCG"),
    "Household & Personal Products": ("Personal Care & FMCG", "Personal Care", "CNXFMCG"),
    "Grocery Stores": ("Consumer & Retail", "Grocery", "CNXFMCG"),
    "Specialty Retail": ("Consumer & Retail", "Retailing", "CNXFMCG"),
    
    # Construction & Derived
    "Engineering & Construction": ("Cement & Construction", "Construction", "CNXINFRA"),
    "Infrastructure Operations": ("Cement & Construction", "Infrastructure", "CNXINFRA"),
    "Building Products & Equipment": ("Derived Materials", "Building Materials", "CNXINFRA"),
    "Construction Materials": ("Cement & Construction", "Cement", "CNXINFRA"),
    "Packaging & Containers": ("Derived Materials", "Packaging", "CNXINFRA"),
    "Specialty Chemicals": ("Chemicals", "Specialty Chemicals", "CNXPHARMA"),
    "Chemicals": ("Chemicals", "Petrochemicals", "CNXPHARMA"),
    
    # Energy
    "Oil & Gas Integrated": ("Energy & Power", "Oil & Gas", "CNXENERGY"),
    "Oil & Gas Refining & Marketing": ("Energy & Power", "Oil & Gas", "CNXENERGY"),
    "Oil & Gas E&P": ("Energy & Power", "Oil & Gas", "CNXENERGY"),
    "Utilities—Regulated Electric": ("Energy & Power", "Power Generation", "CNXENERGY"),
    "Utilities—Renewable": ("Energy & Power", "Renewable Energy", "CNXENERGY"),
    "Solar": ("Energy & Power", "Solar", "CNXENERGY"),
    "Utilities—Regulated Water": ("Utilities", "Water", "CNXENERGY"),
    
    # Metals, Mining & Paper
    "Steel": ("Metals & Mining", "Steel", "CNXMETAL"),
    "Aluminum": ("Metals & Mining", "Aluminium", "CNXMETAL"),
    "Other Industrial Metals & Mining": ("Metals & Mining", "Mining", "CNXMETAL"),
    "Paper & Paper Products": ("Paper & Printing", "Paper", "CNXMETAL"),
    
    # Media
    "Entertainment": ("Media & Entertainment", "Entertainment", "CNXMEDIA"),
    "Broadcasting": ("Media & Entertainment", "Broadcasting", "CNXMEDIA"),
    "Telecom Services": ("Telecom", "Telecom Services", "CNXMEDIA"),
    
    # Misc
    "Real Estate—Development": ("Real Estate", "Residential", "CNXREALTY"),
    "Real Estate Services": ("Real Estate", "Commercial", "CNXREALTY"),
    "Conglomerates": ("Specialty & Diversified", "Conglomerates", "CNXSERVICE"),
    "Apparel Manufacturing": ("Textiles", "Garments", "CNXFMCG"),
    "Textile Manufacturing": ("Textiles", "Textiles", "CNXFMCG"),
    "Footwear & Accessories": ("Apparel & Accessories", "Footwear", "CNXFMCG"),
    "Luxury Goods": ("Apparel & Accessories", "Watches & Accessories", "CNXFMCG"),
    "Travel Services": ("Hotels & Tourism", "Travel", "CNXSERVICE"),
    "Lodging": ("Hotels & Tourism", "Hotels", "CNXSERVICE"),
    "Restaurants": ("Hotels & Tourism", "Restaurants", "CNXSERVICE"),
    "Railroads": ("Logistics & Transport", "Railways", "CNXAUTO"),
    "Marine Shipping": ("Logistics & Transport", "Shipping", "CNXAUTO"),
    "Airports & Air Services": ("Logistics & Transport", "Airlines", "CNXAUTO")
}
