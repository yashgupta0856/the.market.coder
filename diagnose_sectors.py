"""Diagnose sector intelligence data issues."""
from utils.mongo import get_collection
import pandas as pd

# 1. Check sector_indicators latest date data quality
print("=" * 60)
print("1. SECTOR INDICATORS - Latest Date Data Quality")
print("=" * 60)
col = get_collection("sector_indicators")
df = pd.DataFrame(list(col.find({}, {"_id": 0, "sector_index": 1, "date": 1, "close": 1, "roc_63": 1, "reg_slope_100": 1, "sma_200": 1})))
df["date"] = pd.to_datetime(df["date"])
latest = df["date"].max()
print(f"Latest date: {latest}")
sector_latest = df[df["date"] == latest]
print(f"\nSectors on latest date ({len(sector_latest)} sectors):")
for _, row in sector_latest.iterrows():
    print(f"  {row['sector_index']:15s}: close={row['close']:<12} roc_63={row['roc_63']:<12} reg_slope_100={row['reg_slope_100']:<12} sma_200={row['sma_200']}")

# Check second-latest
second = df[df["date"] < latest]["date"].max()
print(f"\nSecond latest date: {second}")
sl2 = df[df["date"] == second]
print(f"Sectors on second latest ({len(sl2)} sectors):")
for _, row in sl2.iterrows():
    print(f"  {row['sector_index']:15s}: close={row['close']:<12} roc_63={row['roc_63']:<12} reg_slope_100={row['reg_slope_100']}")

# 2. Check benchmark latest dates
print("\n" + "=" * 60)
print("2. BENCHMARK INDICATORS - Last 3 rows")
print("=" * 60)
bcol = get_collection("benchmark_indicators")
bdf = pd.DataFrame(list(bcol.find({}, {"_id": 0})))
bdf["date"] = pd.to_datetime(bdf["date"])
bdf = bdf.sort_values("date")
for _, row in bdf.tail(3).iterrows():
    print(f"  date={row['date']}, close={row['close']}, roc_63={row.get('roc_63')}, reg_slope_100={row.get('reg_slope_100')}")

# 3. Check the date alignment issue
print("\n" + "=" * 60)
print("3. DATE ALIGNMENT - sector vs benchmark latest dates")
print("=" * 60)
sector_latest_date = df["date"].max()
bench_latest_date = bdf["date"].max()
print(f"Sector latest date:    {sector_latest_date}")
print(f"Benchmark latest date: {bench_latest_date}")
print(f"Match: {sector_latest_date == bench_latest_date}")

# 4. Check how many non-NaN for key indicators on latest date
print("\n" + "=" * 60)
print("4. NaN COUNT on latest sector_indicators date")
print("=" * 60)
for col_name in ["close", "roc_63", "reg_slope_100", "sma_200"]:
    non_null = sector_latest[col_name].notna().sum()
    total = len(sector_latest)
    print(f"  {col_name}: {non_null}/{total} non-NaN")

# 5. Check sector_regime data
print("\n" + "=" * 60)
print("5. SECTOR REGIME - Distribution")
print("=" * 60)
rcol = get_collection("sector_regime")
rdocs = list(rcol.find({}, {"_id": 0, "sector_index": 1, "sector_regime": 1, "rs_rank": 1}))
from collections import Counter
regimes = Counter(d["sector_regime"] for d in rdocs)
print(f"Total sectors: {len(rdocs)}")
print(f"Regime distribution: {dict(regimes)}")

# 6. Missing sectors (defined in pipeline but not in data)
print("\n" + "=" * 60)
print("6. MISSING SECTORS")
print("=" * 60)
expected = ["CNXAUTO", "CNXIT", "CNXFMCG", "CNXFIN", "CNXPHARMA", "CNXMETAL",
            "CNXENERGY", "CNXINFRA", "CNXREALTY", "CNXMEDIA", "CNXPSUBANK", "CNXSERVICE"]
actual = df["sector_index"].unique().tolist()
missing = set(expected) - set(actual)
print(f"Expected: {len(expected)} sectors")
print(f"Actual:   {len(actual)} sectors")
print(f"Missing:  {missing}")
