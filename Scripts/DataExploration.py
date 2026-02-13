from pathlib import Path
import re
import pandas as pd

# We choose which file we want to explore, and hide the rest
#df1 = Path("Bronze/light-duty-vehicles-2026-02-08.csv")
#df1 = Path("Bronze/Safercar_data.csv")
#df1 = Path("Bronze/vehicles.csv.zip")
#df2 = Path("Silver/light-duty-vehicles-2026-02-08.parquet")
#df2 = Path("Silver/Safercar_data.parquet")
#df2 = Path("Silver/vehicles.parquet")
#df2 = Path("Gold/safety_summary.parquet")
#df2 = Path("Gold/light_vehicle_detail.parquet")
df2 = Path("Gold/fuel_summary.parquet")

#df = pd.read_csv(df1, low_memory=False)
df = pd.read_parquet(df2)

print("\n==============================")
print("BASIC INFO")
print("==============================")
print("Shape:", df.shape)
print("Columns:", len(df.columns))
print("Column types:")
print(df.dtypes.value_counts())
print("\nHead (first 5 rows):")
print(df.head(5))
print("\nDataFrame info:")
df.info()

print("\n==============================")
print("MISSING VALUES (TOP 20)")
print("==============================")
print(df.isna().mean().sort_values(ascending=False).head(20))

print("\n==============================")
print("DUPLICATES")
print("==============================")
print("Full-row duplicates:", df.duplicated().sum())

print("\n==============================")
print("NUMERIC SUMMARY (EXTREMES)")
print("==============================")
num_cols = df.select_dtypes(include="number").columns
print(
    df[num_cols]
    .describe(percentiles=[0.01, 0.99])
    .T[["min", "1%", "99%", "max"]]
)

print("\n==============================")
print("DATE RANGES")
print("==============================")
date_cols = df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]).columns
for c in date_cols:
    print(c, "->", df[c].min(), "to", df[c].max())
