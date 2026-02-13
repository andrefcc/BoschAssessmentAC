from pathlib import Path
import re
import pandas as pd

BRONZE_PATH = Path("C:/Users/e709117/Downloads/Assessment/Bronze/light-duty-vehicles-2026-02-08.csv")
SILVER_FOLDER = Path("C:/Users/e709117/Downloads/Assessment/Silver")
SILVER_PARQUET = SILVER_FOLDER / "light-duty-vehicles-2026-02-08.parquet"

#if the folder doesn't already exist, it creates it
SILVER_FOLDER.mkdir(parents=True, exist_ok=True)

if not BRONZE_PATH.exists():
    raise FileNotFoundError(f"Bronze file not found at {BRONZE_PATH}.")

df = pd.read_csv(BRONZE_PATH, low_memory=False)

#DATA PROCESSING STEPS
# 1) Standardize column names to upper snake case
NEW_COLS = []
for i in df.columns:
    i = i.strip() #remove trailing spaces
    i = re.sub(r"[^\w]+", "_", i) #replace nonword characters with underscore
    i = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", i) #add underscore between camelCase
    i = re.sub(r"_+", "_", i) #replace multiple underscores with single
    i = i.strip("_") #remove trailing underscores
    i = i.upper() #convert to upper case
    NEW_COLS.append(i)
df.columns = NEW_COLS

# 2) Normalize missing values for string columns
null_values = {"", " ", "na", "n/a", "null", "none", "nan", "unknown", "n"}
obj_cols = df.select_dtypes(include=["object"]).columns
for i in obj_cols:
    j = df[i].astype("string").str.strip() #remove trailing spaces
    df[i] = j.where(~j.str.lower().isin(null_values), pd.NA) #normalize nulls

# 3) Remove full-row duplicates
df = df.drop_duplicates()

# 4) Cast numeric-like columns
numeric_like_values = re.compile(r"^-?\d+(\.\d+)?$") #matches integers and decimals, with optional minus sign
for i in df.columns:
    if df[i].dtype == "object" or str(df[i].dtype).startswith("string"):
        j = df[i].dropna().astype(str).str.strip() #drop missing values, convert to string, remove trailing spaces
        if len(j) == 0: #if column has only null values, skip it
            continue
        sample = j.sample(min(2000, len(j)), random_state=42)
        if sample.str.match(numeric_like_values).mean() >= 0.95: #if at least 95% of the 2000 sample values look numeric
            df[i] = pd.to_numeric(df[i], errors="coerce") #convert to numeric, coercing non-convertible values to NaN

# 5) Cast date-like columns
date_cols = []
for i in df.columns:
    # Only try on string columns
    if df[i].dtype not in ["object", "string"]:
        continue
    # Drop missing values and convert to strings, plus, remove trailing spaces
    j = df[i].dropna().astype(str).str.strip()
    if len(j) == 0:
        continue
    sample = j.sample(min(2000, len(j)), random_state=42)
    # Skip columns that are mostly 4-digit years, years should still be treated as numerics
    if sample.str.fullmatch(r"\d{4}").mean() >= 0.8:
        years = pd.to_numeric(sample, errors="coerce")
        if years.between(1900, 2100).mean() >= 0.8:
            continue  # not treated as date column
    # Try parsing as datetime
    parsed = pd.to_datetime(sample, errors="coerce", format="mixed", utc=True)
    # If most values parse successfully, treat as date column
    if parsed.notna().mean() >= 0.95:
        date_cols.append(i)
        
for k in date_cols:
    df[k] = pd.to_datetime(df[k], errors="coerce", format="mixed", utc=True)
    
# 6) Removing 95% missing values columns
missing_ratio = df.isna().mean()
sparse_cols = missing_ratio[missing_ratio >= 0.95].index.tolist()
df = df.drop(columns=sparse_cols)

# 7) Standardize bolean-like columns
boolean_map = {
    "sim": True, "y": True, "yes": True, "true": True, "1": True,
    "não": False, "n": False, "no": False, "false": False, "0": False
}
for i in df.columns:
    # Only try on string-like columns
    if df[i].dtype not in ["object", "string"]:
        continue
    # Drop missing values, convert to strings, remove trailing spaces, and lowercase
    j = df[i].dropna().astype(str).str.strip().str.lower()
    if len(j) == 0:
        continue
    sample = j.sample(min(2000, len(j)), random_state=42)
    if sample.isin(boolean_map.keys()).mean() >= 0.95:
        df[i] = (df[i].astype("string").str.strip().str.lower().map(boolean_map)) #map to boolean
            
# 8) Other Specific file changes:
# 8.1) Standardize and uppercase on main columns
UpperCaseColumns = ["MODEL", "MANUFACTURER", "DRIVETRAIN"]
for col in UpperCaseColumns:
    if col in df.columns:
        df[col] = df[col].astype("string").str.strip().str.upper()

# Table starting in the key columns
join_keys = ["MANUFACTURER", "MODEL", "MODEL_YEAR"]
df = df[join_keys + [i for i in df.columns if i not in join_keys]]

# Write Silver (Parquet)
df.to_parquet(SILVER_PARQUET, index=False)
