"""Convert the two-sheet Online Retail II .xlsx into one clean CSV.

Usage (from repo root):
    pip install pandas openpyxl
    python scripts/xlsx_to_csv.py data/online_retail_II.xlsx data/raw/online_retail_ii.csv
"""
import sys
import pandas as pd

in_path = sys.argv[1] if len(sys.argv) > 1 else "online_retail_II.xlsx"
out_path = sys.argv[2] if len(sys.argv) > 2 else "online_retail_ii.csv"

# 1. Read every sheet. Force the code columns to text so values like
#    "85123A" and cancellation invoices like "C536379" are not mangled.
print(f"Reading {in_path} (this can take a few minutes)...")
sheets = pd.read_excel(
    in_path,
    sheet_name=None,  # None = all sheets -> dict {sheet_name: DataFrame}
    dtype={"Invoice": str, "StockCode": str},
)

for name, sheet in sheets.items():
    print(f"  sheet '{name}': {len(sheet):,} rows")

# 2. Combine the sheets into one table.
df = pd.concat(sheets.values(), ignore_index=True)
print(f"Combined: {len(df):,} rows")

# 3. Tidy column names.
df = df.rename(columns={"Customer ID": "CustomerID", "Price": "UnitPrice"})

# 4. Fix types.
if not pd.api.types.is_datetime64_any_dtype(df["InvoiceDate"]):
    # Only needed if dates came through as text, e.g. "01-12-2010 08:26" (day first)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], dayfirst=True)

df["CustomerID"] = df["CustomerID"].astype("Int64")  # whole numbers, blanks allowed
df["Invoice"] = df["Invoice"].str.strip()
df["StockCode"] = df["StockCode"].str.strip()
df["Description"] = df["Description"].str.strip()

# 5. Remove exact duplicate rows (the two sheets can overlap around Dec 2010).
n_dupes = int(df.duplicated().sum())
df = df.drop_duplicates()
print(f"Dropped {n_dupes:,} exact duplicate rows -> {len(df):,} rows remain")

# 6. Sort by event time (useful for replaying the data into Kafka later).
df = df.sort_values(["InvoiceDate", "Invoice"], kind="stable").reset_index(drop=True)

# 7. Save. Dates are written as unambiguous ISO text (YYYY-MM-DD HH:MM:SS).
df.to_csv(out_path, index=False, date_format="%Y-%m-%d %H:%M:%S")

print(f"Saved {out_path}")
print(f"Date range: {df['InvoiceDate'].min()}  ->  {df['InvoiceDate'].max()}")
print(f"Unique SKUs: {df['StockCode'].nunique():,} | Countries: {df['Country'].nunique()}")
print(f"Missing values per column:\n{df.isna().sum()}")