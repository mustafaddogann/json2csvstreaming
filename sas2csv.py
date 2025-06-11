import pyreadstat
import pandas as pd
import csv

# Efficiently read large SAS file
df, meta = pyreadstat.read_sas7bdat("client_t.sas7bdat")

# CRITICAL: Replace NaN values with empty strings for ADF compatibility
df = df.fillna('')

# Write with explicit parameters for ADF compatibility
df.to_csv(
    "client_t.csv",
    index=False,
    quoting=csv.QUOTE_MINIMAL,    # Consistent with your splitter
    doublequote=True,             # Use "" to escape quotes (ADF standard)
    escapechar=None,              # Don't use escape character
    encoding='utf-8',             # UTF-8 encoding
    lineterminator='\n',          # Unix-style line endings
    na_rep=''                     # Empty string for any remaining NaN
)

print(f"✅ Converted {len(df):,} rows to client_t.csv")
