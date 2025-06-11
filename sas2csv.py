import pyreadstat
import pandas as pd

# Efficiently read large SAS file
df, meta = pyreadstat.read_sas7bdat("client_t.sas7bdat")

# Optional: write in chunks if RAM is limited
chunk_size = 100000  # adjust based on your memory

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i+chunk_size]
    mode = 'w' if i == 0 else 'a'
    header = i == 0
    chunk.to_csv("output.csv", index=False, mode=mode, header=header)
