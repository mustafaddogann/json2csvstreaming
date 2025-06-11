# SAS to ADF Workflow - Two Step Process

## Step 1: Convert SAS to CSV
Use `sas2csv.py` to convert your SAS file to a properly formatted CSV:

```bash
python sas2csv.py
```

This will:
- Read `client_t.sas7bdat`
- Replace NaN values with empty strings (critical for ADF!)
- Create `client_t.csv` with ADF-compatible formatting

## Step 2: Split CSV into Chunks
Use `csvsplitter.py` to split the CSV into manageable chunks:

```bash
python csvsplitter.py
```

This will:
- Read `client_t.csv`
- Split it into 100MB chunks
- Save chunks in `client_t_chunks/` directory

## Complete Workflow Example

```bash
# Step 1: Convert SAS to CSV
python sas2csv.py
# Output: ✅ Converted 50,000 rows to client_t.csv

# Step 2: Split into chunks
python csvsplitter.py
# Output: ✅ Done. 5 chunk(s) created in 'client_t_chunks'.

# Your files are now ready for ADF!
```

## ADF Dataset Configuration

Use these settings in your ADF dataset:

```json
{
    "type": "DelimitedText",
    "typeProperties": {
        "columnDelimiter": ",",
        "rowDelimiter": "\n",
        "encodingName": "UTF-8",
        "escapeChar": "\"",
        "quoteChar": "\"",
        "firstRowAsHeader": true,
        "treatEmptyAsNull": false
    }
}
```

## Why This Works

1. **sas2csv.py** ensures consistent CSV formatting from the start
2. **csvsplitter.py** maintains that formatting while splitting
3. Both use the same quoting rules (QUOTE_ALL)
4. NaN values are handled properly for ADF

## Customization

### Different SAS file:
Edit `sas2csv.py` line 5:
```python
df, meta = pyreadstat.read_sas7bdat("your_file.sas7bdat")
```

### Different chunk size:
Edit `csvsplitter.py` line 42:
```python
split_csv_by_size('client_t.csv', 'client_t_chunks', max_bytes=50 * 1024 * 1024)  # 50MB chunks
``` 