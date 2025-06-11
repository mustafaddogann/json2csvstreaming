# ADF Configuration Options to Try

Since QUOTE_MINIMAL didn't work, here are several ADF configurations to test:

## Configuration 1: Standard Double-Quote Escaping
```json
{
    "type": "DelimitedText",
    "typeProperties": {
        "columnDelimiter": ",",
        "rowDelimiter": "\n",
        "encodingName": "UTF-8",
        "escapeChar": "\"",
        "quoteChar": "\"",
        "firstRowAsHeader": true
    }
}
```

## Configuration 2: No Escape Character
```json
{
    "type": "DelimitedText",
    "typeProperties": {
        "columnDelimiter": ",",
        "rowDelimiter": "\n",
        "encodingName": "UTF-8",
        "quoteChar": "\"",
        "firstRowAsHeader": true
    }
}
```
(Remove escapeChar entirely)

## Configuration 3: Windows Line Endings
```json
{
    "type": "DelimitedText",
    "typeProperties": {
        "columnDelimiter": ",",
        "rowDelimiter": "\r\n",
        "encodingName": "UTF-8",
        "escapeChar": "\"",
        "quoteChar": "\"",
        "firstRowAsHeader": true
    }
}
```

## Configuration 4: With Additional Settings
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
        "treatEmptyAsNull": false,
        "skipLineCount": 0,
        "nullValue": ""
    }
}
```

## Debugging Steps:

1. **Run the diagnostic script** on your CSV chunk:
   ```bash
   python diagnose_csv.py client_t_chunks/client_t_1.csv
   ```

2. **Check the specific error** in ADF:
   - Is it "column count mismatch"?
   - Is it "parsing error"?
   - Does it mention a specific row number?

3. **Test with a small file** first:
   - Create a test CSV with just 10 rows from your data
   - Include the problematic row: `"asd, asdsad ""sas,"" dsada "" dsadsa/dsadsa."""`
   - Try each configuration above

4. **Check ADF activity logs** for the exact error message and row number

## Alternative: Use Different Delimiter

If commas in data are causing issues, you could modify both scripts to use a different delimiter:

In `sas2csv.py`, add:
```python
df.to_csv(
    "client_t.csv",
    index=False,
    sep='|',  # Use pipe delimiter
    quoting=csv.QUOTE_ALL,
    # ... rest of parameters
)
```

In `csvsplitter.py`, modify:
```python
reader = csv.reader(infile, delimiter='|')
# ...
writer = csv.writer(outfile, delimiter='|', quoting=csv.QUOTE_ALL)
```

Then update ADF:
```json
"columnDelimiter": "|"
``` 