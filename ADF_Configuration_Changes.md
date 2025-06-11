# ADF Dataset Configuration Changes

## Current Configuration (Backslash Escaping)
```json
"escapeChar": "\\",
"quoteChar": "\""
```

## New Configuration (Double-Quote Escaping)
```json
"escapeChar": "\"",
"quoteChar": "\""
```

## Key Change
Change the `escapeChar` from `\\` (backslash) to `\"` (double quote)

## Why This Works Better
1. **Smaller file sizes**: Double-quote escaping doesn't add extra characters for backslashes
2. **Standard CSV format**: This matches Python's default CSV output
3. **Better compatibility**: Most CSV tools use double-quote escaping by default

## Example Data Comparison

### With Backslash Escaping (larger):
```
"John","5'5\" tall,123","a"
"Test\\Path","Quote\"Test","Normal"
```

### With Double-Quote Escaping (smaller):
```
"John","5'5"" tall,123","a"
"Test\Path","Quote""Test","Normal"
```

## How Double-Quote Escaping Works
- When a field contains a quote, it's escaped by doubling it: `"` becomes `""`
- Example: `John's "Big" Day` becomes `"John's ""Big"" Day"`
- No need to escape backslashes, they remain as single `\` 