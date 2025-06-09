import os
import re
import json
import argparse
import itertools
from typing import Any, Dict, List, Generator
from io import BytesIO, RawIOBase
import ijson

# Try using C backend
try:
    import ijson.backends.c_yajl2 as ijson_backend
except ImportError:
    import ijson.backends.python as ijson_backend
    print("⚠️ Falling back to slower Python backend.")

def parse_args():
    parser = argparse.ArgumentParser(description="Convert large local JSON to CSV")
    parser.add_argument("input_file", help="Path to the input JSON file.")
    parser.add_argument("output_file", help="Path to the output CSV file.")
    parser.add_argument("--nested_path", default="", help="Optional dot path like 'data.items'")
    parser.add_argument("--max_records", type=int, default=None, help="Limit processing to first N records.")
    return parser.parse_args()

def flatten_json(y: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    out = {}
    def flatten(x: Any, name: str = ''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f"{name}{a}{sep}")
        elif isinstance(x, list):
            out[name[:-1]] = json.dumps(x)
        else:
            out[name[:-1]] = x
    flatten(y, parent_key)
    return out

def expand_rows_generator(row: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
    base_row = {k: v for k, v in row.items() if not (isinstance(v, list) and v and isinstance(v[0], dict))}
    to_expand = {k: v for k, v in row.items() if isinstance(v, list) and v and isinstance(v[0], dict)}

    if not to_expand:
        yield base_row
        return

    queue = [(base_row, to_expand)]
    while queue:
        current_base, current_lists = queue.pop(0)
        if not current_lists:
            yield current_base
            continue
        first_key = next(iter(current_lists))
        items = current_lists[first_key]
        rest = {k: v for k, v in current_lists.items() if k != first_key}

        for item in items:
            new_part = flatten_json(item, f"{first_key}_")
            combined = current_base.copy()
            combined.update(new_part)
            if rest:
                queue.append((combined, rest.copy()))
            else:
                yield combined

def escape_csv_value(value: Any) -> str:
    if value is None:
        return ''
    return f'"{str(value).replace("\"", "\"\"")}"'

def main():
    args = parse_args()
    ijson_path = f'{args.nested_path}.item' if args.nested_path else 'item'

    print(f"📥 Reading from: {args.input_file}")
    with open(args.input_file, 'rb') as f:
        json_iter = ijson_backend.items(f, ijson_path)
        flattened = (flatten_json(obj) for obj in json_iter)
        expanded = (row for flat in flattened for row in expand_rows_generator(flat))
        if args.max_records:
            expanded = itertools.islice(expanded, args.max_records)

        try:
            first_row = next(expanded)
        except StopIteration:
            print("⚠️ No data found in JSON path.")
            return

        headers = list(first_row.keys())
        expanded = itertools.chain([first_row], expanded)

        with open(args.output_file, 'w', encoding='utf-8', newline='') as out_file:
            out_file.write(','.join(escape_csv_value(h) for h in headers) + '\n')
            row_count = 0
            for row in expanded:
                line = ','.join(escape_csv_value(row.get(h)) for h in headers) + '\n'
                out_file.write(line)
                row_count += 1

        print(f"✅ Wrote {row_count} rows with {len(headers)} columns to {args.output_file}")

if __name__ == "__main__":
    main()
