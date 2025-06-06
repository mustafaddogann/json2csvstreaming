import os
import re
import argparse
import json
from typing import Any, Dict, List, Tuple
from azure.storage.blob import BlobServiceClient
from io import BytesIO, StringIO
import ijson

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("connection_string")
    parser.add_argument("input_container")
    parser.add_argument("input_path")
    parser.add_argument("output_container")
    parser.add_argument("output_path_prefix")
    parser.add_argument("nested_path")
    return parser.parse_args()

# ---------- UTILS ----------

def flatten_json(y: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    out = {}
    def flatten(x: Any, name: str = ''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f"{name}{a}{sep}")
        elif isinstance(x, list):
            if all(isinstance(i, dict) for i in x):
                return  # Skip nested dict-lists
            else:
                for i, item in enumerate(x):
                    flatten(item, f"{name}{i}{sep}")
        else:
            out[name[:-1]] = x
    flatten(y, parent_key)
    return out

def expand_rows(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    base_rows = [{}]
    for key, value in data.items():
        if isinstance(value, list) and all(isinstance(i, dict) for i in value):
            new_rows = []
            for row in base_rows:
                for item in value:
                    new_row = row.copy()
                    new_row.update(flatten_json(item, f"{key}_"))
                    new_rows.append(new_row)
            base_rows = new_rows
        else:
            for row in base_rows:
                row[key] = value
    return base_rows

def sanitize_filename(filename: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

def escape_csv_value(value: Any) -> str:
    if value is None:
        return '""'
    val = str(value).replace('\\', '\\\\').replace('"', '\\"')
    return f'"{val}"'

def write_csv_blob(blob_service: BlobServiceClient, container: str, blob_path: str, headers: List[str], rows: List[Dict[str, Any]]):
    output_stream = StringIO()
    output_stream.write(','.join([f'"{h}"' for h in headers]) + '\n')
    for row in rows:
        escaped_row = ','.join([escape_csv_value(row.get(h, "")) for h in headers])
        output_stream.write(escaped_row + '\n')
    output_stream.seek(0)
    blob_client = blob_service.get_blob_client(container=container, blob=blob_path)
    blob_client.upload_blob(output_stream.getvalue().encode('utf-8'), overwrite=True)
    print(f"Wrote CSV: {blob_path} with {len(headers)} columns and {len(rows)} rows.")

def build_ordered_headers(rows: List[Dict[str, Any]], reference_order: List[str] = None) -> List[str]:
    seen = set()
    ordered = []
    if reference_order:
        for key in reference_order:
            if key not in seen:
                seen.add(key)
                ordered.append(key)
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                ordered.append(key)
    return ordered

def extract_nested_rows(json_data: Any, nested_path: str) -> List[Dict[str, Any]]:
    def traverse(data, path_parts, parents, current_path):
        if not path_parts:
            full_prefix = '_'.join(current_path)
            if isinstance(data, list):
                rows = []
                for item in data:
                    row = {}
                    for parent_obj, parent_path in parents:
                        row.update(flatten_json(parent_obj, parent_path + '_'))
                    row.update(flatten_json(item, full_prefix + '_'))
                    rows.append(row)
                return rows
            elif isinstance(data, dict):
                row = {}
                for parent_obj, parent_path in parents:
                    row.update(flatten_json(parent_obj, parent_path + '_'))
                row.update(flatten_json(data, full_prefix + '_'))
                return [row]
            return []
        key = path_parts[0]
        if isinstance(data, dict):
            return traverse(data.get(key), path_parts[1:], parents + [(data, '_'.join(current_path))], current_path + [key])
        elif isinstance(data, list):
            rows = []
            for item in data:
                rows.extend(traverse(item, path_parts, parents, current_path))
            return rows
        return []
    return traverse(json_data, nested_path.split('.'), [], [])

# ---------- MAIN ----------

def main():
    args = parse_args()
    blob_service = BlobServiceClient.from_connection_string(args.connection_string)
    blob_client = blob_service.get_blob_client(container=args.input_container, blob=args.input_path)

    stream = blob_client.download_blob()
    first_byte = stream.read(1)
    if not first_byte:
        print("Blob is empty.")
        return

    full_stream = BytesIO(first_byte + stream.readall())
    full_stream.seek(0)
    start_char = first_byte.decode('utf-8').strip()

    if args.nested_path:
        full_stream.seek(0)
        raw_data = json.load(full_stream)
        nested_rows = extract_nested_rows(raw_data, args.nested_path)
        if not nested_rows:
            print(f"No valid nested rows found at path: {args.nested_path}")
            return

        reference_keys = list(flatten_json(raw_data).keys())
        headers = build_ordered_headers(nested_rows, reference_keys)
        base_name = os.path.splitext(os.path.basename(args.input_path))[0]
        nested_part = sanitize_filename(args.nested_path)
        csv_filename = f"{base_name}_{nested_part}.csv"
        output_path = os.path.join(args.output_path_prefix, csv_filename)
        write_csv_blob(blob_service, args.output_container, output_path, headers, nested_rows)

    elif start_char == '[':
        flat_rows = []
        reference_keys = []
        headers = []
        base_filename = os.path.splitext(os.path.basename(args.input_path))[0]
        csv_filename = base_filename + ".csv"
        output_path = os.path.join(args.output_path_prefix, csv_filename)

        output_stream = StringIO()
        first = True
        for item in ijson.items(full_stream, 'item'):
            flat = flatten_json(item)
            if first:
                reference_keys = list(flat.keys())
                headers = build_ordered_headers([flat])
                output_stream.write(','.join([f'"{h}"' for h in headers]) + '\n')
                first = False
            escaped_row = ','.join([escape_csv_value(flat.get(h, "")) for h in headers])
            output_stream.write(escaped_row + '\n')

        output_stream.seek(0)
        blob_client = blob_service.get_blob_client(container=args.output_container, blob=output_path)
        blob_client.upload_blob(output_stream.getvalue().encode('utf-8'), overwrite=True)
        print(f"Wrote CSV: {output_path} with {len(headers)} columns.")

    elif start_char == '{':
        full_stream.seek(0)
        raw_data = json.load(full_stream)
        records = []
        for v in raw_data.values():
            if isinstance(v, list):
                records = v
                break
        if not records:
            records = [raw_data]

        flat_rows = []
        reference_keys = list(flatten_json(records[0]).keys()) if records else []
        for record in records:
            flat = flatten_json(record)
            expanded = expand_rows(flat)
            flat_rows.extend(expanded)

        if not flat_rows:
            print("No valid rows found.")
            return

        headers = build_ordered_headers(flat_rows, reference_keys)
        base_filename = os.path.splitext(os.path.basename(args.input_path))[0]
        csv_filename = base_filename + ".csv"
        output_path = os.path.join(args.output_path_prefix, csv_filename)
        write_csv_blob(blob_service, args.output_container, output_path, headers, flat_rows)

    else:
        print(f"Unexpected first character in JSON: {start_char}")

if __name__ == "__main__":
    main()
