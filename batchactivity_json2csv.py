import os
import re
import argparse
import json
from typing import Any, Dict, List, Iterator, Generator, Tuple
from azure.storage.blob import BlobServiceClient, ContentSettings
from io import BytesIO
import ijson

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Convert large JSON files from Azure Blob Storage to CSV.")
    parser.add_argument("AZURE_STORAGE_CONNECTION_STRING", help="Azure Storage connection string.")
    parser.add_argument("INPUT_CONTAINER_NAME", help="Name of the input container.")
    parser.add_argument("INPUT_BLOB_PATH_PREFIX", help="Full path to the input JSON blob.")
    parser.add_argument("OUTPUT_CONTAINER_NAME", help="Name of the output container.")
    parser.add_argument("OUTPUT_BLOB_PATH_PREFIX", help="Path prefix for the output CSV file.")
    parser.add_argument("NESTED_PATH", nargs='?', default="", help="Optional dot-separated path to a nested array to extract, e.g., 'data.records'.")
    return parser.parse_args()

# ---------- UTILS ----------

def flatten_json(y: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """Flattens a nested dictionary."""
    out = {}
    def flatten(x: Any, name: str = ''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f"{name}{a}{sep}")
        elif isinstance(x, list):
            # To prevent memory issues, we avoid expanding lists here.
            # Lists of simple types will be converted to a string.
            # Lists of dicts should be handled by expand_rows_generator.
            out[name[:-1]] = json.dumps(x)
        else:
            out[name[:-1]] = x
    flatten(y, parent_key)
    return out

def expand_rows_generator(row: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
    """
    Expands a single row into multiple rows if it contains lists of dictionaries.
    This is a generator to keep memory usage low.
    """
    base_row = {k: v for k, v in row.items() if not (isinstance(v, list) and v and isinstance(v[0], dict))}
    list_of_dicts_to_expand = {k: v for k, v in row.items() if isinstance(v, list) and v and isinstance(v[0], dict)}

    if not list_of_dicts_to_expand:
        yield base_row
        return

    # Iteratively expand one list of dicts at a time
    current_key = next(iter(list_of_dicts_to_expand))
    remaining_items = {k:v for k,v in list_of_dicts_to_expand.items() if k != current_key}
    
    for item in list_of_dicts_to_expand[current_key]:
        new_row = base_row.copy()
        new_row.update(flatten_json(item, f"{current_key}{'_'}"))
        new_row.update(remaining_items)
        # Recursively expand other lists
        yield from expand_rows_generator(new_row)


def sanitize_filename(filename: str) -> str:
    """Removes illegal characters from a filename."""
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

def escape_csv_value(value: Any) -> str:
    """Escapes a value for CSV formatting."""
    if value is None:
        return ''
    # Convert to string, escape double quotes, and wrap in double quotes
    s_val = str(value).replace('"', '""')
    return f'"{s_val}"'

def upload_csv_stream(blob_client: Any, row_iterator: Iterator[Dict[str, Any]]):
    """
    Streams rows to a CSV file in Azure Blob Storage using BytesIO buffer to avoid memory issues.
    """
    try:
        first_row = next(row_iterator)
    except StopIteration:
        print("Warning: JSON stream was empty, no CSV file created.")
        return 0, []

    headers = list(first_row.keys())
    buffer = BytesIO()
    row_count = 0

    def write_row(row: Dict[str, Any]):
        nonlocal buffer
        line = ','.join(escape_csv_value(row.get(h)) for h in headers) + '\n'
        buffer.write(line.encode('utf-8'))

    # Write header
    buffer.write((','.join(map(escape_csv_value, headers)) + '\n').encode('utf-8'))
    write_row(first_row)
    row_count = 1

    # Write remaining rows
    for row in row_iterator:
        write_row(row)
        row_count += 1

    buffer.seek(0)
    print(f"Starting upload to {blob_client.blob_name}...")

    blob_client.upload_blob(buffer, overwrite=True, content_settings=ContentSettings(content_type='text/csv'))

    return row_count, headers

# ---------- MAIN LOGIC ----------

def get_json_iterator(stream: Any, nested_path: str) -> Iterator[Dict]:
    """
    Returns an iterator over JSON objects from a stream, targeting a nested path if provided.
    """
    if nested_path:
        # Stream items from a nested array, e.g., 'data.records.item'
        path_prefix = f"{nested_path}.item"
        print(f"Streaming from nested path: {path_prefix}")
        return ijson.items(stream, path_prefix)
    
    # Check the first character to determine if it's a root array or object
    first_bytes = stream.read(1024)
    if not first_bytes:
        return iter([]) # Empty file
        
    start_char = first_bytes.decode('utf-8', errors='ignore').lstrip()[0]
    
    # We need a way to "chain" the first bytes back to the stream
    import itertools
    combined_stream = itertools.chain(iter([first_bytes]), stream)

    if start_char == '[':
        print("Detected root JSON array. Streaming items.")
        return ijson.items(combined_stream, 'item')
    elif start_char == '{':
        print("Detected root JSON object. Attempting to find and stream the first array.")
        # This is a heuristic: find the first major array and stream its items.
        # This may need adjustment based on the actual JSON structure.
        try:
            # Using a low-level parser to find the first array to stream
            parser = ijson.parse(combined_stream)
            for prefix, event, _ in parser:
                if event == 'start_array':
                    # We found an array, now stream its items
                    # The prefix tells us where the array is.
                    return ijson.items(combined_stream, f"{prefix}.item")
        except Exception as e:
            print(f"Could not stream from root object, treating as single document. Error: {e}")
            # Fallback for single object: wrap it in a list to make it iterable
            stream.seek(0)
            return iter([json.load(stream)])
    
    raise ValueError(f"Unsupported JSON structure starting with character: {start_char}")


def main():
    """Main function to orchestrate the JSON to CSV conversion."""
    args = parse_args()
    
    blob_service = BlobServiceClient.from_connection_string(args.AZURE_STORAGE_CONNECTION_STRING)
    input_blob_client = blob_service.get_blob_client(container=args.INPUT_CONTAINER_NAME, blob=args.INPUT_BLOB_PATH_PREFIX)

    print(f"Downloading and processing blob: {input_blob_client.blob_name}")
    
    blob_data = input_blob_client.download_blob().readall()
    stream = BytesIO(blob_data)

    # Get an iterator that yields JSON objects from the source file
    json_iterator = get_json_iterator(stream, args.NESTED_PATH)
    
    # Create a processing pipeline using generators
    flattened_iterator = (flatten_json(obj) for obj in json_iterator)
    expanded_row_iterator = (row for flat_row in flattened_iterator for row in expand_rows_generator(flat_row))
    
    # Define output path
    base_name = os.path.splitext(os.path.basename(args.INPUT_BLOB_PATH_PREFIX))[0]
    nested_part = sanitize_filename(args.NESTED_PATH) if args.NESTED_PATH else ""
    csv_filename = f"{base_name}{'_' + nested_part if nested_part else ''}.csv"
    output_path = os.path.join(args.OUTPUT_BLOB_PATH_PREFIX, csv_filename)
    
    output_blob_client = blob_service.get_blob_client(container=args.OUTPUT_CONTAINER_NAME, blob=output_path)
    
    # Stream the processed rows directly to the output CSV blob
    num_rows, headers = upload_csv_stream(output_blob_client, expanded_row_iterator)
    if num_rows > 0:
        print(f"✅ Successfully wrote {num_rows} rows and {len(headers)} columns to: {output_path}")
    else:
        print("No data was written.")
if __name__ == "__main__":
    main()
