import os
import sys

# Add the 'packages' directory to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'packages'))

import re
import argparse
import json
from typing import Any, Dict, List, Iterator, Generator, Tuple
from azure.storage.blob import BlobServiceClient, ContentSettings
from io import BytesIO, BufferedReader, RawIOBase
import ijson
import itertools

# Recommended: Use the faster C backend if available
try:
    import ijson.backends.c_yajl2 as ijson_backend
except ImportError:
    import ijson.backends.python as ijson_backend 
    print("Warning: C backend for ijson not found. Falling back to slower Python backend.")

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
    Expands only the first list-of-dicts field in a row into multiple rows.
    All other lists (simple or nested) are serialized into strings.
    Prevents Cartesian explosion by not cross-joining multiple lists.
    """
    base_row = {}
    expandable_list_key = None
    expandable_list = []

    for k, v in row.items():
        if isinstance(v, list) and v and isinstance(v[0], dict) and expandable_list_key is None:
            expandable_list_key = k
            expandable_list = v
        else:
            base_row[k] = json.dumps(v) if isinstance(v, list) else v

    if expandable_list_key is None:
        yield base_row
    else:
        for item in expandable_list:
            new_row = base_row.copy()
            flat = flatten_json(item, parent_key=f"{expandable_list_key}_")
            new_row.update(flat)
            yield new_row


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

class CsvStreamer(RawIOBase):
    """
    A file-like object that generates CSV data on the fly.
    It takes an iterator of dictionaries and yields byte chunks for CSV.
    """
    def __init__(self, row_iterator: Iterator[Dict[str, Any]], headers: List[str]):
        self.row_iterator = row_iterator
        self.headers = headers
        self._buffer = BytesIO()
        self._write_header = True
        self._row_count = 0

    def readable(self):
        return True

    def _write_to_internal_buffer(self):
        """Writes the next chunk of CSV data to the internal BytesIO buffer."""
        if self._write_header:
            header_line = (','.join(map(escape_csv_value, self.headers)) + '\n').encode('utf-8')
            self._buffer.write(header_line)
            self._write_header = False

        try:
            row = next(self.row_iterator)
            line = ','.join(escape_csv_value(row.get(h)) for h in self.headers) + '\n'
            self._buffer.write(line.encode('utf-8'))
            self._row_count += 1
        except StopIteration:
            pass # No more rows

    def read(self, n=-1):
        """Reads up to n bytes from the stream."""
        if self._buffer.tell() == self._buffer.getbuffer().nbytes: # If buffer is empty or fully consumed
            self._buffer = BytesIO() # Reset buffer
            self._write_to_internal_buffer()
            if self._buffer.tell() == 0: # If nothing was written to buffer, means iterator is exhausted
                return b'' # End of stream

        # Read from internal buffer
        self._buffer.seek(0)
        chunk = self._buffer.read(n if n != -1 else self._buffer.getbuffer().nbytes)
        # Shift remaining data to the beginning of the buffer for next read
        remaining = self._buffer.read()
        self._buffer = BytesIO(remaining)
        return chunk
    
    def get_row_count(self):
        return self._row_count

def main():
    """Main function to orchestrate the JSON to CSV conversion."""
    args = parse_args()
    
    blob_service = BlobServiceClient.from_connection_string(args.AZURE_STORAGE_CONNECTION_STRING)
    input_blob_client = blob_service.get_blob_client(container=args.INPUT_CONTAINER_NAME, blob=args.INPUT_BLOB_PATH_PREFIX)

    print(f"Downloading and processing blob: {input_blob_client.blob_name}")
    
    # Get the blob input stream directly. download_blob() returns a BlobStream object
    # which is a file-like object that reads chunks on demand.
    download_stream = input_blob_client.download_blob()

    # Determine the ijson path based on NESTED_PATH
    ijson_path = f'{args.NESTED_PATH}.item' if args.NESTED_PATH else 'item'
    
    # Use ijson_backend.items directly with the download_stream
    # This is highly memory-efficient as ijson pulls data as needed.
    print(f"Streaming JSON items from path: {ijson_path}")
    json_iterator = ijson_backend.items(download_stream, ijson_path)
    
    # Create a processing pipeline using generators
     # Flatten and expand only top-level list-of-dict fields (no cross-joins)
    def expanded_generator():
        row_count = 0
        for obj in json_iterator:
            for row in expand_rows_generator(flatten_json(obj)):
                row_count += 1
                if row_count % 10000 == 0:
                    print(f"Processed {row_count} rows...")
                yield row

    expanded_row_iterator = expanded_generator()

    
    # Get the first row to determine headers, but don't hold the whole iterator
    try:
        first_row = next(expanded_row_iterator)
        headers = list(first_row.keys())
    except StopIteration:
        print("Warning: JSON stream was empty, no CSV file created.")
        return

    # Chain the first row back to the iterator for the CSV streamer
    # This is crucial for not losing the first row after extracting headers.
    full_row_iterator = itertools.chain([first_row], expanded_row_iterator)

    # Define output path
    base_name = os.path.splitext(os.path.basename(args.INPUT_BLOB_PATH_PREFIX))[0]
    nested_part = sanitize_filename(args.NESTED_PATH) if args.NESTED_PATH else ""
    csv_filename = f"{base_name}{'_' + nested_part if nested_part else ''}.csv"
    output_path = os.path.join(args.OUTPUT_BLOB_PATH_PREFIX, csv_filename)
    
    output_blob_client = blob_service.get_blob_client(container=args.OUTPUT_CONTAINER_NAME, blob=output_path)
    
    print(f"Starting upload to {output_blob_client.blob_name}...")

    # Create an instance of our streaming CSV writer
    csv_streamer = CsvStreamer(full_row_iterator, headers)

    # Upload the CSV data directly from the CsvStreamer
    # The Azure SDK will read chunks from csv_streamer as it uploads.
    output_blob_client.upload_blob(csv_streamer, overwrite=True, content_settings=ContentSettings(content_type='text/csv'))
    
    num_rows = csv_streamer.get_row_count()
    if num_rows > 0:
        print(f"Successfully wrote {num_rows} rows and {len(headers)} columns to: {output_path}")
    else:
        print("No data was written.")
            
if __name__ == "__main__":
    main()

    #powershell -Command "Invoke-WebRequest -Uri https://aka.ms/vs/17/release/vc_redist.x64.exe -OutFile vc_redist.x64.exe; Start-Process -FilePath .\\vc_redist.x64.exe -ArgumentList '/install', '/passive', '/norestart' -Wait; Remove-Item .\\vc_redist.x64.exe"