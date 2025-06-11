import os
import sys
import io
import csv
import json
import logging
import argparse
import itertools
import requests
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions
from typing import Any, Dict, List, Iterator, Generator, Tuple
import ijson
import re

# Add the 'packages' directory to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'packages'))

# Recommended: Use the faster C backend if available
try:
    import ijson.backends.yajl2_cffi as ijson_backend
    print("Using yajl2_cffi ijson backend.")
except ImportError:
    try:
        import ijson.backends.yajl2_c as ijson_backend
        print("Using yajl2_c ijson backend.")
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
    """
    Main function to orchestrate the JSON to CSV conversion process.
    """
    print(f"--- Running script version 1.2: SAS Token Streaming ---")
    
    args = parse_args()

    # Add the 'packages' directory to sys.path
    packages_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'packages')
    sys.path.insert(0, packages_path)

    try:
        blob_service = BlobServiceClient.from_connection_string(args.AZURE_STORAGE_CONNECTION_STRING)
        input_blob_client = blob_service.get_blob_client(container=args.INPUT_CONTAINER_NAME, blob=args.INPUT_BLOB_PATH_PREFIX)

        # Generate a SAS token to stream the blob directly with the `requests` library.
        # This bypasses a bug in the Azure SDK's streaming downloader and allows true streaming.
        print("Generating SAS token for direct download.")
        sas_token = generate_blob_sas(
            account_name=input_blob_client.account_name,
            container_name=input_blob_client.container_name,
            blob_name=input_blob_client.blob_name,
            account_key=blob_service.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
        blob_url_with_sas = f"{input_blob_client.url}?{sas_token}"

        print(f"Streaming blob directly from URL using requests...")
        with requests.get(blob_url_with_sas, stream=True) as r:
            r.raise_for_status()

            ijson_path = f'{args.NESTED_PATH}.item' if args.NESTED_PATH else 'item'
            json_iterator = ijson_backend.items(r.raw, ijson_path)

            # Create a processing pipeline using generators
            def expanded_generator():
                for obj in json_iterator:
                    # In this version, we only expand top-level list-of-dicts
                    # More complex logic would be needed for deeper nesting or cross-products
                    expanded_rows = [obj]
                    list_fields_to_expand = {k for k, v in obj.items() if isinstance(v, list) and v and isinstance(v[0], dict)}

                    if list_fields_to_expand:
                        base_row = {k: v for k, v in obj.items() if k not in list_fields_to_expand}
                        # Assumption: expand only the first found list-of-dicts field
                        field_to_expand = list(list_fields_to_expand)[0]
                        expanded_rows = [{**base_row, **sub_dict} for sub_dict in obj[field_to_expand]]
                    
                    for row in expanded_rows:
                        yield row

            expanded_row_iterator = expanded_generator()

            try:
                first_row = next(expanded_row_iterator)
                headers = list(first_row.keys())
            except StopIteration:
                print("Warning: JSON stream was empty, no CSV file created.")
                return

            full_row_iterator = itertools.chain([first_row], expanded_row_iterator)

            # Define output path
            output_blob_name = os.path.basename(args.INPUT_BLOB_PATH_PREFIX).replace('.json', '.csv')
            output_blob_path = os.path.join(os.path.dirname(args.INPUT_BLOB_PATH_PREFIX), output_blob_name)
            
            # Create an instance of our streaming CSV writer
            csv_streamer = CsvStreamer(full_row_iterator, headers)

            # Upload the CSV data directly from the CsvStreamer
            output_blob_client = blob_service.get_blob_client(container=args.INPUT_CONTAINER_NAME, blob=output_blob_path)
            
            print(f"Uploading processed CSV to: {output_blob_path}")
            output_blob_client.upload_blob(csv_streamer, overwrite=True, content_settings=ContentSettings(content_type='text/csv'))
            print("Upload complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
        # In a real-world scenario, you might want more specific error handling
        # and possibly exit with a non-zero status code.
        sys.exit(1)

if __name__ == "__main__":
    main()

    #powershell -Command "Invoke-WebRequest -Uri https://aka.ms/vs/17/release/vc_redist.x64.exe -OutFile vc_redist.x64.exe; Start-Process -FilePath .\\vc_redist.x64.exe -ArgumentList '/install', '/passive', '/norestart' -Wait; Remove-Item .\\vc_redist.x64.exe"