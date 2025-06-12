import os
import sys
import time

# Add packages directory to path BEFORE any other imports
# This script needs to find dependencies in the 'packages' directory
try:
    # When running from Azure Batch, find packages relative to script location
    app_path = os.path.abspath(sys.argv[0])
    app_dir = os.path.dirname(app_path)
    packages_dir = os.path.join(app_dir, 'packages')
    if os.path.isdir(packages_dir):
        sys.path.insert(0, packages_dir)
        # Also add to system PATH for DLLs
        os.environ["PATH"] = packages_dir + os.pathsep + os.environ["PATH"]
except Exception:
    # Fallback for local execution
    cwd = os.getcwd()
    packages_dir = os.path.join(cwd, 'packages')
    if os.path.isdir(packages_dir) and packages_dir not in sys.path:
        sys.path.insert(0, packages_dir)
        os.environ["PATH"] = packages_dir + os.pathsep + os.environ["PATH"]

import io
import csv
import json
import logging
import argparse
import itertools
import requests
from io import BytesIO, RawIOBase
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions
from typing import Any, Dict, List, Iterator, Generator, Tuple
import ijson
import re

# Recommended: Use the faster C backend if available
try:
    import ijson.backends.yajl2_cffi as ijson_backend
    print("Using yajl2_cffi ijson backend.")
    backend_name = "yajl2_cffi"
except ImportError:
    try:
        import ijson.backends.yajl2_c as ijson_backend
        print("Using yajl2_c ijson backend.")
        backend_name = "yajl2_c"
    except ImportError:
        import ijson.backends.python as ijson_backend
        print("Warning: C backend for ijson not found. Falling back to slower Python backend.")
        backend_name = "python"

# Print diagnostic info about the backend
print(f"ijson backend module: {ijson_backend}")
print(f"ijson backend name: {backend_name}")
print(f"ijson backend file: {getattr(ijson_backend, '__file__', 'Unknown')}")

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
    start_time = time.time()
    
    print(f"--- Running script version 1.3: Chunked Output ---")
    print(f"Script started at: {datetime.now()}")
    
    args = parse_args()

    # Define chunk size for output files (e.g., 250MB)
    CHUNK_THRESHOLD_BYTES = 250 * 1024 * 1024

    try:
        blob_service = BlobServiceClient.from_connection_string(args.AZURE_STORAGE_CONNECTION_STRING)
        input_blob_client = blob_service.get_blob_client(container=args.INPUT_CONTAINER_NAME, blob=args.INPUT_BLOB_PATH_PREFIX)

        # Get blob size to determine if we need to chunk
        print("Getting input blob properties...")
        blob_properties = input_blob_client.get_blob_properties()
        input_blob_size = blob_properties.size
        print(f"Input blob size: {input_blob_size / (1024*1024):.2f} MB")

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
            # Use 1MB buffer for better performance on dedicated nodes
            json_iterator = ijson_backend.items(r.raw, ijson_path, buf_size=1024*1024)

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

            # --- UPLOAD LOGIC ---
            if input_blob_size < CHUNK_THRESHOLD_BYTES:
                # Original logic: process all at once for smaller files
                print("Input blob is smaller than threshold. Processing as a single CSV.")
                output_blob_name = os.path.basename(args.INPUT_BLOB_PATH_PREFIX).replace('.json', '.csv')
                output_blob_path = os.path.join(os.path.dirname(args.INPUT_BLOB_PATH_PREFIX), output_blob_name)
                
                # Create an instance of our streaming CSV writer
                csv_streamer = CsvStreamer(full_row_iterator, headers)

                # Upload the CSV data directly from the CsvStreamer
                output_blob_client = blob_service.get_blob_client(container=args.INPUT_CONTAINER_NAME, blob=output_blob_path)
                
                print(f"Uploading processed CSV to: {output_blob_path}")
                upload_start = time.time()
                output_blob_client.upload_blob(csv_streamer, overwrite=True, content_settings=ContentSettings(content_type='text/csv'))
                upload_end = time.time()
                print("Upload complete.")
                
                end_time = time.time()
                print(f"Upload time: {upload_end - upload_start:.2f} seconds")
                print(f"Total processing time: {end_time - start_time:.2f} seconds")
                print(f"Rows processed: {csv_streamer.get_row_count()}")
                print(f"Processing rate: {csv_streamer.get_row_count() / (end_time - start_time):.1f} rows/second")
            else:
                # New chunking logic for large files
                print(f"Input blob is large. Using chunked CSV output (chunk size: {CHUNK_THRESHOLD_BYTES / (1024*1024):.2f} MB).")
                chunk_number = 1
                total_rows_processed = 0
                output_blob_basename = os.path.basename(args.INPUT_BLOB_PATH_PREFIX).replace('.json', '')
                output_blob_dir = os.path.dirname(args.INPUT_BLOB_PATH_PREFIX)
                
                header_line = (','.join(map(escape_csv_value, headers)) + '\n').encode('utf-8')
                current_chunk_buffer = io.BytesIO()
                current_chunk_buffer.write(header_line)

                for row in full_row_iterator:
                    line = ','.join(escape_csv_value(row.get(h)) for h in headers) + '\n'
                    current_chunk_buffer.write(line.encode('utf-8'))
                    total_rows_processed += 1

                    if current_chunk_buffer.tell() >= CHUNK_THRESHOLD_BYTES:
                        output_blob_name = f"{output_blob_basename}_part_{chunk_number:04d}.csv"
                        output_blob_path = os.path.join(output_blob_dir, output_blob_name)
                        output_blob_client = blob_service.get_blob_client(container=args.INPUT_CONTAINER_NAME, blob=output_blob_path)
                        
                        print(f"Uploading chunk {chunk_number} ({current_chunk_buffer.tell() / (1024*1024):.2f} MB) to {output_blob_path}")
                        current_chunk_buffer.seek(0)
                        output_blob_client.upload_blob(current_chunk_buffer.getvalue(), overwrite=True, content_settings=ContentSettings(content_type='text/csv'))
                        print(f"Chunk {chunk_number} upload complete.")
                        
                        chunk_number += 1
                        current_chunk_buffer = io.BytesIO()
                        current_chunk_buffer.write(header_line)

                # Upload the last remaining chunk if it contains data
                if current_chunk_buffer.tell() > len(header_line):
                    output_blob_name = f"{output_blob_basename}_part_{chunk_number:04d}.csv"
                    output_blob_path = os.path.join(output_blob_dir, output_blob_name)
                    output_blob_client = blob_service.get_blob_client(container=args.INPUT_CONTAINER_NAME, blob=output_blob_path)

                    print(f"Uploading final chunk {chunk_number} ({current_chunk_buffer.tell() / (1024*1024):.2f} MB) to {output_blob_path}")
                    current_chunk_buffer.seek(0)
                    output_blob_client.upload_blob(current_chunk_buffer.getvalue(), overwrite=True, content_settings=ContentSettings(content_type='text/csv'))
                    print(f"Chunk {chunk_number} upload complete.")
                
                end_time = time.time()
                print(f"Total processing time: {end_time - start_time:.2f} seconds")
                print(f"Total rows processed: {total_rows_processed}")
                if (end_time - start_time) > 0:
                    print(f"Processing rate: {total_rows_processed / (end_time - start_time):.1f} rows/second")

    except Exception as e:
        print(f"An error occurred: {e}")
        # In a real-world scenario, you might want more specific error handling
        # and possibly exit with a non-zero status code.
        sys.exit(1)

if __name__ == "__main__":
    main()

    #powershell -Command "Invoke-WebRequest -Uri https://aka.ms/vs/17/release/vc_redist.x64.exe -OutFile vc_redist.x64.exe; Start-Process -FilePath .\\vc_redist.x64.exe -ArgumentList '/install', '/passive', '/norestart' -Wait; Remove-Item .\\vc_redist.x64.exe"