import os
import sys
import argparse
import io
import math
from datetime import datetime, timedelta
from typing import Any

# --- Ensure 'packages' directory is importable in Azure Batch ---
try:
    app_path = os.path.abspath(sys.argv[0])
    app_dir = os.path.dirname(app_path)
    packages_dir = os.path.join(app_dir, 'packages')
    if os.path.isdir(packages_dir):
        sys.path.insert(0, packages_dir)
        os.environ["PATH"] = packages_dir + os.pathsep + os.environ["PATH"]
except Exception:
    pass

import pandas as pd
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    BlobSasPermissions,
    generate_blob_sas,
)

# ---------- CSV helpers (same logic as sas_stream_chunker.py) ----------

def escape_field(field: Any) -> str:
    field_str = str(field)
    field_str = field_str.replace("\\", "\\\\")
    field_str = field_str.replace("\"", "\\\"")
    return f'"{field_str}"'

def cell_to_str(cell):
    """Replicates conversions done in sas_stream_chunker.py"""
    import pandas as _pd
    if cell is None or (isinstance(cell, float) and math.isnan(cell)) or cell is _pd.NA or cell is _pd.NaT or (
        isinstance(cell, _pd.Timestamp) and _pd.isna(cell)
    ):
        return ""
    if isinstance(cell, _pd.Timestamp):
        if (
            cell.hour == 0
            and cell.minute == 0
            and cell.second == 0
            and cell.microsecond == 0
        ):
            return cell.strftime("%Y-%m-%d")
        return cell.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(cell, (int, float)) and 0 <= cell < 86_400:
        total_sec = int(round(cell))
        h = total_sec // 3600
        m = (total_sec % 3600) // 60
        s = total_sec % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    return str(cell)

# ---------- Main streaming logic ----------

def sas_blob_to_chunked_csv(
    blob_service: BlobServiceClient,
    input_container: str,
    input_blob_path: str,
    output_container: str,
    output_prefix: str,
    max_mb: int = 100,
    read_chunk_size: int = 50_000,
):
    """Reads a SAS file from blob and uploads size-capped CSV chunks back to blob."""
    # Create local temp file for the SAS download
    tmp_sas_path = os.path.join(os.getcwd(), "input.sas7bdat")
    print(f"Downloading SAS blob to local temp file {tmp_sas_path} ...")
    input_blob_client = blob_service.get_blob_client(container=input_container, blob=input_blob_path)

    # Generate SAS URL for streaming via requests (faster than SDK)
    sas = generate_blob_sas(
        account_name=input_blob_client.account_name,
        container_name=input_container,
        blob_name=input_blob_path,
        account_key=blob_service.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=2),
    )
    url_with_sas = f"{input_blob_client.url}?{sas}"

    import requests

    with requests.get(url_with_sas, stream=True) as r, open(tmp_sas_path, "wb") as f:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    print("Download complete.")

    base_filename = os.path.splitext(os.path.basename(input_blob_path))[0]
    max_bytes = max_mb * 1024 * 1024

    print("Beginning streaming conversion to CSV chunks ...")

    reader = pd.read_sas(tmp_sas_path, chunksize=read_chunk_size)

    header_line: str = ""
    header_size = 0
    chunk_number = 1
    current_size = 0
    buffer = io.BytesIO()

    def flush_chunk(buf: io.BytesIO, number: int):
        buf.seek(0)
        blob_name = f"{output_prefix}{base_filename}_{number}.csv"
        print(f"Uploading chunk {number}: {blob_name} ({buf.getbuffer().nbytes / (1024*1024):.2f} MB)")
        out_client = blob_service.get_blob_client(container=output_container, blob=blob_name)
        out_client.upload_blob(buf, overwrite=True, content_settings=ContentSettings(content_type="text/csv"))
        buf.truncate(0)
        buf.seek(0)

    for df_chunk in reader:
        if not header_line:
            headers = df_chunk.columns.tolist()
            escaped_header = [escape_field(h) for h in headers]
            header_line = ",".join(escaped_header) + "\n"
            buffer.write(header_line.encode("utf-8"))
            header_size = len(header_line.encode("utf-8"))
            current_size = header_size

        for _, row in df_chunk.iterrows():
            values = [cell_to_str(x) for x in row]
            line = ",".join(escape_field(v) for v in values) + "\n"
            line_bytes = line.encode("utf-8")
            # If adding this row would exceed the limit and we already have data, flush first.
            if current_size + len(line_bytes) > max_bytes and current_size > header_size:
                flush_chunk(buffer, chunk_number)
                chunk_number += 1
                # start new chunk with header
                buffer.write(header_line.encode("utf-8"))
                current_size = header_size
            buffer.write(line_bytes)
            current_size += len(line_bytes)

    # Final flush
    if current_size > header_size:
        flush_chunk(buffer, chunk_number)

    print("✅ All chunks uploaded.")
    # Clean up temp file
    try:
        os.remove(tmp_sas_path)
    except Exception:
        pass

# ---------- CLI ----------

def parse_args():
    p = argparse.ArgumentParser("Stream SAS7BDAT from blob and write chunked CSVs back to blob")
    p.add_argument("AZURE_STORAGE_CONNECTION_STRING")
    p.add_argument("INPUT_CONTAINER")
    p.add_argument("INPUT_BLOB_PATH")
    p.add_argument("OUTPUT_CONTAINER")
    p.add_argument("OUTPUT_BLOB_PREFIX", help="Folder or prefix where <base>_N.csv will be placed, e.g. 'client_t_chunks/'")
    p.add_argument("--max-mb", type=int, default=100)
    p.add_argument("--read-chunk-size", type=int, default=50000)
    return p.parse_args()


def main():
    args = parse_args()
    print("--- batch_sas_stream_chunker starting ---")

    blob_service = BlobServiceClient.from_connection_string(args.AZURE_STORAGE_CONNECTION_STRING)

    sas_blob_to_chunked_csv(
        blob_service,
        args.INPUT_CONTAINER,
        args.INPUT_BLOB_PATH,
        args.OUTPUT_CONTAINER,
        args.OUTPUT_BLOB_PREFIX,
        max_mb=args.max_mb,
        read_chunk_size=args.read_chunk_size,
    )

    print("Done.")


if __name__ == "__main__":
    main() 