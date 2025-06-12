import os
import argparse
import pandas as pd
import numpy as np

def escape_field(field):
    """Escape quotes and backslashes using backslash escaping for ADF compatibility."""
    field_str = str(field)
    field_str = field_str.replace('\\', '\\\\')
    field_str = field_str.replace('"', '\\"')
    return f'"{field_str}"'

def stream_sas_to_csv_chunks(
    input_sas_path,
    output_dir,
    max_bytes_per_chunk,
    read_chunk_size
):
    """
    Reads a SAS file in chunks and writes it to multiple CSV files, splitting
    them by size in a memory-efficient, streaming manner.
    """
    print(f"Starting conversion of {input_sas_path}...")
    os.makedirs(output_dir, exist_ok=True)
    base_filename = os.path.splitext(os.path.basename(input_sas_path))[0]

    # Use pandas' chunked reader for SAS files to avoid loading the whole
    # dataset in memory at once. `read_sas` with `chunksize` returns an
    # iterator that yields DataFrames of up to `read_chunk_size` rows.
    try:
        reader = pd.read_sas(input_sas_path, chunksize=read_chunk_size)
    except Exception as e:
        print(f"Error reading SAS file in chunks: {e}")
        return

    chunk_number = 1
    outfile = None
    current_size = 0
    header = None
    header_line = ''
    header_size = 0

    print("Reading SAS file and writing CSV chunks...")
    for df_chunk in reader:
        if header is None:
            header = df_chunk.columns.tolist()
            escaped_header = [escape_field(h) for h in header]
            header_line = ','.join(escaped_header) + '\n'
            header_size = len(header_line.encode('utf-8'))

            chunk_path = os.path.join(output_dir, f'{base_filename}_{chunk_number}.csv')
            print(f"  - Started new chunk: {chunk_path}")
            outfile = open(chunk_path, mode='w', newline='', encoding='utf-8')
            outfile.write(header_line)
            current_size = header_size
        
        for _, row in df_chunk.iterrows():
            def cell_to_str(cell):
                """Convert cell to string suitable for CSV: blank for NA/NaT/None."""
                import math
                import pandas as _pd
                import numpy as _np
                # Handle bytes -> decode first
                if isinstance(cell, bytes):
                    cell = cell.decode('utf-8', 'replace')
                # Pandas NA types / numpy.nan / None
                if cell is None or (isinstance(cell, float) and math.isnan(cell)) or cell is _pd.NA or cell is _pd.NaT or (isinstance(cell, _pd.Timestamp) and _pd.isna(cell)):
                    return ''
                # pandas Timestamp -> format without time component if it's exactly midnight
                if isinstance(cell, _pd.Timestamp):
                    if cell.hour == 0 and cell.minute == 0 and cell.second == 0 and cell.microsecond == 0:
                        return cell.strftime('%Y-%m-%d')
                    else:
                        return cell.strftime('%Y-%m-%d %H:%M:%S')
                # Numeric seconds since midnight -> HH:MM:SS  (SAS time)
                if isinstance(cell, (int, float)):
                    # treat as SAS time if within 0->86400 range
                    if 0 <= cell < 86_400:
                        total_sec = int(round(cell))
                        h = total_sec // 3600
                        m = (total_sec % 3600) // 60
                        s = total_sec % 60
                        return f"{h:02d}:{m:02d}:{s:02d}"
                return str(cell)

            str_row = [cell_to_str(item) for item in row]
            
            escaped_row = [escape_field(field) for field in str_row]
            line_to_write = ','.join(escaped_row) + '\n'
            row_size = len(line_to_write.encode('utf-8'))

            if current_size + row_size > max_bytes_per_chunk and current_size > header_size:
                outfile.close()
                print(f"  - Closed chunk {chunk_number} at {current_size / (1024*1024):.2f} MB.")
                
                chunk_number += 1
                chunk_path = os.path.join(output_dir, f'{base_filename}_{chunk_number}.csv')
                outfile = open(chunk_path, mode='w', newline='', encoding='utf-8')
                outfile.write(header_line)
                current_size = header_size
                print(f"  - Started new chunk: {chunk_path}")

            outfile.write(line_to_write)
            current_size += row_size

    if outfile:
        outfile.close()
        print(f"  - Closed final chunk {chunk_number} at {current_size / (1024*1024):.2f} MB.")

    print(f"✅ Done. {chunk_number} chunk(s) created in '{output_dir}'.")


def main():
    parser = argparse.ArgumentParser(
        description="""
        Convert a large SAS (.sas7bdat) file to multiple smaller CSV files in a
        memory-efficient, streaming way.
        This script is designed to run in environments with memory constraints,
        like Azure Batch.
        """
    )
    parser.add_argument("input_sas_path", help="Path to the input SAS (.sas7bdat) file.")
    parser.add_argument("output_dir", help="Directory where the output CSV chunks will be saved.")
    parser.add_argument(
        "--max-mb",
        type=int,
        default=100,
        help="Maximum size of each CSV chunk in Megabytes (MB). Default is 100."
    )
    parser.add_argument(
        "--read-chunk-size",
        type=int,
        default=50000,
        help="Number of rows to read from the SAS file into memory at a time. Default is 50000."
    )
    args = parser.parse_args()

    max_bytes = args.max_mb * 1024 * 1024

    stream_sas_to_csv_chunks(
        args.input_sas_path,
        args.output_dir,
        max_bytes_per_chunk=max_bytes,
        read_chunk_size=args.read_chunk_size
    )

if __name__ == "__main__":
    main() 