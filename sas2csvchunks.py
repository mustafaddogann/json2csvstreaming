import pyreadstat
import os
import csv

def sas_to_csv_chunks_streaming(sas_file_path, output_dir, chunk_size=100000):
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(sas_file_path))[0]

    reader = pyreadstat.read_file_in_chunks(pyreadstat.read_sas7bdat, sas_file_path, chunksize=chunk_size)
    
    for i, (df_chunk, _) in enumerate(reader, 1):
        output_path = os.path.join(output_dir, f"{base_name}_{i}.csv")
        df_chunk.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
        print(f"✅ Wrote chunk {i} with {len(df_chunk)} rows to: {output_path}")

    print("✅ All chunks written.")

# Example usage:
if __name__ == "__main__":
    sas_to_csv_chunks_streaming("client_t.sas7bdat", "client_t_chunks2", chunk_size=100000)
