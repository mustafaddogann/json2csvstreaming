import os
import csv

def split_csv_by_size(input_csv_path, output_dir, max_bytes=100 * 1024 * 1024):
    """
    Splits a CSV into chunks of approximately max_bytes each, escaping special characters properly.
    All chunks use consistent quoting for ADF compatibility.
    """
    os.makedirs(output_dir, exist_ok=True)

    base_filename = os.path.splitext(os.path.basename(input_csv_path))[0]

    with open(input_csv_path, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)

        chunk_number = 1
        chunk_path = os.path.join(output_dir, f'{base_filename}_{chunk_number}.csv')
        outfile = open(chunk_path, mode='w', newline='', encoding='utf-8')
        writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        current_size = outfile.tell()

        for row in reader:
            writer.writerow(row)
            current_size = outfile.tell()

            if current_size >= max_bytes:
                outfile.close()
                chunk_number += 1
                chunk_path = os.path.join(output_dir, f'{base_filename}_{chunk_number}.csv')
                outfile = open(chunk_path, mode='w', newline='', encoding='utf-8')
                writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(header)
                current_size = outfile.tell()

        outfile.close()

    print(f"Done. {chunk_number} chunk(s) created in '{output_dir}'.")

# Run the function
if __name__ == "__main__":
    split_csv_by_size('client_t.csv', 'client_t_chunks', max_bytes=100 * 1024 * 1024)
