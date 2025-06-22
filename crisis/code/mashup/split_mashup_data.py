import os
import csv
import math

base_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(base_dir, 'mashup.csv')
output_dir = os.path.join(base_dir, 'sub_mash')
num_chunks = 5

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# count total rows
with open(input_file, 'r', encoding='utf-8') as f:
    total_rows = sum(1 for line in f) - 1  # subtract header row

rows_per_chunk = math.ceil(total_rows / num_chunks)

with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)

    for chunk_index in range(1, num_chunks + 1):
        output_file = os.path.join(output_dir, f'chunk_{chunk_index}.csv')
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)

            rows_written = 0
            # write rows_per_chunk rows into this chunk
            while rows_written < rows_per_chunk:
                try:
                    row = next(reader)
                    writer.writerow(row)
                    rows_written += 1
                except StopIteration:
                    # no more rows left at this point (or hopefully)
                    break

print(f"Created {num_chunks} chunk files in '{output_dir}' directory.")
