import os
import csv

MAX_CHUNK_SIZE = 25 * 1024 * 1024  # 25 MB limit

base_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(base_dir, 'mashup.csv')
output_dir = os.path.join(base_dir, 'sub_mash')

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

with open(input_file, 'r', newline='', encoding='utf-8') as infile:
    reader = csv.reader(infile)
    header = next(reader)

    chunk_index = 1
    output_file = os.path.join(output_dir, f'chunk_{chunk_index}.csv')
    outfile = open(output_file, 'w', newline='', encoding='utf-8')
    writer = csv.writer(outfile)
    writer.writerow(header)
    current_size = outfile.tell()

    for row in reader:
        row_data = ','.join(row) + '\n'  # roughly estimate row size
        row_size = len(row_data.encode('utf-8'))

        if current_size + row_size > MAX_CHUNK_SIZE:
            outfile.close()
            chunk_index += 1
            output_file = os.path.join(output_dir, f'chunk_{chunk_index}.csv')
            outfile = open(output_file, 'w', newline='', encoding='utf-8')
            writer = csv.writer(outfile)
            writer.writerow(header)
            current_size = outfile.tell()

        writer.writerow(row)
        current_size = outfile.tell()

    outfile.close()

print(f"Created {chunk_index} chunk files in '{output_dir}' directory.")
