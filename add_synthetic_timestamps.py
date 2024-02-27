import argparse
import hashlib
import gzip
import jsonlines
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
import os

def generate_timestamp(key, start_date, end_date):
    hash_object = hashlib.sha256(key.encode())
    hash_int = int(hash_object.hexdigest(), 16)
    date_range = (end_date - start_date).days
    hash_date_offset = hash_int % date_range
    return (start_date + timedelta(days=hash_date_offset)).isoformat()

def add_timestamps_to_file(file_info):
    input_file, input_dir, output_dir, start_date, end_date = file_info
    input_path = os.path.join(input_dir, input_file)
    output_filename = 'timestamped' + input_file
    output_path = os.path.join(output_dir, output_filename)
    with gzip.open(input_path, 'rb') as gz_in, gzip.open(output_path, 'wb') as gz_out:
        reader = jsonlines.Reader(gz_in)
        writer = jsonlines.Writer(gz_out)
        for obj in reader:
            key = obj.get('_key', '')
            timestamp = generate_timestamp(key, start_date, end_date)
            obj['timestamp'] = timestamp
            writer.write(obj)

def process_directory(input_dir, output_dir, start_date, end_date):
    files = [f for f in os.listdir(input_dir) if f.endswith('.jsonl.gz')]
    file_infos = [(file, input_dir, output_dir, start_date, end_date) for file in files]
    with Pool(cpu_count()) as pool:
        pool.map(add_timestamps_to_file, file_infos)

def main():
    parser = argparse.ArgumentParser(description='Add synthetic timestamps to gzipped JSON Lines data.')
    parser.add_argument('--input-dir', type=str, help='Input directory containing .jsonl.gz files')
    parser.add_argument('--output-dir', type=str, help='Output directory for timestamped files')
    parser.add_argument('--start-date', type=str, default='2020-01-01', help='Start date for synthetic timestamps (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default='2025-01-01', help='End date for synthetic timestamps (YYYY-MM-DD)')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    process_directory(args.input_dir, args.output_dir, start_date, end_date)

if __name__ == '__main__':
    main()

