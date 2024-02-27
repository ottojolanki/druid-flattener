import argparse
import hashlib
from datetime import datetime, timedelta
import json

def generate_timestamp(key, start_date, end_date):
    """Generate a synthetic timestamp based on a hash of the key."""
    hash_object = hashlib.sha256(key.encode())
    hash_int = int(hash_object.hexdigest(), 16)
    date_range = (end_date - start_date).days
    hash_date_offset = hash_int % date_range
    return (start_date + timedelta(days=hash_date_offset)).isoformat()

def add_timestamps_to_jsonl(input_path, output_path, start_date, end_date):
    """Read a .jsonl file, add synthetic timestamps, and write to another .jsonl file."""
    with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
        for line in infile:
            obj = json.loads(line)
            # Assuming '_key' is the field to hash. Change this if necessary.
            key = obj.get('_key', '')
            timestamp = generate_timestamp(key, start_date, end_date)
            obj['timestamp'] = timestamp
            json.dump(obj, outfile)
            outfile.write('\n')

def main():
    parser = argparse.ArgumentParser(description='Add synthetic timestamps to JSON Lines data.')
    parser.add_argument('--input-file', type=str, help='Path to the input .jsonl file')
    parser.add_argument('--output-file', type=str, help='Path to the output .jsonl file')
    parser.add_argument('--start-date', type=str, default='2020-01-01', help='Start date for synthetic timestamps (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default='2025-01-01', help='End date for synthetic timestamps (YYYY-MM-DD)')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    add_timestamps_to_jsonl(args.input_file, args.output_file, start_date, end_date)

if __name__ == '__main__':
    main()

