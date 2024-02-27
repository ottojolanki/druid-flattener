import argparse
import gzip
import json
import jsonlines
import os
import re
from multiprocessing import Pool, cpu_count

def flatten_record(obj):
    annotations = obj.pop('annotations', {})
    freq = annotations.pop('freq', {})
    for key, value in freq.items():
        obj[f'freq_{key}_ref'] = value.get('ref:long', None)
        obj[f'freq_{key}_alt'] = value.get('alt:long', None)
    obj.update({f'ann_{key}': value for key, value in annotations.items()})
    return obj

def duplicate_entries_for_rsid(obj):
    rsids = obj.pop('rsid', [])
    duplicated_entries = []
    for rsid in rsids:
        duplicated_obj = obj.copy()
        duplicated_obj['rsid'] = rsid
        duplicated_entries.append(duplicated_obj)
    return duplicated_entries

def process_file(file_info):
    input_file, input_dir, output_dir = file_info
    input_path = os.path.join(input_dir, input_file)
    output_filename = re.sub(r'\.gz', '.jsonl.gz')
    output_path = os.path.join(output_dir, output_filename)
    with gzip.open(input_path, 'rb') as gz_in, gzip.open(output_path, mode='wb') as gz_out:
        reader = jsonlines.Reader(gz_in)
        writer = jsonlines.Writer(gz_out)
        for obj in reader:
            flattened_obj = flatten_record(obj)
            duplicated_entries = duplicate_entries_for_rsid(flattened_obj)
            for entry in duplicated_entries:
                writer.write(entry)

def process_directory(input_dir, output_dir):
    files = [f for f in os.listdir(input_dir) if f.endswith('.gz')]
    file_paths = [(file, input_dir, output_dir) for file in files]
    with Pool(cpu_count()) as pool:
        pool.map(process_file, file_paths)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-dir', type=str, help='Input directory containing .jsonl.gz files')
    parser.add_argument('--output-dir', type=str, help='Output directory for processed files')
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    process_directory(args.input_dir, args.output_dir)

if __name__ == '__main__':
    main()
