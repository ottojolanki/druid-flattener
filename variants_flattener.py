import argparse
import gzip
import json

import jsonlines

def flatten_record(obj):
    # Flatten the 'annotations' and 'freq' nested objects
    annotations = obj.pop('annotations', {})
    freq = annotations.pop('freq', {})
    # Handle variable-length nested objects in 'freq'
    for key, value in freq.items():
        obj[f'freq_{key}_ref'] = value.get('ref:long', None)
        obj[f'freq_{key}_alt'] = value.get('alt:long', None)
    # Flatten and add other annotations as new fields
    obj.update({f'ann_{key}': value for key, value in annotations.items()})
    return obj

def duplicate_entries_for_rsid(obj):
    rsids = obj.pop('rsid', [])
    duplicated_entries = []
    for rsid in rsids:
        # Duplicate the object for each rsid
        duplicated_obj = obj.copy()
        duplicated_obj['rsid'] = rsid
        duplicated_entries.append(duplicated_obj)
    return duplicated_entries

def process_jsonl(input_file, output_file):
    with gzip.open(input_file, 'rb') as gz_in, gzip.open(output_file, mode='wb') as gz_out:
        reader = jsonlines.Reader(gz_in)
        writer = jsonlines.Writer(gz_out)
        for obj in reader:
            # First, flatten the record
            flattened_obj = flatten_record(obj)
            # Then duplicate the entry for each rsid and write to the output
            duplicated_entries = duplicate_entries_for_rsid(flattened_obj)
            for entry in duplicated_entries:
                writer.write(entry)
def main(args):
    process_jsonl(args.input_file, args.output_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-file', type=str)
    parser.add_argument('--output-file', type=str)
    args = parser.parse_args()
    main(args)
