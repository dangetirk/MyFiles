import pandas as pd
import os
import random
import configparser
import sys
from datetime import datetime
import re
import csv

def read_config_file(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def mask_postcode(postcode):
    if ' ' in postcode:
        parts = postcode.split(' ')
        first_part = parts[0]
        second_part = 'X' * len(parts[1])
        return f"{first_part} {second_part}"
    else:
        return f"{postcode[:2]}{'Z' * (len(postcode) - 2)}"

def replace_first_name(name):
    if 'whereare' in name:
        return 'KING'
    elif 'sastry' in name:
        return 'MILLER'
    elif 'jusf' in name:
        return 'SCOTT'
    elif re.search(r'jaju.*rriv', name):
        return 'CHLOE'
    else:
        return name

def process_config_entries(config_entries, input_dir, mask_folder):
    path = os.getcwd()
    for entry in config_entries:
        src_file = entry['src_file']
        column_mappings = entry['columns']
        src_folder = entry['src_folder']
        output_dir = os.path.join(path, mask_folder, src_folder)
        os.makedirs(output_dir, exist_ok=True)
        input_file_path = os.path.join(input_dir, src_folder, src_file)

        if not os.path.isfile(input_file_path):
            print(f"The file '{input_file_path}' does not exist. Skipping this file.")
            continue

        output_file = os.path.join(output_dir, src_file.replace('.csv', '_mask.csv'))
        with open(input_file_path, 'r', newline='') as file:
            reader = csv.reader(file, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
            lines = list(reader)

        header = '|'.join(lines[0]) + '\n'
        footer = '|'.join(lines[-1]) + '\n'
        body_lines = lines[1:-1]
        body = pd.DataFrame(body_lines, dtype=str)
        column_mappings = dict(column.split('=') for column in column_mappings.split(','))

        if '1' in column_mappings:
            body.iloc[:, 0] = body.iloc[:, 0].apply(replace_first_name)
        if '5' in column_mappings:
            body.iloc[:, 4] = body.iloc[:, 4].apply(mask_postcode)

        body = body.applymap(lambda x: x.replace("|", ",").replace("\n", " ") if isinstance(x, str) else x)
        csv_body = body.to_csv(index=False, sep='|', header=False, quoting=csv.QUOTE_NONE, quotechar="", escapechar="\\")

        with open(output_file, 'w', newline='\n') as file:
            file.write(header)
            file.write(csv_body)
            file.write(footer)

        print(f"Masked file saved to {output_file}")

if len(sys.argv) < 2:
    print("Please provide a path to the configuration file as an argument.")
    sys.exit(1)

config_file = sys.argv[1]
config = read_config_file(config_file)
base_path = config.get('PATHS', 'base_path')
input_folder = config.get('PATHS', 'input_folder')
mask_folder = config.get('PATHS', 'mask_folder')
input_dir = os.path.join(base_path, input_folder)
config_entries = [
    dict(config[section])
    for section in config.sections() if section != 'PATHS'
]

process_config_entries(config_entries, input_dir, mask_folder)
