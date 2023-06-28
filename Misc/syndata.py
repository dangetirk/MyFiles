
import csv
from faker import Faker
import random

# Define the masking function for free text comments
def mask_comments(comment):
    tokens = comment.split()
    mask_ratio = 0.5  # proportion of tokens to replace
    num_to_replace = int(len(tokens) * mask_ratio)  # number of tokens to replace
    replace_indices = random.sample(range(len(tokens)), num_to_replace)
    for index in replace_indices:
        tokens[index] = "****"
    return " ".join(tokens)

# Define the masking function for address fields
def mask_address_field(address_field):
    fake = Faker('en_GB')
    return getattr(fake, address_field)()

# Open the CSV file for reading and writing
with open('input.csv', 'r') as input_file, open('output.csv', 'w', newline='') as output_file:
    csv_reader = csv.reader(input_file)
    csv_writer = csv.writer(output_file)

    # Write the header row to the output file
    header_row = next(csv_reader)
    csv_writer.writerow(header_row)

    # Define the columns to mask and how to mask them
    mask_columns = {
        2: {'type': 'comments', 'mask_function': mask_comments},
        11: {'type': 'address', 'mask_fields': ['street_address', 'secondary_address', 'postcode'], 'mask_function': mask_address_field},
        # Add more columns to mask here, e.g.:
        # 5: {'type': 'comments', 'mask_function': mask_comments},
        # 17: {'type': 'address', 'mask_fields': ['street_address', 'secondary_address', 'postcode'], 'mask_function': mask_address_field},
    }

    # Write the masked rows to the output file
    for row in csv_reader:
        for column, mask_info in mask_columns.items():
            if mask_info['type'] == 'comments':
                row[column] = mask_info['mask_function'](row[column])
            elif mask_info['type'] == 'address':
                for field in mask_info['mask_fields']:
                    row[column + field_offsets[field]] = mask_info['mask_function'](row[column + field_offsets[field]])
        csv_writer.writerow(row)
