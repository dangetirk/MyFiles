import csv
import yaml

source_file = 'table.csv'
output_file = 'dbt_tests.yml'

# Define data types for which tests should be generated
valid_datatypes = ['string', 'time', 'boolean', 'date', 'datetime', 'decimal', 'integer', 'NUMERIC']

models = []
with open(source_file, mode='r') as f_in:
    reader = csv.DictReader(f_in)
    for row in reader:
        model_name = row['table_name']
        column_name = row['column_name']
        datatype = row['datatype'].lower()
        size = row['size']
        mandatory_check = row['MandatoryCheck'].lower() == 'yes'
        column = {'name': column_name, 'tests': []}
        if datatype in valid_datatypes:
            if datatype == 'string':
                column['tests'].append({'length_check': {'max_length': int(size)}})
                column['tests'].append({'dbt_expectations.expect_column_values_to_be_of_type': {'column_type': 'string'}})
            elif datatype == 'time':
                column['tests'].append({'dbt_expectations.expect_column_values_to_be_of_type': {'column_type': 'time'}})
            elif datatype == 'date':
                column['tests'].append({'dbt_expectations.expect_column_values_to_be_of_type': {'column_type': 'date'}})
            elif datatype == 'datetime':
                column['tests'].append({'dbt_expectations.expect_column_values_to_be_of_type': {'column_type': 'datetime'}})
            elif datatype == 'decimal':
                column['tests'].append({'dbt_expectations.expect_column_values_to_be_of_type': {'column_type': 'numeric'}})
            elif datatype == 'integer':
                column['tests'].append({'dbt_expectations.expect_column_values_to_be_of_type': {'column_type': 'integer'}})
            elif datatype == 'boolean':
                column['tests'].append({'dbt_expectations.expect_column_values_to_be_of_type': {'column_type': 'boolean'}})
            if mandatory_check:
                column['tests'].append({'dbt_expectations.expect_column_values_to_not_be_null': {}})
            model = next((m for m in models if m['name'] == model_name), None)
            if model is None:
                model = {'name': model_name, 'columns': []}
                models.append(model)
            model['columns'].append(column)

with open(output_file, mode='w') as f_out:
    yaml_out = yaml.dump({'models': models}, sort_keys=False)
    f_out.write(yaml_out)
