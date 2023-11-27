import pandas as pd
from faker import Faker
import random
from datetime import date

def generate_sort_code():
    """Generate a random bank sort code."""
    return "{}{}{}".format(random.randint(10, 99), random.randint(10, 99), random.randint(10, 99))

def calculate_age(birth_date):
    today = date.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

def generate_fake_age(fake, min_age=20, max_age=90):
    dob = fake.date_of_birth(minimum_age=min_age, maximum_age=max_age)
    return calculate_age(dob)

# Function to replace column values with fake data
def replace_columns_with_fake_data(dataframe, column_mappings):
    fake = Faker('en_GB')
    fake_data_functions = {
        "first_name": fake.first_name,
        "last_name": fake.last_name,
        "name": fake.name,
        "age": lambda: generate_fake_age(fake),
        "random_int": fake.random_int,
        "city": fake.city,
        "state": lambda: random.choice(["ENG", "SCO", "WAL", "NIR"]),
        "address_line1": fake.street_address,
        "address_line2": fake.secondary_address,
        "zipcode": fake.postcode,
        "comments": fake.sentence,
        "phone_number": fake.phone_number,
        "email": fake.email,
        "company": fake.company,
        "job": fake.job,
        "date_of_birth": fake.date_of_birth,
        "organization": fake.company,
        "sort_code": generate_sort_code,
        "alphanumeric": lambda: fake.bothify(text='??????'),
        "text": fake.word
    }

    for column, fake_data_type in column_mappings.items():
        if fake_data_type in fake_data_functions:
            column_index = int(column) - 1
            dataframe.iloc[:, column_index] = dataframe.iloc[:, column_index].apply(
                lambda x: fake_data_functions[fake_data_type]() if pd.notnull(x) and x.strip() != '' else None
            )
        else:
            print(f"Invalid fake data type for column {column}!")

    return dataframe
