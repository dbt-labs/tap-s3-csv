import csv
import re


def generator_wrapper(reader):
    to_return = {}

    for row in reader:
        for key, value in row.items():
            if key is None:
                key = '_s3_extra'

            formatted_key = key

            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", '', formatted_key)

            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", '_', formatted_key)

            to_return[formatted_key.lower()] = value

        yield to_return


def get_row_iterator(table_spec, file_handle):
    field_names = None

    if 'field_names' in table_spec:
        field_names = table_spec['field_names']

    reader = csv.DictReader(file_handle, fieldnames=field_names)

    return generator_wrapper(reader)
