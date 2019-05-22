import codecs
import csv
import re
import gzip


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
    if table_spec.get('unzip'):
        raw_stream = gzip.GzipFile(fileobj=file_handle._raw_stream)
    else:
        raw_stream = file_handle._raw_stream

    # we use a protected member of the s3 object, _raw_stream, here to create
    # a generator for data from the s3 file.
    # pylint: disable=protected-access
    file_stream = codecs.iterdecode(
        raw_stream, encoding='utf-8')

    field_names = None

    if 'field_names' in table_spec:
        field_names = table_spec['field_names']

    delimiter = table_spec.get('delimiter', ',')

    quote_config = table_spec.get('quoting', 'QUOTE_MINIMAL')
    quoting = getattr(csv, quote_config)

    reader = csv.DictReader(file_stream, quoting=quoting, delimiter=delimiter, fieldnames=field_names)

    return generator_wrapper(reader)
