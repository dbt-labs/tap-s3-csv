from smart_open import smart_open

import tap_s3_csv.csv_handler
import tap_s3_csv.excel_handler


def get_file_handle(config, s3_path):
    bucket = config['bucket']
    s3_uri = f"s3://{bucket}/{s3_path}"
    return smart_open(s3_uri, 'r')


def get_row_iterator(config, table_spec, s3_path):
    file_handle = get_file_handle(config, s3_path)

    if table_spec['format'] == 'csv':
        return tap_s3_csv.csv_handler.get_row_iterator(
            table_spec, file_handle)

    elif table_spec['format'] == 'excel':
        return tap_s3_csv.excel_handler.get_row_iterator(
            table_spec, file_handle)
