import codecs
import csv
import re

import boto3
from tap_s3_csv.logger import LOGGER as logger


def formatted_key_generator(reader):
    to_return = {}

    for row in reader:
        for key, value in row.items():
            formatted_key = key

            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", '', formatted_key)

            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", '_', formatted_key)

            to_return[formatted_key.lower()] = value

        yield to_return


def get_row_iterator(config, s3_path):
    bucket = config['bucket']
    s3_client = boto3.resource(
        's3',
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key'])

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    body = s3_object.get()['Body']

    # we use a protected member of the s3 object, _raw_stream, here to create
    # a generator for data from the s3 file.
    # pylint: disable=protected-access
    file_stream = codecs.iterdecode(body._raw_stream, encoding='utf-8')

    reader = csv.DictReader(file_stream)

    return formatted_key_generator(reader)


def sample_file(config, s3_path, sample_rate, max_records):
    logger.info('Sampling {} ({} records, every {}th record).'
                .format(s3_path, max_records, sample_rate))

    samples = []

    iterator = get_row_iterator(config, s3_path)

    current_row = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    logger.info('Sampled {} records.'.format(len(samples)))

    return samples


def sample_files(config, s3_files,
                 sample_rate=10, max_records=1000, max_files=5):
    to_return = []

    files_so_far = 0

    for s3_file in s3_files:
        to_return += sample_file(config, s3_file['key'],
                                 sample_rate, max_records)

        files_so_far += 1

        if files_so_far >= max_files:
            break

    return to_return


def get_input_files_for_table(config, table_spec, modified_since=None):
    bucket = config['bucket']

    to_return = []
    pattern = table_spec['pattern']
    matcher = re.compile(pattern)

    logger.debug(
        'Checking bucket "{}" for keys matching "{}"'
        .format(bucket, pattern))

    s3_objects = list_files_in_bucket(config, bucket)

    for s3_object in s3_objects:
        key = s3_object['Key']
        last_modified = s3_object['LastModified']

        logger.debug('Last modified: {}'.format(last_modified))

        if(matcher.search(key) and
           (modified_since is None or modified_since < last_modified)):
            logger.debug('Will download key "{}"'.format(key))
            to_return.append({'key': key, 'last_modified': last_modified})
        else:
            logger.debug('Will not download key "{}"'.format(key))

    to_return = sorted(to_return, key=lambda item: item['last_modified'])

    return to_return


def list_files_in_bucket(config, bucket):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key'])

    s3_objects = []

    max_results = 1000
    result = s3_client.list_objects_v2(
        Bucket=bucket,
        MaxKeys=max_results)

    s3_objects += result['Contents']
    next_continuation_token = result.get('NextContinuationToken')

    while next_continuation_token is not None:
        logger.debug('Continuing pagination with token "{}".'
                     .format(next_continuation_token))
        result = s3_client.list_objects_v2(
            Bucket=bucket,
            ContinuationToken=next_continuation_token,
            MaxKeys=max_results)

        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    logger.info("Found {} files.".format(len(s3_objects)))

    return s3_objects
