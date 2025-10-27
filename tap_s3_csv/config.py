import json
from datetime import datetime, timedelta

from tap_s3_csv.logger import LOGGER as logger
from voluptuous import Schema, Required, Any, Optional

CONFIG_CONTRACT = Schema({
    Required('aws_access_key_id'): str,
    Required('aws_secret_access_key'): str,
    Required('start_date'): str,
    Required('bucket'): str,
    Optional('lookback_days'): int,  # If set, overrides start_date with dynamic calculation
    Required('tables'): [{
        Required('name'): str,
        Required('pattern'): str,
        Required('key_properties'): [str],
        Required('format'): Any('csv', 'excel', 'jsonl'),
        Optional('search_prefix'): str,
        Optional('field_names'): [str],
        Optional('worksheet_name'): str,
        Optional('max_results'): int,
        Optional('sample_max_records'): int,
        Optional('sample_rate'): int,
        Optional('sample_max_files'): int,
        Optional('schema_overrides'): {
            str: {
                Required('type'): Any(str, [str]),
                Required('_conversion_type'): Any('string',
                                                  'integer',
                                                  'number',
                                                  'date-time')
            }
        }
    }]
})


def load(filename):
    config = {}

    try:
        with open(filename) as handle:
            config = json.load(handle)
    except:
        logger.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError

    CONFIG_CONTRACT(config)
    
    # If lookback_days is specified, calculate start_date dynamically
    if 'lookback_days' in config:
        days = config['lookback_days']
        calculated_date = datetime.utcnow() - timedelta(days=days)
        config['start_date'] = calculated_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        logger.info(f"Calculated start_date from lookback_days={days}: {config['start_date']}")

    return config
