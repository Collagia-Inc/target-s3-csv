#!/usr/bin/env python3

from datetime import datetime
import time
import singer
import json
import re
import collections
import inflection

from decimal import Decimal
from datetime import datetime

logger = singer.get_logger('target_s3_csv')


def validate_config(config):
    """Validates config"""
    errors = []
    required_config_keys = [
        's3_bucket'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append(
                "Required key is missing from config: [{}]".format(k))

    return errors


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    
    schema_message['schema']['properties'] = {key.upper() if key.startswith('_') else key: value 
        for key, value in schema_message['schema']['properties'].items()}
    schema_message["key_properties"] = [key.upper() if key.startswith('_') else key for key in schema_message["key_properties"]]
    # extended_schema_message['schema']['properties']['_sdc_batched_at'] = {
    #     'type': ['null', 'string'], 'format': 'date-time'}
    # extended_schema_message['schema']['properties']['_sdc_deleted_at'] = {
    #     'type': ['null', 'string']}
    # extended_schema_message['schema']['properties']['_sdc_extracted_at'] = {
    #     'type': ['null', 'string'], 'format': 'date-time'}
    # extended_schema_message['schema']['properties']['_sdc_primary_key'] = {
    #     'type': ['null', 'string']}
    # extended_schema_message['schema']['properties']['_sdc_received_at'] = {
    #     'type': ['null', 'string'], 'format': 'date-time'}
    # extended_schema_message['schema']['properties']['_sdc_sequence'] = {
    #     'type': ['integer']}
    # extended_schema_message['schema']['properties']['_sdc_table_version'] = {
    #     'type': ['null', 'string']}

    return extended_schema_message


def add_metadata_values_to_record(record_message, schema_message, config):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    record_message["record"] = {key.upper() if key.startswith('_') else key: value for key, value in record_message["record"].items()}
    extended_record = record_message['record']
    

    # extended_record['_sdc_batched_at'] = datetime.now().isoformat()
    # extended_record['_sdc_deleted_at'] = record_message.get(
    #     'record', {}).get('_sdc_deleted_at')
    # extended_record['_sdc_extracted_at'] = record_message.get('time_extracted')
    # extended_record['_sdc_primary_key'] = schema_message.get('key_properties')
    # extended_record['_sdc_received_at'] = datetime.now().isoformat()
    # extended_record['_sdc_sequence'] = int(round(time.time() * 1000))
    # extended_record['_sdc_table_version'] = record_message.get('version')

    if config.get('custom_metadata_columns'):
        metadata_dict = json.loads(config['custom_metadata_columns'])
        for key, val in metadata_dict.items():
            extended_record[key] = val


    return extended_record


def remove_metadata_values_from_record(record_message, config):
    """Removes every metadata _sdc column from a given record message
    """
    record_message["record"] = {key.upper() if key.startswith('_') else key: value for key, value in record_message["record"].items()}
    cleaned_record = record_message['record']
    # cleaned_record.pop('_sdc_batched_at', None)
    # cleaned_record.pop('_sdc_deleted_at', None)
    # cleaned_record.pop('_sdc_extracted_at', None)
    # cleaned_record.pop('_sdc_primary_key', None)
    # cleaned_record.pop('_sdc_received_at', None)
    # cleaned_record.pop('_sdc_sequence', None)
    # cleaned_record.pop('_sdc_table_version', None)

    if config.get('custom_metadata_columns'):
        metadata_dict = json.loads(config['custom_metadata_columns'])
        for key, _ in metadata_dict.items():
            cleaned_record.pop(key, None)

    return cleaned_record


# pylint: disable=unnecessary-comprehension
def flatten_key(k, parent_key, sep):
    """
    """
    full_key = parent_key + [k]
    inflected_key = [n for n in full_key]
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 255 and reducer_index < len(inflected_key):
        reduced_key = re.sub(
            r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) >
             1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_record(d, parent_key=[], sep='__'):
    """
    """
    items = []
    for k in sorted(d.keys()):
        v = d[k]
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_record(v, parent_key + [k], sep=sep).items())
        else:
            items.append((new_key, json.dumps(v) if type(v) is list else v))
    return dict(items)


def get_target_key(message, prefix=None, naming_convention=None, partition_value=None):
    """Creates and returns an S3 key for the message"""
    if not naming_convention:
        # o['stream'] + '-' + now + '.csv'
        naming_convention = '{stream}-{timestamp}.csv'
    key = naming_convention

    # replace simple tokens
    tokens_dict={
        '{stream}': message['stream'],
        '{partition_key}': partition_value,
        '{timestamp}': datetime.now().strftime('%Y%m%dT%H%M%S'),
        '{date}': datetime.now().strftime('%Y-%m-%d')
    }
    if message.get("record",{}).get("UPDATED_TS"):
        tokens_dict["{updated_ts}"]=datetime.strptime(str(message['record']['UPDATED_TS'])
            , '%Y-%m-%dT%H:%M:%S.%f%z').strftime("%Y%m%dT%H%M%S.%f")
    
    for k, v in tokens_dict.items():
        if k in key:
            key = key.replace(k, v)

    # replace dynamic tokens
    # todo: replace dynamic tokens such as {date(<format>)} with the date formatted as requested in <format>

    if prefix:
        filename = key.split('/')[-1]
        key = key.replace(filename, f'{prefix}{filename}')
    return key
