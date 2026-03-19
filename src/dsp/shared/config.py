import os
from contextlib import contextmanager
from typing import Dict, Union, Any

import boto3
import json

from dsp.shared.aws import service_config, s3_bucket, s3_object, s3_split_path, local_mode, get_toggles
from dsp.shared.constants import PATHS

_service_config = None


def databricks_kwargs(host: str, api_token: str) -> dict:
    return dict(
        host='https://{}'.format(host),
        token=api_token,
        verify=False if local_mode() else os.environ.get('YANAI_CA_BUNDLE', '/etc/ssl/certs/ca-certificates.crt')
    )


@contextmanager
def temp_service_config(config: Dict[str, Any]):
    global _service_config
    previous = _service_config

    _service_config = config

    yield config

    _service_config = previous


def _get_service_config() -> Dict[str, any]:

    config = service_config(os.environ.get('CONFIG_SECTION', 'apps'))

    if config.get('databricks_host'):
        config['databricks_kwargs'] = databricks_kwargs(
            config['databricks_host'], config['databricks_token']
        )

    config['toggles'] = get_toggles()
    return config


def _resolve_service_config() -> Dict[str, Union[str, dict, list]]:
    global _service_config

    if _service_config is None:

        _service_config = _get_service_config()

    return _service_config


def clear_service_config():
    global _service_config
    _service_config = None


def refresh_service_config():
    global _service_config
    _service_config = _get_service_config()


_not_set_sentinel = object()


def config_item(key: str, default: Any = _not_set_sentinel) -> Union[str, dict, list]:

    if default == _not_set_sentinel:
        return _resolve_service_config()[key]

    return _resolve_service_config().get(key, default)


def feature_toggles() -> Dict[str, bool]:

    toggles = config_item('toggles')  # type: Dict[str, Dict[str, bool]]
    return toggles['feature']


def service_toggles() -> Dict[str, bool]:

    toggles = config_item('toggles')  # type: Dict[str, Dict[str, bool]]
    return toggles['service']


def feature_enabled(key: str) -> bool:

    return feature_toggles()[key]


def service_enabled(key: str) -> bool:

    return service_toggles()[key]


def table_name(table: str) -> str:
    return '{}{}'.format(table, config_item('rds_table_suffix'))


def s3_raw_bucket() -> boto3.session.Session.resource:
    return s3_bucket(config_item('raw_bucket'))


def s3_extracts_bucket() -> boto3.session.Session.resource:
    return s3_bucket(config_item('extracts_bucket'))


def s3_load_metadata(working_folder: str, bucket: str = None) -> dict:
    if not bucket:
        _, bucket, working_folder = s3_split_path(working_folder)

    obj = s3_object(bucket, os.path.join(working_folder, PATHS.META_DOT_DATA)).get()

    metadata = json.loads(obj['Body'].read())
    metadata['working_folder'] = 's3://{}/{}'.format(bucket, working_folder)

    return metadata
