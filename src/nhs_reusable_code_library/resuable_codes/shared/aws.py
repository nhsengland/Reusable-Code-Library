import gzip
import io
import json
import os
import re
import subprocess
from datetime import datetime
from collections import defaultdict
from functools import wraps, reduce, partial, lru_cache
from json import JSONDecodeError
from multiprocessing import Process, Pipe, connection
from subprocess import Popen, PIPE, STDOUT
from time import sleep
from typing import Union, List, Tuple, Callable, Iterable, Dict, Generator, Any, Optional
from urllib import parse
from uuid import uuid4

import boto3
import botocore.credentials
import botocore.session
import yaml
from boto3.dynamodb.types import TypeDeserializer
from botocore.config import Config
from botocore.exceptions import ClientError, UnknownServiceError
from botocore.parsers import ResponseParserError

from nhs_reusable_code_library.resuable_codes.shared.common import strtobool, retry
from nhs_reusable_code_library.resuable_codes.shared.common.s3_object_buffer import S3ObjectBuffer
from nhs_reusable_code_library.resuable_codes.shared.logger import log_action, add_fields

LOCAL_MODE = 'LOCAL_MODE'
AWS_REGION = 'eu-west-2'

_TEMP_LOCATION_KEY_PREFIX = "temp/"

s3re = re.compile(r'^(s3(?:a|n)?):\/\/([^\/]+)\/(.+)$', re.IGNORECASE)


def s3_build_uri(bucket: str, key: str) -> str:
    return 's3://{}/{}'.format(bucket, key)


def s3_build_uri_from_keys(bucket: str, *keys: str) -> str:
    return f"s3://{bucket}/{'/'.join(keys).replace('//', '/')}"


def s3_split_path(s3uri: str) -> Tuple[str, str, str]:
    match = s3re.match(s3uri)
    if not match:
        raise ValueError("Not a s3 uri: {}".format(s3uri))
    scheme, bucket, key = match.groups()
    return scheme, bucket, key


def local_mode() -> bool:
    return strtobool(os.environ.get(LOCAL_MODE, 'False'))


def boto3_thing(service: str, local_endpoint: str, factory: Callable, **kwargs) -> boto3.client:
    def _create() -> boto3.client:
        if local_mode():
            return factory(
                service, endpoint_url=local_endpoint, region_name=AWS_REGION,
                aws_access_key_id='abc', aws_secret_access_key='123', **kwargs
            )

        return factory(service, region_name=AWS_REGION, **kwargs)

    boto_thing = _create()

    return boto_thing


def s3_client(session: boto3.Session = None) -> boto3.session.Session.client:
    return boto3_thing(
        's3', 'http://{}:8080'.format(os.environ.get('LOCAL_S3_HOST', 'localhost')),
        session.client if session else boto3.client
    )


def s3_resource(session: boto3.Session = None) -> boto3.session.Session.resource:
    return boto3_thing(
        's3', 'http://{}:8080'.format(os.environ.get('LOCAL_S3_HOST', 'localhost')),
        session.resource if session else boto3.resource
    )


def dynamodb(session: boto3.Session = None) -> boto3.session.Session.resource:
    return boto3_thing(
        'dynamodb', 'http://{}:4569'.format(os.environ.get('LOCAL_AWS_HOST', 'localhost')),
        session.resource if session else boto3.resource
    )


def dynamodb_client(session: boto3.Session = None) -> boto3.session.Session.client:
    return boto3_thing(
        'dynamodb', 'http://{}:4569'.format(os.environ.get('LOCAL_AWS_HOST', 'localhost')),
        session.client if session else boto3.client
    )

def sts_client(session: boto3.Session = None) -> boto3.session.Session.client:
    return boto3_thing(
        'sts', 'http://{}:4566'.format(os.environ.get('LOCAL_AWS_HOST', 'localhost')),
        session.client if session else boto3.client
    )


def ddb_table(table_name: str, session: boto3.Session = None):
    return dynamodb(session).Table(table_name)


def sqs(session: boto3.Session = None):
    return boto3_thing(
        'sqs', 'http://{}:4576'.format(os.environ.get('LOCAL_AWS_HOST', 'localhost')),
        session.client if session else boto3.client
    )


def ssm_client(session: boto3.Session = None) -> boto3.client:
    return boto3_thing(
        'ssm', 'http://{}:4583'.format(os.environ.get('LOCAL_AWS_HOST', 'localhost')),
        session.client if session else boto3.client
    )


def secrets_client(session: boto3.Session = None) -> boto3.client:
    return boto3_thing(
        'secretsmanager', 'http://{}:4584'.format(os.environ.get('LOCAL_AWS_HOST', 'localhost')),
        session.client if session else boto3.client
    )


def ses_client(region_name: str, ses_proxy: str = None, session: boto3.Session = None) -> boto3.client:
    factory = session.client if session else boto3.client
    return factory('ses', region_name=region_name, config=Config(
        proxies={
            'https': f'http://{ses_proxy}:443'
        } if ses_proxy else {}
    ))


def lambda_client(session: boto3.Session = None) -> boto3.client:
    return boto3_thing(
        'lambda', 'http://{}:4574'.format(os.environ.get('LOCAL_AWS_HOST', 'localhost')),
        session.client if session else boto3.client
    )


def secret_value(name: str, session: boto3.Session = None) -> str:
    try:
        return secrets_client(session).get_secret_value(SecretId=name)["SecretString"]
    except UnknownServiceError:
        return get_secret_via_cli(name)


def secret_binary_value(name: str, session: boto3.Session = None) -> bytes:
    try:
        return secrets_client(session).get_secret_value(SecretId=name)["SecretBinary"]
    except UnknownServiceError:
        return get_secret_binary_via_cli(name)


def rds_client(session: boto3.Session = None) -> boto3.client:
    factory = session.client if session else boto3.client
    return factory('rds', region_name=AWS_REGION)


# TODO when we have latest boto remove the conditional has_latest_boto logic below
def _ssm_parameter(name: str, decrypt: bool = False) -> Union[str, List[str]]:
    """
    Internal helper function to perform the retrieve functionality for retrieving SSM parameters.
    Used by ssm_parameter and ssm_parameter_with_retry

    Args:
        name: The SSM parameter name to retrieve.
        decrypt: Whether the SSM parameter is SecureString and will thus need to be decrypted. Defaults to false.

    Returns:
        Union[str, List[str]]: The value of the SSM parameter requested as per the name argument.
    """
    ssm = ssm_client()
    has_latest_boto = hasattr(ssm, "get_parameter")

    if has_latest_boto:
        return ssm.get_parameter(Name=name, WithDecryption=decrypt)['Parameter']['Value']
    else:
        return get_ssm_parameter_via_cli(name, decrypt)


def ssm_parameter(name: str, decrypt: bool = False) -> Union[str, List[str]]:
    """
    Helper function to retrieve a given SSM parameter. This implementation does not feature any retry functionality.
    For a version of this function that has retry functionality in case of throttling with an exponential backoff time,
    see ssm_parameter_with_retry

    Args:
        name: The SSM parameter name to retrieve.
        decrypt: Whether the SSM parameter is SecureString and will thus need to be decrypted. Defaults to false.

    Returns:
        Union[str, List[str]]: The value of the SSM parameter requested as per the name argument.

    Raises:
        ValueError: A wrapper exception around the original ClientError raised by boto denoting the parameter that we
        failed to retrieve.
    """
    try:
        return _ssm_parameter(name, decrypt)
    except Exception as e:
        raise ValueError("Failed to retrieve parameter {}".format(name)) from e


def ssm_retry_predicate(err: Exception) -> bool:
    if not isinstance(err, ClientError):
        return False

    return err.response.get('Error', {}).get('Code', 'Unknown') == 'ThrottlingException'

def ssm_parameter_with_retry(name: str, decrypt: bool = False, num_retries: int = 5) -> Union[str, List[str]]:
    """
    Helper function to retrieve a given SSM parameter. Includes retry functionality with an exponential backoff in
    order to account for potential throttling exceptions thrown when retrieving a given SSM parameter.
    The total number of SSM calls increases linearly per the number of retries as per the max retry call per boto call.
    Effectively this means that the total number of SSM calls is (num_retries * 4)
    for i.e. a total of 20 SSM GetParameter calls for the default num_retries of 5.

    Args:
        name: The SSM parameter name to retrieve.
        decrypt: Whether the SSM parameter is SecureString and will thus need to be decrypted. Defaults to false.
        num_retries: The number of retry attempts after which we bail out. Defaults to 5 attempts.

    Returns:
        Union[str, List[str]]: The value of the SSM parameter requested as per the name argument.

    Raises:
        ClientError: The original exception raised by boto denoting the failure reason and any additional metadata
        associated with the failure.
    """

    @retry(max_retries=num_retries, exponential_backoff=True, should_retry_predicate=ssm_retry_predicate)
    def _retrieve_ssm_parameter() -> Union[str, List[str]]:
        return _ssm_parameter(name, decrypt)

    return _retrieve_ssm_parameter()


def get_ssm_parameter_via_cli(name: str, decrypt=False) -> Union[str, List[str], None]:
    endpoint = ''
    if local_mode():
        endpoint = '--endpoint http://localhost:4583'

    decryption_flag = "--with-decryption" if decrypt else "--no-with-decryption"
    cmd = 'aws {} --region eu-west-2 ssm get-parameter --name {} {}'.format(endpoint, name, decryption_flag)
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    output = p.stdout.read()
    try:
        response = json.loads(output.decode("utf-8"))
        return response["Parameter"]["Value"]
    except JSONDecodeError:
        return None


def set_ssm_parameter_via_cli(name: str, value: str, description="", overwrite=False, key_id=None):
    endpoint = ''
    if local_mode():
        endpoint = '--endpoint http://localhost:4583'

    key_flag = "--key-id {}".format(key_id) if key_id is not None else ""
    overwrite_flag = "--overwrite" if overwrite else "--no-overwrite"
    cmd = 'aws {} --region eu-west-2 ssm put-parameter --name {} ' \
          '--description "{}" --value "{}" --type String {} {}' \
        .format(endpoint, name, description, value, overwrite_flag, key_flag)
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, close_fds=True)
    output = p.stdout.read()
    response = json.loads(output.decode("utf-8"))
    return response["Version"]


def get_secret_via_cli(name: str):
    cmd = 'aws --region eu-west-2 secretsmanager get-secret-value --secret-id {}'.format(name)
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    output = p.stdout.read()
    response = json.loads(output.decode("utf-8"))
    return response["SecretString"]


def get_secret_binary_via_cli(name: str):
    cmd = 'aws --region eu-west-2 secretsmanager get-secret-value --secret-id {}'.format(name)
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    output = p.stdout.read()
    response = json.loads(output.decode("utf-8"))
    return response["SecretBinary"]


def s3_bucket(bucket: str, session: boto3.Session = None) -> boto3.session.Session.resource:
    return s3_resource(session=session).Bucket(bucket)


def s3_object(
        bucket_or_url: str, key: str = None, session: boto3.Session = None
) -> boto3.session.Session.resource:
    if key is not None:
        bucket = bucket_or_url
    else:
        url_parsed = parse.urlparse(bucket_or_url)
        bucket = url_parsed.netloc
        key = url_parsed.path.lstrip('/')

    return s3_resource(session).Object(bucket, key)


def s3_throttle_retry_predicate(err: Exception):
    if not isinstance(err, ClientError):
        return False

    return err.response.get('Error', {}).get('Code', 'Unknown') == 'SlowDown'


@retry(max_retries=5, exponential_backoff=True, should_retry_predicate=s3_throttle_retry_predicate)
def s3_get_all_keys(bucket: str, prefix: str, session: boto3.Session = None) -> List[str]:
    client = s3_resource(session).meta.client
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = []
    for page in page_iterator:
        for content in page['Contents']:
            keys.append(content['Key'])
    return keys


def s3_key_exists(s3_uri:str, session: boto3.Session = None) -> bool:
    try:
        _, bucket, key = s3_split_path(s3_uri)
        s3_get_all_keys(bucket, key, session)
        return True 
    except KeyError:
        return False


def s3_delete_keys(keys: Iterable[str], bucket: str, session: boto3.Session = None, should_retry: bool = False):
    bc = s3_bucket(bucket, session=session)

    all_keys = [key for key in keys]

    if not all_keys:
        return []

    def _s3_delete_retry_predicate(err: Exception):
        return should_retry and (s3_throttle_retry_predicate(err) or isinstance(err, ResponseParserError))

    @retry(max_retries=10, exponential_backoff=True, should_retry_predicate=_s3_delete_retry_predicate)
    def _s3_delete_objects(batch_keys):
        bc.delete_objects(Delete={'Objects': [{'Key': key} for key in batch_keys]})

    deleted = []

    batch = all_keys[:999]
    remaining = all_keys[999:]
    while batch:
        _s3_delete_objects(batch)
        deleted.extend(batch)
        batch = remaining[:999]
        remaining = remaining[999:]

    return deleted


def s3_delete_versioned_keys(keys: Iterable[Tuple[str, str]], bucket: str, session: boto3.Session = None):
    # delete specific versions, rather than deleting "objects" and adding delete_marker
    bc = s3_bucket(bucket, session=session)

    all_keys = list(keys)
    deleted = []

    # batch deleting can only handle certain sized batches
    batch = all_keys[:999]
    remaining = all_keys[999:]
    while batch:
        bc.delete_objects(
            Delete={'Objects': [
                {
                    'Key': key,
                    'VersionId': version_id
                } for key, version_id in batch
            ]}
        )
        deleted.extend(batch)
        batch = remaining[:999]
        remaining = remaining[999:]

    return deleted


def s3_delete_all_versions(
        bucket: str, prefix: str = None, predicate=None, dry_run: bool = True, session: boto3.Session = None
):
    s3 = s3_client(session)
    object_response_paginator = s3.get_paginator('list_object_versions')

    delete_marker_list = []
    version_list = []

    for object_response_itr in object_response_paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'DeleteMarkers' in object_response_itr:
            for delete_marker in object_response_itr['DeleteMarkers']:
                if predicate and not predicate(delete_marker['Key']):
                    continue
                delete_marker_list.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

        if 'Versions' in object_response_itr:
            for version in object_response_itr['Versions']:
                if predicate and not predicate(version['Key']):
                    continue
                version_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})

    if dry_run:
        print('dry run found:')
        print(delete_marker_list)
        print(version_list)
        return

    for i in range(0, len(delete_marker_list), 1000):
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': delete_marker_list[i:i + 1000],
                'Quiet': True
            }
        )
        print(response)

    for i in range(0, len(version_list), 1000):
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': version_list[i:i + 1000],
                'Quiet': True
            }
        )
        print(response)


@log_action(log_args=['extracts_bucket_name', 'extract_prefix'])
def s3_cleanup(extracts_bucket_name: str, extract_prefix: str, session: boto3.Session = None):
    """
    Delete any objects in an S3 bucket with the given prefix
    """
    keys = s3_get_all_keys(
        bucket=extracts_bucket_name,
        prefix=extract_prefix,
        session=session
    )
    s3_delete_keys(
        bucket=extracts_bucket_name, keys=keys, session=session
    )


def s3_ls(
        uri: str, recursive: bool = True, predicate: Callable[[str], bool] = None,
        versioning: bool = False, session: boto3.Session = None
):
    _, bucket, path = s3_split_path(uri)

    yield from s3_list_bucket(
        bucket, path, recursive=recursive, predicate=predicate,
        versioning=versioning, session=session
    )


def s3_list_bucket(
        bucket: str, prefix: str, recursive: bool = True, predicate: Callable[[str], bool] = None,
        versioning: bool = False, session: boto3.Session = None
) -> Generator[object, None, None]:
    """ list contents of S3 bucket based on filter criteria and versioning flag

    Args:
        bucket (str): bucket name to list contents of
        prefix (str): prefix to filter on
        recursive (bool): whether to recurse or not
        predicate (Callable[[str], bool]): predicate function to filter results on
        versioning (bool): whether to return objects or versions
        session (boto3.Session): optional existing session r

    Returns:
        Generator[object, None, None]: resulting objects or versions
    """
    bc = s3_bucket(bucket, session=session)
    bc_objects = bc.object_versions if versioning else bc.objects

    for s3_obj in bc_objects.filter(Prefix=prefix, Delimiter='' if recursive else '/'):
        if predicate and not predicate(s3_obj.key):
            continue
        yield s3_obj


def s3_list_prefixes(s3_path: str, session: boto3.Session = None) -> List[str]:
    _na, bucket, prefix = s3_split_path(s3_path)

    if not prefix.endswith('/'):
        prefix += '/'

    client = s3_client(session)
    paginator = client.get_paginator('list_objects')
    result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')

    if local_mode():
        return sorted({o['Key'].replace(prefix, '').split('/')[0] for o in result.search('Contents') if o})

    return [o['Prefix'].replace(prefix, '').strip('/') for o in result.search('CommonPrefixes') if o]


@retry(max_retries=5, exponential_backoff=True)
@log_action(log_args=['bucket_name', 'prefix', 'local_path', 'include', 'exclude'])
def s3_sync_to_local(bucket_name: str, prefix: str, local_path: str, include=None, exclude=None):
    _s3_sync_local(from_=f"s3://{bucket_name}/{prefix}", to=local_path, include=include, exclude=exclude)


@log_action(log_args=['bucket_name', 'prefix', 'local_path', 'include', 'exclude'])
def s3_sync_from_local(bucket_name: str, prefix: str, local_path: str, include=None, exclude=None):
    _s3_sync_local(from_=local_path, to=f"s3://{bucket_name}/{prefix}", include=include, exclude=exclude)


def _s3_sync_local(
        from_: str,
        to: str,
        exclude: Optional[Union[str, Iterable[str]]],
        include: Optional[Union[str, Iterable[str]]]
):
    assert isinstance(exclude, (str, list, type(None)))
    assert isinstance(include, (str, list, type(None)))

    exclude_list = [exclude] if isinstance(exclude, str) else exclude or []
    include_list = [include] if isinstance(include, str) else include or []
    exclude_ = ' '.join(f'--exclude "{e}"' for e in exclude_list)
    include_ = ' '.join(f'--include "{i}"' for i in include_list)
    endpoint = f"--endpoint-url=http://{os.environ.get('LOCAL_S3_HOST', 'localhost')}:8080" if local_mode() else ''

    try:
        subprocess.check_output(f'aws {endpoint} s3 sync "{from_}" "{to}" {exclude_} {include_}', shell=True, stderr=STDOUT)
    except subprocess.CalledProcessError as e:
        print(f"Subprocess output: {e.output}")
        raise e


@log_action(log_args=['s3_uri'])
def s3_uri_get_size(s3_uri: str, session: boto3.Session = None) -> int:
    """
    Get the size in bytes of the file(s) at/under the provided uri
    """
    _, bucket, key = s3_split_path(s3_uri)
    return s3_get_size(bucket, key, session)


@log_action(log_args=['bucket_name', 'prefix'])
def s3_get_size(bucket_name: str, prefix: str, session: boto3.Session = None) -> int:
    """
    Get the size in bytes of the file(s) at/under the provided prefix
    """
    return sum([s3_obj.size for s3_obj in s3_bucket(bucket_name, session=session).objects.filter(Prefix=prefix)])


def s3_object_last_modified(bucket: str, key: str) -> Union[datetime, None]:
    """
    Returns utc timestamp for a given s3 object.
    """
    _s3c = s3_client()

    try:
        return _s3c.head_object(Bucket=bucket, Key=key)["LastModified"]
    except ClientError:
        return None


@log_action(log_args=['source_url', 'destination_url'])
@retry(max_retries=5, exponential_backoff=True, should_retry_predicate=s3_throttle_retry_predicate)
def s3_copy_object(
        source_url: str, destination_url: str, version_id: str = None,
        extra_args: dict = None, session: boto3.Session = None
):
    src_scheme, src_bucket_name, src_key = s3_split_path(source_url)  # pylint:disable=unused-variable
    dest_scheme, dest_bucket_name, dest_key = s3_split_path(destination_url)  # pylint:disable=unused-variable

    copy_source = {
        'Bucket': src_bucket_name,
        'Key': src_key
    }
    if version_id is not None:
        copy_source['VersionId'] = version_id

    destination_buck = s3_bucket(dest_bucket_name, session=session)
    destination_buck.copy(copy_source, dest_key, ExtraArgs=extra_args or {})


@log_action(log_args=["source_file_uri", "destination_uri"])
def s3_move(
    source_file_uri: str,
    destination_uri: str,
    replace: Optional[bool] = False,
    version_id: Optional[str] = None,
    extra_args: Optional[dict] = None,
    session: Optional[boto3.Session] = None,
):
    """
    Moves an s3 object to another location within s3. As part of a standard 'move' operation,
    this will delete the original key on confirmation of a successful copy of the object.

    Args:
        source_file_uri (str): The location of the key that you want to move.
        destination_uri (str): The location of where you want to move the key to.
        replace (str): Allows for the replacement of an existing object in the destination_uri. Default to False.
        version_id (Optional[str], optional): Allows for versioning of s3 key. Defaults to None.
        extra_args (Optional[dict], optional): Extra arguments to be utilised by the client operation. Defaults to None.
        session (Optional[boto3.Session], optional): Client to be used for the operation. Defaults to None.

    Raises:
        IOError: Unable to find the key based on the `source_file_uri` provided.
        IOError: Unable to find the key in the new location.
    """
    if not s3_key_exists(source_file_uri, session):
        raise IOError(f"Move operation failed. Unable to find file `{source_file_uri}`.")

    dest_obj_exists = s3_key_exists(destination_uri, session)

    if not replace and dest_obj_exists:
        raise IOError(f"Move operation failed. Found duplicate key in `{destination_uri}`.")

    s3_copy_object(source_file_uri, destination_uri, version_id, extra_args, session)

    if s3_key_exists(destination_uri, session):
        _, bucket, source_key = s3_split_path(source_file_uri)
        s3_delete_keys([source_key], bucket, session, should_retry=True)
    else:
        raise IOError(f"Move operation failed. Unable to find `{destination_uri}`.")


def s3_get_temp_path(s3_path: str) -> str:
    """
    Args: An s3 path e.g 's3://local-testing/mesh/2'
    Returns: The corresponding s3_path for temp files e.g 's3://local-testing/temp/mesh/2'
    """
    scheme, bucket, key = s3_split_path(s3_path)
    temp_path = '{scheme}://{bucket}/{temp}{key}'.format(
        scheme=scheme, bucket=bucket, temp=_TEMP_LOCATION_KEY_PREFIX, key=key
    )
    return temp_path


def s3_is_temp_path(s3_uri: str) -> bool:
    """
    Determine whether a given S3 URI represents a temporary location

    Args:
        s3_uri: The URI to test

    Returns:
        Whether the URI appears to represent a temporary location
    """
    _, _, key = s3_split_path(s3_uri)
    return key.startswith(_TEMP_LOCATION_KEY_PREFIX)


def s3_upload_file(file_path: str, bucket: str, key: str, session: boto3.Session = None):
    """
    Upload a file to a given S3 location
    Args:
        file_path: Path to file to be uploaded
        bucket: Target bucket
        key: Target key
        session: boto3 session
    """
    client = s3_resource(session=session).meta.client
    client.upload_file(file_path, bucket, key)


def s3_upload_fileobj(data: object, bucket: str, key: str, session: boto3.Session = None):
    """
    Upload a file to a given S3 location
    Args:
        data: File-like object be uploaded
        bucket: Target bucket
        key: Target key
        session: boto3 session
    """
    client = s3_resource(session=session).meta.client
    client.upload_fileobj(data, bucket, key)


def s3_create_presigned_url(bucket: str, key: str, expiration: int = 3600, session: boto3.Session = None):
    """
    Creates a presigned url for a given S3 object
    Args:
        bucket: Target bucket
        key: Target key
        expiration: Time in seconds for the presigned URL to remain valid
        session: boto3 session

    Returns:
        Presigned URL as string
    """
    # why not s3_client helper ?
    client = s3_resource(session=session).meta.client
    params = {
        'Bucket': bucket,
        'Key': key
    }
    response = client.generate_presigned_url('get_object', Params=params, ExpiresIn=expiration)
    return response

@lru_cache(maxsize=1)
def get_aws_account_id() -> str:
    return ssm_parameter('/core/common/account_id')


def instance_profile_to_arn(instance_profile) -> str:
    if instance_profile.startswith("arn:aws:iam::"):
        return instance_profile

    return f"arn:aws:iam::{get_aws_account_id()}:instance-profile/{instance_profile}"


def service_config(config_section: str) -> Union[str, dict, list]:
    with io.StringIO(ssm_parameter_with_retry('/core/{}/config'.format(config_section))) as f:
        loaded = yaml.full_load(f)

    config = loaded.get('static_params', {})

    for param in loaded.get('ssm_params', []):
        config[param['name']] = ssm_parameter_with_retry(param['ssm_key'], param.get('secure', False))

    for param in loaded.get('secrets', []):
        config[param['name']] = secret_value(param['secret_id'])

    return config


def get_feature_toggles() -> Dict[str, bool]:
    return get_toggles()["feature"]


def get_toggle(toggle_type: str, name: str) -> bool:
    return strtobool(ssm_parameter_with_retry('/core/toggles/{}/{}'.format(toggle_type, name)))


def get_feature_toggle(feature: str) -> bool:
    return get_toggle('feature', feature)


_RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException', 'ThrottlingException')


def dynamodb_retry_backoff(max_retries=6):
    """ retry dynamodb actions with exponential backoff of 2 ^ retries

    Args:
        max_retries (int): maximum number of retries default 6 = 64 seconds

    """

    def _wrapping(f):
        """Nested decorator.

        Args:
            f (function) - the function to be wrapped
        Returns:
            decorated function
        """

        @wraps(f)
        def _wrapper(*args, **kwargs):

            retries = 0
            while True:

                try:

                    result = f(*args, **kwargs)

                    return result

                except ClientError as err:

                    if err.response['Error']['Code'] not in _RETRY_EXCEPTIONS:
                        raise

                    if retries > max_retries:
                        raise

                    retries += 1
                    sleep(pow(2, retries))

        return _wrapper

    return _wrapping


@log_action()
@retry(max_retries=5, exponential_backoff=True, should_retry_predicate=ssm_retry_predicate)
def get_toggles() -> Dict[str, Dict[str, bool]]:
    root = '/core/toggles/'
    paginator = ssm_client().get_paginator('get_parameters_by_path')
    pages = paginator.paginate(Path=root, Recursive=True)

    toggles = defaultdict(dict)  # type: Dict[str, Dict[str, bool]]

    for page in pages:
        for param in (page.get('Parameters') or []):
            toggle_type, name = param['Name'].split('/')[-2:]
            toggles[toggle_type][name] = strtobool(param['Value'])

    return toggles


@dynamodb_retry_backoff()
def get_items_batched(
        ddb_table_name: str, keys: List[Dict[str, Any]], session: boto3.Session = None
) -> Dict[str, Any]:
    ddb = dynamodb_client(session=session)
    response = ddb.batch_get_item(
        RequestItems={
            ddb_table_name: {
                'Keys': keys
            }
        }
    )
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    return response


def ddb_get_items(
        ddb_table_name: str, keys: List[Dict[str, Any]], session: boto3.Session = None
) -> List[Dict[str, Any]]:
    result = []
    remaining = keys
    while remaining:
        batch = remaining[:100]
        remaining = remaining[100:]
        response = get_items_batched(ddb_table_name, batch, session=session)
        if response.get('Responses') and response['Responses'].get(ddb_table_name):
            result.extend(response['Responses'][ddb_table_name])
        if response.get('UnprocessedKeys') and response['UnprocessedKeys'].get(ddb_table_name):
            remaining = response['UnprocessedKeys'][ddb_table_name]['Keys'] + remaining
    return result


def ddb_query_batch_get_items(
        key_field: str, session: boto3.Session = None, **kwargs
) -> Generator[dict, None, None]:
    table_name = kwargs["TableName"]

    paginator = dynamodb_client(session=session).get_paginator('query')

    page_iterator = paginator.paginate(**kwargs)

    deserializer = TypeDeserializer()

    for page in page_iterator:

        keys = [{key_field: rec[key_field]} for rec in page['Items']]
        recs = len(keys)
        items = ddb_get_items(table_name, keys)
        for item in items:
            yield {k: deserializer.deserialize(v) for k, v in item.items()}


def ddb_query_paginate(session: boto3.Session = None, **kwargs) -> Generator[dict, None, None]:
    paginator = dynamodb(session=session).meta.client.get_paginator('query')

    page_iterator = paginator.paginate(**kwargs)

    for page in page_iterator:
        yield from page['Items']


def ddb_query_paginated_count(session: boto3.Session = None, **kwargs) -> int:
    paginator = dynamodb(session=session).meta.client.get_paginator('query')

    kwargs = {**kwargs, 'Select': 'COUNT'}

    page_iterator = paginator.paginate(**kwargs)

    return reduce(lambda acc, page: acc + page['Count'], page_iterator, 0)


@log_action(log_args=['source_url', 'destination_url'])
def s3_gunzip(source_url: str, destination_url: str, session: boto3.Session = None, buffer_size: int = 1024 * 1024 * 5):
    """
    Unzip a file on S3 to another S3 location. Streams the file and unzips it with local resources, and
    streams the bytes to the new location. Does not use local disk and minimally uses local memory.
    The size of the write buffer is controlled through the buffer_size parameter and is 5MiB by default, a larger
    buffer size is required for larger files of 50GiB uncompressed or more to avoid the 10,000 parts error
    """
    _, src_bucket_name, src_key = s3_split_path(source_url)  # pylint:disable=unused-variable
    _, dest_bucket_name, dest_key = s3_split_path(destination_url)  # pylint:disable=unused-variable

    s3obj = s3_object(src_bucket_name, src_key, session=session)
    down_stream = s3obj.get()['Body']

    s3obj = s3_bucket(dest_bucket_name, session=session).put_object(Key=dest_key)
    if not s3obj:
        raise Exception(f'Unknown S3 error when putting empty buffer to {destination_url}') from UnknownServiceError

    with gzip.GzipFile(mode='rb', fileobj=down_stream) as f:
        with S3ObjectBuffer(s3obj, buffer_size=buffer_size, writable=True, encoding=None) as new_obj_buff:
            buf = True
            while buf:
                buf = f.read(io.DEFAULT_BUFFER_SIZE * 1000)
                if buf:
                    new_obj_buff.write(buf)


def assumed_credentials(account_id: str, role: str, role_session_name: str = None, duration_seconds: int = 1200):
    """
    Refreshes the IAM credentials provided to us by STS for the duration of our session. This callback is invoked
    automatically by boto when we are past the lifetime of our session (see DurationSeconds in params).

    Returns:
         dict -> A dictionary containing our new set of credentials from STS as well as the expiration timestamp
         for the session.
    """
    region = 'eu-west-2'

    role_session_name = role_session_name or f"assumed-{uuid4().hex}"

    sts_client = boto3.client(
        'sts', region_name=region, endpoint_url=f'https://sts.{region}.amazonaws.com'
    )

    params = {
        "RoleArn": f"arn:aws:iam::{account_id}:role/{role}",
        "RoleSessionName": role_session_name,
        "DurationSeconds": duration_seconds,
    }

    response = sts_client.assume_role(**params).get("Credentials")

    credentials = {
        "access_key": response.get("AccessKeyId"),
        "secret_key": response.get("SecretAccessKey"),
        "token": response.get("SessionToken"),
        "expiry_time": response.get("Expiration").isoformat(),
    }
    return credentials


@log_action()
def assumed_role_session(
        account_id: str, role: str, role_session_name: str = None, duration_seconds: int = 1200,
        refreshable: bool = False
) -> boto3.session.Session:
    """
    gets assumed role session for given account id and role name
    :return: Boto session for assumed role
    """

    get_credentials = partial(
        assumed_credentials, account_id, role,
        role_session_name=role_session_name,
        duration_seconds=duration_seconds
    )

    credentials = get_credentials()
    if not refreshable:

        boto_credentials = botocore.credentials.Credentials(
            access_key=credentials['access_key'],
            secret_key=credentials['secret_key'],
            token=credentials['token'],
            method="sts-assume-role",
        )
    else:
        boto_credentials = botocore.credentials.RefreshableCredentials.create_from_metadata(
            metadata=credentials,
            refresh_using=get_credentials,
            method="sts-assume-role",
        )

    session = botocore.session.get_session()
    session._credentials = boto_credentials

    return boto3.session.Session(botocore_session=session)


def get_latest_s3_version_id(s3_path: str) -> str:
    """ Get the latest version id for S3 object path

    Args:
        s3_path (str): path of S3 object

    Returns:
        str: version ID
    """

    [version_id] = [version.version_id for version in s3_ls(s3_path, versioning=True) if version.is_latest]
    return version_id


@log_action(log_args=['bucket', 'key', 'tag_key', 'tag_value'])
def tag_s3_object(bucket: str, key: str, tag_key: str, tag_value):
    object_tags = s3_client().get_object_tagging(
        Bucket=bucket,
        Key=key
    ).get('TagSet', [])

    for tag in object_tags:
        if tag['Key'] == tag_key:
            tag['Value'] = tag_value
            break
    else:
        object_tags.append({'Key': tag_key, 'Value': tag_value})

    s3_client().put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={'TagSet': object_tags}
    )


@log_action()
def parallel_s3_copy_objects(sources_and_destinations, delete_after=False):
    def copy_file(source: str, destination: str, child_connection: connection.Connection):
        try:
            s3_copy_object(source, destination)
            child_connection.send([(source, destination, "success")])
            if delete_after:
                s3_delete_keys([s3_split_path(source)[2]], s3_split_path(source)[1], should_retry=True)
        except Exception as e:
            child_connection.send([(source, destination, f"error: {e}")])
        finally:
            child_connection.close()

    parent_conns = []
    processes = []

    for source_url, destination_url in sources_and_destinations:
        parent_conn, child_conn = Pipe()
        process = Process(target=copy_file, args=(source_url, destination_url, child_conn))
        parent_conns.append(parent_conn)
        processes.append(process)
        process.start()

    results = [conn.recv()[0] for conn in parent_conns]
    for process in processes:
        process.join()

    success = [(src, dest) for src, dest, status in results if status == "success"]
    errors = [(src, dest, error) for src, dest, error in results if "error" in error]

    add_fields(files_copied=success, files_failed_to_copy=errors)

    if errors:
        raise IOError(f"Copy operation failed: {errors}")

