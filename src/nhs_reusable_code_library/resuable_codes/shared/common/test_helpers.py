import csv
import glob
import gzip
import io
import json
import os
import re
import shutil
import time
from contextlib import contextmanager
from datetime import datetime, date
from random import randint
from tempfile import TemporaryDirectory, TemporaryFile
from typing import Callable, Generator, List, Iterable, Union, Pattern, IO

from src.nhs_reusable_code_library.resuable_codes.shared import aws
from src.nhs_reusable_code_library.resuable_codes.shared import local_path
from src.nhs_reusable_code_library.resuable_codes.shared.aws import s3_bucket, s3_split_path, s3_object, s3_delete_keys, s3_copy_object
from src.nhs_reusable_code_library.resuable_codes.shared.common import retry
from src.nhs_reusable_code_library.resuable_codes.shared.common.datasets import namespace_and_dataset
from src.nhs_reusable_code_library.resuable_codes.shared.common.retry_predicates import s3_throttle_retry_predicate
from src.nhs_reusable_code_library.resuable_codes.shared.common.s3_object_buffer import S3ObjectBuffer
from src.nhs_reusable_code_library.resuable_codes.shared.constants import METADATA, DS, DATASET_ID_VERSION_MAP
from src.nhs_reusable_code_library.resuable_codes.shared.models import MeshMetadata
from src.nhs_reusable_code_library.resuable_codes.shared.store.mesh_download_queues import MeshDownloadQueuesStore
from src.nhs_reusable_code_library.resuable_codes.shared.version_data import FULL_VERSION_STRING

LOCAL_TESTING_BUCKET = 'local-testing'

_EXPECTED_FILE_NAME = {
    DS.CSDS_GENERIC: {
        'xml': 'expected.json',
        'accdb': 'expected.json'
    },
    DS.CSDS_V1_6: {
        'xml': 'expected.json',
        'accdb': 'expected.json'
    },
    DS.CQUIN: {
        'xlsx': 'expected.csv',
    },
    DS.DIDS: {
        'csv': 'expected.csv',
        'xml': 'expected.csv',
        'json': 'expected.json',
    },
     DS.IAPT_GENERIC: {
        'accdb': 'expected.json',
        'xml': 'full.xml'
    },
    DS.IAPT_V2_1: {
        'accdb': 'expected.json',
        'xml': 'full.xml'
    },
    DS.MHSDS_V6: {
        'accdb': 'expected.json',
    },
    DS.MHSDS_GENERIC: {
        'accdb': 'expected.json',
    },
    DS.MSDS: {
        'accdb': 'expected.json',
        'xml': 'expected.json',
    },
    DS.AHAS: {
        'jsonl': 'expected.jsonl',
    },
    DS.PCAREMEDS: {
        'csv': 'expected.json',
    },
    DS.DEID: {
        'json': 'expected.json',
        'csv': 'expected.csv'
    },
    DS.DAE_DATA_IN: {
        'csv': 'expected.csv'
    },
    DS.DMS_UPLOAD: {
        'xlsx': 'expected.csv',
        'csv': 'expected.csv',
    },
    DS.GP_DATA_APPOINTMENT: {
        'xml': 'expected.json',
    },
    DS.GP_DATA_PATIENT: {
        'xml': 'expected.json'
    },
    # Used only for snapshot feature test
    'snapshot': {
        'csv': 'expected.csv',
        'parquet': 'expected.parquet',
        'json': 'expected.json',
    },
    DS.LPOS: {
        'zip': 'input.zip'
    },
    DS.MESH_TO_POS: {
        'csv': 'input.csv'
    },
    DS.CPOS: {
        'zip': 'input.zip'
    },
    DS.POS_CANCEL_DISSEMINATION: {
        'csv': 'input.csv'
    },
    DS.POS_DISSEMINATION: {
        'csv': 'input.csv'
    },
    DS.RANDOX: {
        'csv': 'input.csv'
    },
    DS.DUMMY_PIPELINE: {
        'csv': 'input.csv'
    },
    DS.EPMAWSPC: {
        'csv': 'expected.json'
    },
    DS.EPMAWSAD: {
        'csv': 'expected.json'
    },
    DS.EPMAWSPC2: {
        'csv': 'expected.json'
    },
    DS.EPMAWSAD2: {
        'csv': 'expected.json'
    },
    DS.NPEX: {
        'csv': 'input.csv'
    },
    DS.GENERIC: {
        'csv': 'expected.csv',
        'xml': 'expected.json',
        'jsonl': 'expected.json',
        'parquet': 'expected.json'
    },
    DS.P2C: {
        'parquet': 'expected.parquet'
    },
    DS.GDPPR: {
        'xml': 'expected.json'
    },
    DS.NDRS_RDC: {
        'parquet': 'input.parquet',
        'zip': 'input.zip'
    },
    DS.PHE_CANCER: {
        'parquet': 'input.parquet',
        'zip': 'input.zip'
    },
    DS.OXI_HOME: {
        'xlsm': 'input.xlsm'
    },
    DS.GRAIL_KCL: {
        "csv": "expected.csv"
    },
    DS.MYS: {
        'csv': 'expected.csv'
    },
    DS.DIGITRIALS_DCM: {
        'csv': 'output.csv',
        'txt': 'output.txt',
        'parquet': 'output.csv',
        'zip': 'expected.csv'
    },
    DS.DIGITRIALS_OUT: {
        'csv': 'output.csv',
        'txt': 'output.txt',
        'parquet': 'input.parquet',
        'zip': 'expected.csv'
    },
    DS.DIGITRIALS_COMS: {
        'csv': 'output.csv'
    },
    DS.DIGITRIALS_REC: {
        'csv': 'output.csv'
    },
    DS.DIGITRIALS_BMC: {
        'csv': 'output.csv'
    },
    DS.EPMANATIONALPRES: {
        'xml': 'expected.json'
    },
    DS.EPMANATIONALADM: {
        'xml': 'expected.json'
    },
    DS.NDRS_GERMLINE: {
        'parquet': 'expected.json',
        'zip': 'expected.json'
    }
}

_EXTRACTS_EXPECTED_FILE_NAME = {
    'json': 'expected.json',
    'jsonl': 'expected.json',
    'xml': 'expected.xml',
    'csv': 'expected.csv',
}
DQ_XML = 'dq.xml'
INPUT_CSV = 'input.csv'
INPUT_XML = 'input.xml'


def mock_type(thing_to_mock: Union[type, callable], method_or_attribute: str = None, in_module: str = None) -> str:
    module = in_module or thing_to_mock.__module__
    parts = [module, thing_to_mock.__name__]
    if method_or_attribute:
        parts.append(method_or_attribute)
    return '.'.join(parts)


def smart_list_files(root_dir: str, predicate: Union[Callable[[str], bool], None] = None) -> Iterable[str]:
    if not root_dir.lower().startswith('s3'):

        for f in os.listdir(root_dir):

            if predicate and not predicate(f):
                continue
            yield f

        return

    _, bucket_name, root_folder = s3_split_path(root_dir)

    bucket = s3_bucket(bucket_name)

    for obj in bucket.objects.all():

        key = obj.key
        if predicate and not predicate(key):
            continue

        yield key.replace(root_folder, '').strip('/')


@contextmanager
def smart_open(uri: str, mode: str = 'rb', zipped=False, version_id: str = None, encoding: str = 'utf-8') -> Generator[
        IO, None, None]:
    """
    Return a file handle for a path which may be situated on local disk or on s3

    Args:
        uri: The path to the file to open, either a local path or an S3 URI
        mode: The mode in which to open the file
        zipped: Whether the file is expected to be gzip compressed
        version_id: the required version of the file
        encoding: character encoding for the file (if text)

    Yields:
        A handle for the file at the requested URI
    """
    if zipped and 'w' in mode:
        raise ValueError('zipped open does not support writing')

    encoding = None if 'b' in mode else encoding
    if not uri.lower().startswith('s3'):
        f = gzip.open(uri, mode) if zipped else open(uri, mode, encoding=encoding)

        yield f

        f.close()

        return

    _, bucket_name, key = s3_split_path(uri)
    bucket = s3_bucket(bucket_name)
    encoding = None if 'b' in mode else encoding
    obj = bucket.Object(key=key)

    if version_id is not None and obj.version_id != version_id:
        obj = obj.Version(version_id)

    f = S3ObjectBuffer(obj=obj, writable=not mode.startswith('r'), encoding=encoding)

    if zipped:
        f = gzip.open(f, mode)

    yield f

    f.close()


def mps_csv_reader(temp_dir):
    mps_dir = os.path.join(temp_dir, 'mps_extract')

    mps_dir.startswith('s3')

    mps_file = next(f for f in smart_list_files(mps_dir, lambda f: f.endswith('.csv')))
    if not mps_file:
        raise IOError('no mps file found under {}'.format(mps_dir))

    mps_csv_path = os.path.join(mps_dir, mps_file)

    return smart_csv_reader(mps_csv_path)


@contextmanager
def smart_csv_reader(csv_path: str, fieldnames: List[str] = None) -> Generator[csv.DictReader, None, None]:
    with smart_open(csv_path, 'r') as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)

        yield reader


@retry(max_retries=5, exponential_backoff=True, should_retry_predicate=s3_throttle_retry_predicate)
def s3_keys(bucket, prefix, max_keys=None):
    if max_keys:
        s3_objs = bucket.objects.filter(Prefix=prefix, MaxKeys=max_keys).limit(max_keys)
    else:
        s3_objs = bucket.objects.filter(Prefix=prefix)

    for s3_obj in s3_objs:
        yield s3_obj.key


def s3_exists(bucket, key):
    for _ in s3_keys(bucket, prefix=key, max_keys=1):
        return True

    return False


def smart_exists(uri):
    if not uri.lower().startswith('s3'):
        return os.path.exists(uri)

    _, bucket_name, key = s3_split_path(uri)
    bucket = s3_bucket(bucket_name)

    return s3_exists(bucket, key)


def smart_ls(uri: str, suffix: str = None) -> List[str]:
    if not uri.lower().startswith('s3'):
        if not suffix:
            return [os.path.join(uri, f) for f in os.listdir(uri)]

        return glob.glob(os.path.join(uri, '*' + suffix))

    _, bucket_name, prefix = s3_split_path(uri)

    return [
        os.path.join('s3://', bucket_name, k)
        for k in s3_keys(s3_bucket(bucket_name), prefix)
        if not suffix or k.endswith(suffix)
    ]


def smart_listdir(path: str) -> Generator[str, None, None]:
    if not path.lower().startswith('s3'):
        yield from os.listdir(path)
        return

    _, bucket_name, prefix = s3_split_path(path)

    pattern = re.compile(r"{}\/(?P<child>[^\/]+)(?:.*|\Z)".format(re.escape(prefix)))  # type: Pattern

    files = set()
    for key in s3_keys(s3_bucket(bucket_name), prefix):
        key_match = pattern.match(key)
        if key_match:
            file = key_match.group('child')
            if file not in files:
                yield file
                files.add(file)


def smart_delete(paths: List[str]) -> List[str]:
    deleted = []
    s3_paths = {}
    for path in paths:
        if not path.lower().startswith('s3'):
            os.remove(path)
            deleted.append(path)
        else:
            _, bucket_name, key = s3_split_path(path)
            if bucket_name in s3_paths:
                s3_paths[bucket_name].append(key)
            else:
                s3_paths[bucket_name] = [key]

    for bucket_name, keys in s3_paths.items():
        deleted.extend(s3_delete_keys(bucket=bucket_name, keys=keys))
    return deleted


def smart_upload(path, uri):
    if not os.path.exists(path):
        raise IOError(path)

    if not uri.lower().startswith('s3'):
        os.makedirs(os.path.dirname(uri), exist_ok=True)
        if os.path.isfile(path):
            shutil.copy(path, uri)
        else:
            shutil.copytree(path, uri)
        return

    _, bucket_name, key = s3_split_path(uri)

    bucket = s3_bucket(bucket_name)
    bucket.upload_file(path, key)


def smart_download(uri, path):
    if not uri.lower().startswith('s3'):
        shutil.copy(uri, path)
        return

    s3_object(uri).download_file(path)


def wait_until(check: Callable, tick: int = None, timeout: int = None) -> None:
    start = datetime.now()
    while True:
        if timeout is not None and (datetime.now() - start).total_seconds() >= timeout:
            raise TimeoutError('Condition not satisfied in {} seconds'.format(timeout))
        if check():
            return
        if tick is not None:
            time.sleep(tick)


def minimal_metadata(working_folder, dataset_id, file_type, **kwargs):
    minimal = {
        'submission_id': kwargs.get('submission_id', randint(1, 12)),
        'sender_id': 'TEST',
        'working_folder': working_folder,
        'dataset_id': dataset_id,
        'file_type': file_type,
        'submitted_timestamp': datetime.utcnow(),
        **kwargs
    }
    return minimal


def basic_metadata_dids(working_folder, dataset_id, file_type, **kwargs):
    if 'submitted_timestamp' not in kwargs:
        kwargs['submitted_timestamp'] = '201809010000000000'
    minimal = minimal_metadata(working_folder, dataset_id, file_type, **kwargs)
    if METADATA.REQUEST not in minimal:
        minimal[METADATA.REQUEST] = {
            METADATA.DATASET_ID: dataset_id,
            METADATA.SENDER_ID: 'DMS',
            METADATA.EXTRA: {'submitter_org_id': 'TEST'
            }
        }
    return minimal


def basic_metadata_cquin(working_folder, dataset_id, file_type, **kwargs):
    assert 'filename' in kwargs
    return minimal_metadata(working_folder, dataset_id, file_type, **kwargs)


def basic_metadata_iapt_v2_1(working_folder, dataset_id, file_type, **kwargs):
    minimal = {
        'submission_id': kwargs.get('submission_id', randint(1, 12)),
        'sender_id': 'RBA1234',
        'submitted_timestamp': datetime(2019, 12, 31, 10, 59, 59),
        'working_folder': working_folder,
        'dataset_id': dataset_id,
        'file_type': file_type,
        'request': {
            'extra': {
                'provider_id': 'RR',
                'reporting_period_start': '2019-04-01T00:00:00',
                'reporting_period_end': '2019-04-30T00:00:00',
            }
        },
        **kwargs
    }
    return minimal


def basic_metadata_csds_v1_6(working_folder, dataset_id, file_type, **kwargs):
    minimal = {
        'submission_id': kwargs.get('submission_id', randint(1, 12)),
        'sender_id': 'RBA1234',
        'submitted_timestamp': datetime(2011, 1, 1, 10, 59, 59),
        'working_folder': working_folder,
        'dataset_id': dataset_id,
        'file_type': file_type,
        'request': {
            'extra': {
                'provider_id': 'RR',
                'reporting_period_start': '2015-11-01T00:00:00',
                'reporting_period_end': '2015-11-30T00:00:00',
            }
        },
        **kwargs
    }
    return minimal


def basic_metadata_mhsds(working_folder, dataset_id, file_type, **kwargs):
    minimal = {
        'submission_id': kwargs.get('submission_id', randint(1, 12)),
        'sender_id': 'RAL',
        'submitted_timestamp': datetime(2018, 7, 1, 10, 59, 59),
        'working_folder': working_folder,
        'dataset_id': dataset_id,
        'file_type': file_type,
        'request': {
            'extra': {
                'provider_id': 'RAL',
                'reporting_period_start': '2015-05-01T00:00:00',
                'reporting_period_end': '2015-05-30T00:00:00',
            }
        },
        **kwargs
    }
    return minimal


def basic_metadata_mhsds_v6(working_folder, dataset_id, file_type, **kwargs):
    minimal = {
        'submission_id': kwargs.get('submission_id', randint(1, 12)),
        'sender_id': 'F81072',
        'submitted_timestamp': datetime(2018, 6, 1, 4, 00, 00),
        'working_folder': working_folder,
        'dataset_id': dataset_id,
        'file_type': file_type,
        'request': {
            'extra': {
                'provider_id': 'RA4',
                'reporting_period_start': '2018-05-01T00:00:00',
                'reporting_period_end': '2018-05-31T00:00:00',
            }
        },
        **kwargs
    }
    return minimal


def basic_metadata_msds(working_folder, dataset_id, file_type, **kwargs):
    minimal = {
        'submission_id': kwargs.get('submission_id', randint(1, 12)),
        'sender_id': 'F81072',
        'submitted_timestamp': datetime(2019, 5, 1, 0, 00, 00),
        'working_folder': working_folder,
        'dataset_id': dataset_id,
        'file_type': file_type,
        'request': {
            'extra': {
                'provider_id': 'RA4',
                'reporting_period_start': '2000-05-01T00:00:00',
                'reporting_period_end': '2019-05-01T00:00:00',
            }
        },
        **kwargs
    }
    return minimal


_metadata_gen = {
    DS.CQUIN: basic_metadata_cquin,
    DS.DIDS: basic_metadata_dids,
    DS.PLICS: minimal_metadata,
    DS.IAPT_V2_1: basic_metadata_iapt_v2_1,
    DS.CSDS_V1_6: basic_metadata_csds_v1_6,
    DS.MHSDS_V6: basic_metadata_mhsds_v6,
    DS.MSDS: basic_metadata_msds
}


def basic_metadata(working_folder, dataset_id, file_type, **kwargs):
    if _metadata_gen.get(dataset_id):
        return _metadata_gen[dataset_id](working_folder, dataset_id, file_type, **kwargs)

    return minimal_metadata(working_folder, dataset_id, file_type, **kwargs)


@contextmanager
def tempdir_scenario_path(dataset_id: str, file_type: str, scenario: str) -> Generator[str, None, None]:
    """
    Args:
        dataset_id (str): dataset id, e.g dids plics etc
        file_type (str): csv / xml etc.
        scenario (str): scenario folder name .. e.g. 00001_example_csv

    Yields:
        str: formatted test path in a temporary directory
    """
    with TemporaryDirectory(prefix='tempdir_scenario_path_') as temp_directory:
        source_path = scenario_path(True, dataset_id, file_type, scenario)
        yield shutil.copytree(source_path, os.path.join(temp_directory, os.path.basename(source_path)))


def scenario_path(local: bool = True, *args) -> str:
    """
    Args:
        args (str): list of path parts
        local (bool): make the path a local path .. default True

    Returns:
        str: formatted test path
    """

    path = os.path.join('testdata', *args)
    if not local:
        return path

    return local_path(path)


def scenario_input_path(dataset_id: str, file_type: str, scenario: str, dataset_version: str = None, local: bool = True,
                        file_name: str = 'input') -> str:
    """

    Args:
        dataset_id (str): dataset id, e.g dids plics etc
        file_type (str): csv / xml etc.
        scenario (str): scenario folder name .. e.g. 00001_example_csv
        dataset_version (str): dataset version e.g 2.0 , 2.1 etc if relevant
        local (bool): make the path a local path .. default True
        file_name (str): file name without extension .. default 'input'

    Returns:
        str: formatted test path
    """
    return os.path.join(
        scenario_path(local, *filter(None, namespace_and_dataset(
            DATASET_ID_VERSION_MAP.get((dataset_id, dataset_version)) or dataset_id)), file_type, scenario),
        '{}.{}'.format(file_name, file_type)
    )


def extract_scenario_input_path(
        dataset_id: str, extract_type: str, scenario: str, file_type: str, local: bool = True,
        file_name: str = 'input') -> str:
    """

    Args:
        file_name: file name without extension .. default 'input'
        dataset_id (str): dataset id, e.g dids plics etc
        extract_type (str): validation_summary ... etc .
        file_type (str): expected input file type
        scenario (str): scenario folder name .. e.g. 00001_example_csv
        local (bool): make the path a local path .. default True

    Returns:
        str: formatted test path
    """
    return os.path.join(
        scenario_path(local, 'extracts', dataset_id, extract_type, scenario), f'{file_name}.{file_type}')


def extract_scenario_expected_path(dataset_id: str,
                                   expected_directory: str,
                                   scenario: str,
                                   file_type: str,
                                   local: bool = True,
                                   result_type: str = None,
                                   extra_path: str = None) -> str:
    """

    Args:
        dataset_id (str): dataset id, e.g dids plics etc
        expected_directory (str): validation_summary ... etc .
        file_type (str): expected output file type
        scenario (str): scenario folder name .. e.g. 00001_example_csv
        local (bool): make the path a local path .. default True
        result_type (str): Filename of expected file.
        extra_path (str): Extra path between the scenario and expected file name.

    Returns:
        str: formatted test path
    """
    file_name = "{}.{}".format(result_type, file_type) if result_type else _EXTRACTS_EXPECTED_FILE_NAME[file_type]
    extra_path = extra_path or ''
    return scenario_path(local, 'extracts', dataset_id, expected_directory, scenario, extra_path, file_name)


def extract_expected_path(dataset_id: str, extract_type: str, scenario: str,
                          local: bool = True,
                          extra_path: str = None) -> str:
    """
    Args:
        dataset_id (str): dataset id, ex. IAPT, CSDS
        extract_type (str): validation_summary ... etc .
        scenario (str): scenario folder name .. e.g. 00001_simple
        local (bool): make the path a local path .. default True
        extra_path (str): Extra path between the scenario and expected file name.

    Returns:
        str: formatted test path
    """
    extra_path = extra_path or ''
    return scenario_path(local, 'extracts', dataset_id, extract_type, scenario, extra_path)


def scenario_expected_path(dataset_id: str, file_type: str, scenario: str, local: bool = True) -> str:
    """

    Args:
        dataset_id (str): dataset id, e.g dids plics etc
        file_type (str): csv / xml / accdb etc.
        scenario (str): scenario folder name .. e.g. 00001_example_csv
        local (bool): make the path a local path .. default True

    Returns:
        str: formatted test path
    """
    return os.path.join(scenario_path(local, dataset_id, file_type, scenario),
                        _EXPECTED_FILE_NAME[dataset_id][file_type])


def scenario_expected_dq_path(dataset_id: str, file_type: str, scenario: str, local: bool = True) -> str:
    """

    Args:
        dataset_id (str): dataset id, e.g dids plics etc
        file_type (str): csv / xml etc.
        scenario (str): scenario folder name .. e.g. 00001_example_csv
        local (bool): make the path a local path .. default True

    Returns:
        str: formatted test path
    """

    return os.path.join(scenario_path(local, dataset_id, file_type, scenario), DQ_XML)


def scenario_expected_mps_request_path(dataset_id: str, file_type: str, scenario: str, local: bool = True) -> str:
    """

    Args:
        dataset_id (str): dataset id, e.g dids plics etc
        file_type (str): csv / xml / accdb etc.
        scenario (str): scenario folder name .. e.g. 00001_example_csv
        local (bool): make the path a local path .. default True

    Returns:
        str: formatted test path
    """
    return os.path.join(scenario_path(local, dataset_id, file_type, scenario), 'mps', 'request.csv')


def scenario_expected_mps_responses_path(dataset_id: str, file_type: str, scenario: str, local: bool = True) -> List[
        str]:
    """

    Args:
        dataset_id (str): dataset id, e.g dids plics etc
        file_type (str): csv / xml / accdb etc.
        scenario (str): scenario folder name .. e.g. 00001_example_csv
        local (bool): make the path a local path .. default True

    Returns:
        List[str]: formatted test paths

    """
    response_folder = os.path.join(scenario_path(local, dataset_id, file_type, scenario), 'mps', 'response')
    if os.path.exists(response_folder):
        return sorted([os.path.join(response_folder, f) for f in os.listdir(response_folder)])
    return []


def dict_to_escaped_string(d: dict) -> str:
    """
    Convert a dictionary into an escaped string that
    can safely be passed to a notebook text widget.
    """
    return json.dumps(d).replace('"', '\\"')


def get_notebook_databricks_path(notebook_name: str) -> str:
    """
    Finds the current released version of the named notebook
    Args:
        notebook_name: the notebook name (and any subpath it is on)

    Returns:
        Path from root on databricks the notebook is at. Can be used to submit a run.
    """
    release = os.environ['RELEASE']
    if release is None or release == "":
        release = FULL_VERSION_STRING
    return "/Users/admin/releases/{}/{}".format(release, notebook_name)


def upload_s3_test_message(bucket: str, key: str, message: bytes):
    response_output = io.BytesIO()
    response_output.write(message)
    response_output.seek(0)
    aws.s3_object(bucket, key).upload_fileobj(response_output)


def create_job_and_mark_ready(store: MeshDownloadQueuesStore, message_id: str, workflow_id: str, s3_bucket: str,
                              s3_key: str, sender: str, message: bytes = b"", upload_to_s3: bool = False):
    mesh_metadata = MeshMetadata(
        dict(
            filename='myfile.txt',
            subject='mysubject'
        )
    )
    transfer_id = store.create_mesh_download_job(
        workflow_id=workflow_id,
        message_id=message_id,
        s3_location='s3://{}/{}'.format(s3_bucket, s3_key),
        mesh_metadata=mesh_metadata,
        mailbox_to='to',
        mailbox_from=sender
    )
    store.mark_preparing(transfer_id)
    store.mark_ready(transfer_id)

    if upload_to_s3:
        upload_s3_test_message(s3_bucket, s3_key, message)
    return store.get_by_transfer_id(transfer_id)


@contextmanager
def prepare_test_file(file_path: str, compress: bool, mode: str = 'rb') -> Generator[IO, None, None]:
    """ Compress the file into a temporary file if required, otherwise just open the file

    Args:
        file_path (str): path to the test file
        compress (bool): whether or not to gzip compress the file

    Yields:
        A handle for the prepared test file
    """
    with smart_open(file_path, mode) as data:
        if compress:
            with TemporaryFile(mode="w+b") as compressed_data:
                with gzip.GzipFile(fileobj=compressed_data, mode='wb') as gzip_file:
                    for chunk in data:
                        gzip_file.write(chunk)

                compressed_data.seek(0)
                yield compressed_data
        else:
            yield data


def smart_copy(file_path: str, new_path: str):
    """
    Copy a local file to the new location or if it's an S3 file copy it to new S3 location. If one is an S3 file
    and the other is local then it copies the file to the correct location.

    Args:
        file_path: Local or S3 location of file to copy.
        new_path: Local or S3 location of copied file.

    """
    file_on_s3 = file_path.lower().startswith('s3')
    dest_on_s3 = new_path.lower().startswith('s3')
    local_file = os.path.exists(file_path)
    local_dest = not dest_on_s3

    func = None
    if file_on_s3:
        if dest_on_s3:
            func = s3_copy_object
        elif local_dest:
            func = smart_download
    elif local_file:
        if dest_on_s3:
            func = smart_upload
        elif local_dest:
            func = shutil.copy
    else:
        raise IOError(file_path)

    func(file_path, new_path)


def gzip_file_copy(input_file_name: str,
                   gzipped_output_name: str) -> None:
    """
    Read the contents of data_input_file_name, writes a gzipped copy
    to gzipped_output_name.

    Does not check files exist before reading/writing.

    Args:
        input_file_name: Path to input file
        gzipped_output_name: Path to gzipped output file
    """
    with open(input_file_name, "rb") as f_in:
        with gzip.open(gzipped_output_name, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def transform_by_python_type(obj) -> str:
    """
    Transforms a given Python object into its textual representation in a format that reflects the strings included in
    BDD tests.

    Args:
        obj: The object to convert to a string
    Returns:
        Any: The BDD-compliant textual representation of the object if cast-able,
        reference to the original object otherwise.
    """
    if isinstance(obj, datetime) or isinstance(obj, date):
        return obj.isoformat()
    if obj is None:
        return "null"
    if isinstance(obj, int):
        return str(obj)

    return obj


def file_compare_in_memory_any_order(file1_path: str, file2_path: str):
    with smart_open(file1_path, "r", encoding="utf-8") as f:
        lines_1 = [l.strip() for l in f.readlines()]

    with smart_open(file2_path, "r", encoding="utf-8") as f:
        lines_2 = [l.strip() for l in f.readlines()]

    assert len(lines_1) == len(
        lines_2
    ), f"Line count mismatch:\nFile 1: {len(lines_1)}\n File 2: {len(lines_2)}"

    assert set(lines_1) == set(lines_2), (
        f"Lines in first file not in second:\n{set(lines_1) - set(lines_2)}\n"
        f"Lines in second file not in first:\n{set(lines_2) - set(lines_1)}"
    )
