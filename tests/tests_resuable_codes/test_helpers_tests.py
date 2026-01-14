import os
from typing import List

import pytest
from collections import OrderedDict
from mock import Mock
import shutil
import gzip

# noinspection PyUnresolvedReferences
from shared import local_path
from shared.common import test_helpers
from shared.common.test_helpers import smart_csv_reader, smart_upload, scenario_input_path, smart_open, smart_copy, \
    gzip_file_copy
from shared.constants import DS, FT, PATHS


@pytest.fixture()
def column_headers() -> List[str]:
    return ["column {}".format(col_no) for col_no in range(18)]


@pytest.fixture()
def scenario_path() -> str:
    return scenario_input_path(DS.DIDS, FT.CSV, '00002_example_500_4_dq_issues')


@pytest.fixture()
def gzip_scenario_path() -> str:
    return local_path('testdata/mps/csv/mps_test_data/correct.csv.gz')


@pytest.fixture()
def expected_dict() -> OrderedDict:
    return OrderedDict({
        "column 0": "9201403968",
        "column 1": "",
        "column 2": "2001-08-02",
        "column 3": "J",
        "column 4": "02",
        "column 5": "WN5 8QX",
        "column 6": "J81002",
        "column 7": "04",
        "column 8": "G0613369",
        "column 9": "J81002",
        "column 10": "2018-07-30",
        "column 11": "2018-07-30",
        "column 12": "2018-07-30",
        "column 13": "",
        "column 14": "417905003",
        "column 15": "2018-07-30",
        "column 16": "RR864",
        "column 17": "J810020494027921202"
    })


@pytest.fixture()
def expected_row() -> str:
    return "9201403968,,2001-08-02,J,2,WN5 8QX,J81002,04,G0613369,J81002,2018-07-30,2018-05-01,2018-05-02,," \
           "417905003,2018-05-30,RR864,J810020494027921202"


def test_s3_upload_read(s3_temp, column_headers, scenario_path, expected_dict):

    bucket, s3_path = s3_temp

    smart_upload(scenario_path, os.path.join(s3_path, PATHS.RAW_DATA))

    with smart_csv_reader(os.path.join(s3_path, PATHS.RAW_DATA), column_headers) as f:
        for line in f:
            lines = line
            break

    assert lines == expected_dict


def test_local_read(column_headers, scenario_path, expected_dict):

    with smart_csv_reader(scenario_path, column_headers) as f:
        for line in f:
            lines = line
            break

    assert lines == expected_dict


def test_s3_gzip_read(s3_temp, gzip_scenario_path):
    _, s3_path = s3_temp

    smart_upload(gzip_scenario_path, os.path.join(s3_path, PATHS.RAW_DATA))

    with smart_open(os.path.join(s3_path, PATHS.RAW_DATA), 'rb', zipped=True) as f:
        for line in f:
            lines = line
            break

    assert lines == b'MPTREQ_20190301000000,DSP,1,00\n'


@pytest.mark.parametrize("version_number", [0, 1])
def test_get_version_with_id(version_number, s3_temp, gzip_scenario_path):
    bucket, s3_path = s3_temp
    s3_key = os.path.join(s3_path, PATHS.RAW_DATA)

    if version_number == 1:
        smart_upload(local_path('testdata/gp_data_patient/xml/00006_invalid_filename/input_invalid_file.xml'), s3_key)
        smart_upload(gzip_scenario_path, s3_key)
    else:
        smart_upload(gzip_scenario_path, s3_key)
        smart_upload(local_path('testdata/gp_data_patient/xml/00006_invalid_filename/input_invalid_file.xml'), s3_key)

    versions = bucket.object_versions.filter(Prefix=s3_key)
    versions = sorted(versions, key=lambda version_object: version_object.last_modified)
    assert len(versions) == 2

    with smart_open(s3_key, 'rb', zipped=True, version_id=versions[version_number].version_id) as f:
        for line in f:
            lines = line
            break

    assert lines == b'MPTREQ_20190301000000,DSP,1,00\n'


def test_local_gzip_read(gzip_scenario_path):
    lines = []

    with smart_open(gzip_scenario_path, zipped=True) as f:
        for line in f:
            lines.append(line)

    assert lines[0] == b'MPTREQ_20190301000000,DSP,1,00\n'


def test_s3_write(s3_temp):
    _, s3_path = s3_temp
    path = os.path.join(s3_path, 'testing.txt')

    with smart_open(path, 'wt') as f:
        f.write('Hello world')

    with smart_open(path, 'rt') as f:
        assert f.read() == 'Hello world'


def test_write(temp_dir):
    with smart_open(os.path.join(temp_dir, 'testing.txt'), 'wt') as f:
        f.write('Hello world')

    with smart_open(os.path.join(temp_dir, 'testing.txt'), 'rt') as f:
        assert f.read() == 'Hello world'


def test_s3_gzip_write(s3_temp):
    _, s3_path = s3_temp
    path = os.path.join(s3_path, 'testing.txt.gz')

    with pytest.raises(ValueError) as e:
        with smart_open(path, 'wt', zipped=True) as f:
            f.write('Hello world')
            assert e


def test_gzip_write(temp_dir):
    with pytest.raises(ValueError) as e:
        with smart_open(os.path.join(temp_dir, 'testing.txt.gz'), 'wt', zipped=True) as f:
            f.write('Hello world')
            assert e


@pytest.mark.parametrize("file_path, new_file, expected", [
    ('s3://nhsd-dspp-core-dev-ops/temp/550093.txt', 's3://nhsd-dspp-core-dev-ops/temp/487557.txt', 's3_copy_object'),
    ('s3://nhsd-dspp-core-dev-ops/temp/524957.txt', '/tmp/956373.txt', 'smart_download'),
    ('{temp_dir}/775079.txt', 's3://nhsd-dspp-core-dev-ops/temp/934844.txt', 'smart_upload'),
    ('{temp_dir}/744059.txt', '/tmp/187981.txt', 'shutil.copy'),
    ('435461', '829713', '435461'),
])
def test_smart_copy(file_path, new_file, expected, temp_dir):
    if 'temp_dir' in file_path:
        file_path = file_path.format(temp_dir=temp_dir)
        open(file_path, 'a').close()
    test_helpers.s3_copy_object = Mock(side_effect=Exception('s3_copy_object'))
    test_helpers.smart_upload = Mock(side_effect=Exception('smart_upload'))
    test_helpers.smart_download = Mock(side_effect=Exception('smart_download'))
    shutil.copy = Mock(rside_effect=Exception('shutil.copy'))
    try:
        smart_copy(file_path, new_file)
    except Exception as e:
        assert str(e) == expected


def test_gzip_file_copy(temp_dir: str):
    file_name = temp_dir+'file.txt'
    nfile_name = temp_dir+'file.txt.gz'
    file_contents = b'temp content'
    with open(file_name, 'wb') as f:
        f.write(file_contents)
    gzip_file_copy(file_name, nfile_name)
    with gzip.open(nfile_name, 'rb') as f:
        assert f.read() == file_contents
