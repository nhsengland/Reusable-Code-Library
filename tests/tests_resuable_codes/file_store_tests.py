from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from src.nhs_reusable_code_library.resuable_codes.file_store import File, FileStore

buckets = {
    "bucket_1": ["user_A", "user_B"],
    "bucket_2": ["user_C"],
}


def test_exposes_file_data():
    file = File(
        "bucket_1",
        "user_A",
        "user_A/dummies\\dummy1",
        {
            "LastModified": datetime(2021, 3, 4, 11, 12, 13),
            "ETag": '"0d730dcc81d0e5e248d17e87706f45c7"',
        },
    )

    assert file.absolute_path == "s3://bucket_1/user_A/dummies\\dummy1"
    assert file.relative_path == "dummies\\dummy1"
    assert file.last_modified == datetime(2021, 3, 4, 11, 12, 13)
    assert file.version == "0d730dcc81d0e5e248d17e87706f45c7"
    assert file.bucket == "bucket_1"
    assert file.key == "user_A/dummies\\dummy1"


dummy_object_1 = {
    "Key": "user_A/dummies\\dummy1",
    "Size": 42,
    "LastModified": datetime(2021, 3, 4, 11, 12, 13),
    "ETag": '"0d730dcc81d0e5e248d17e87706f45c7"',
}

dummy_object_2 = {
    "Key": "user_C/dummies\\dummy2",
    "Size": 35,
    "LastModified": datetime(2021, 3, 3, 13, 14, 15),
    "ETag": '"f8d287f7f3b791a0e9e30053712b00e6"',
}

mock_object_1 = {
    "Key": "user_A/mocks/mock1",
    "Size": 20,
    "LastModified": datetime(2021, 3, 2, 10, 11, 12),
    "ETag": '"c3e9fbc0250f2d59283d87f909c7348c"',
}

mock_object_2 = {
    "Key": "user_A/mocks/mock2",
    "Size": 8,
    "LastModified": datetime(2021, 3, 2, 9, 11, 12),
    "ETag": '"2a54e20b2ce15100bc87c62c4eb49445"',
}

mock_directory_1 = {
    "Key": "user_A/mocks/",
    "Size": 0,
    "LastModified": datetime(2021, 3, 2, 9, 11, 12),
    "ETag": '"6h54e20b2ce15100bc44c62c4eb49496"',
}

object_pages = {
    "bucket_1": {
        "user_A": [
            {"Contents": [dummy_object_1, mock_object_1]},
            {"Contents": [mock_directory_1, mock_object_2]},
        ],
        "user_B": [{}],
    },
    "bucket_2": {"user_C": [{"Contents": [dummy_object_2]}]},
}


def test_lists_files_from_all_buckets():
    file_store = FileStore(buckets)
    file_store._get_object_pages = Mock(wraps=lambda bucket, user: object_pages[bucket][user])

    # ACT
    files = file_store.list_files()

    # ASSERT
    expected_files = [
        File("bucket_1", "user_A", dummy_object_1["Key"], dummy_object_1),
        File("bucket_1", "user_A", mock_object_1["Key"], mock_object_1),
        File("bucket_1", "user_A", mock_object_2["Key"], mock_object_2),
        File("bucket_2", "user_C", dummy_object_2["Key"], dummy_object_2),
    ]

    assert len(files) == len(expected_files)

    for i, file in enumerate(files):
        assert vars(file) == vars(expected_files[i])


objects = {
    "bucket_2": {
        "user_C/dummies\\dummy2": {
            "LastModified": datetime(2021, 3, 4, 11, 12, 13),
            "ETag": '"0d730dcc81d0e5e248d17e87706f45c7"',
        }
    },
}


@pytest.fixture()
def file_store() -> FileStore:

    file_store = FileStore(buckets)
    file_store._get_object_metadata = Mock(wraps=lambda bucket, key: objects[bucket][key])
    return file_store


def test_gets_a_file(file_store):
    # ACT
    file = file_store.get_file("s3://bucket_2/user_C/dummies\\dummy2")

    # ASSERT
    assert vars(file) == vars(
        File(
            "bucket_2",
            "user_C",
            "user_C/dummies\\dummy2",
            objects["bucket_2"]["user_C/dummies\\dummy2"],
        )
    )


def test_raises_an_error_for_an_unavailable_bucket(file_store):
    # ACT
    with pytest.raises(Exception) as context:
        file_store.get_file("s3://bucket_invalid/user_C/dummies\\dummy2")

    # ASSERT
    expected_exception = (
        f"Attempting to access file"
        f" 's3://bucket_invalid/user_C/dummies\\dummy2' for"
        f" unavailable bucket 'bucket_invalid'."
    )
    assert expected_exception == str(context.value)


def test_raises_an_error_for_an_unavailable_user(file_store):
    # ACT
    with pytest.raises(Exception) as context:
        file_store.get_file("s3://bucket_2/user_A/dummies\\dummy2")

    # ASSERT
    expected_exception = (
        f"Attempting to access file"
        f" 's3://bucket_2/user_A/dummies\\dummy2' for"
        f" unavailable user 'user_A' for bucket 'bucket_2'."
    )
    assert expected_exception in str(context.value)


def test_raises_an_error_for_an_invalid_path(file_store):
    # ACT
    with pytest.raises(Exception) as context:
        file_store.get_file("s3://bucket_2\\user_A")

    # ASSERT
    expected_exception = "Cannot not process file path 's3://bucket_2\\user_A'."
    assert expected_exception in str(context.value)


def test_raises_an_error_for_an_unsupported_protocol(file_store):
    # ACT
    with pytest.raises(Exception) as context:
        file_store.get_file("http://bucket_2/user_A/dummies\\dummy2")

    # ASSERT
    expected_exception = "Cannot not process file path 'http://bucket_2/user_A/dummies\\dummy2'."
    assert expected_exception in str(context.value)
