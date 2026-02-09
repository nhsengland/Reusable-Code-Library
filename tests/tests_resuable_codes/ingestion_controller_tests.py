from datetime import datetime
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession, Row

from nhs_reusable_code_library.resuable_codes.file_store import File
from nhs_reusable_code_library.resuable_codes.ingestion_controller import IngestionController

CONTROL_TABLE = "test_table_ingestion_control"

from nhs_reusable_code_library.resuable_codes.shared import local_path

warehouse_location = local_path('testdata/test_helpers_test/')

@pytest.fixture
def spark():
    spark = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
    temp_db = "temp_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {temp_db}")
    yield spark



IngestionController._create_control_table = Mock()


@pytest.fixture()
def controller(spark: SparkSession) -> IngestionController:
    temp_db = "temp_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {temp_db}")
    _ctrl = IngestionController(spark, temp_db, 0, CONTROL_TABLE)
    _ctrl._spark = Mock()
    _ctrl._get_next_file_id = Mock(side_effect=[0, 1])
    _ctrl.list_failed_files = Mock(return_value=[])
    return _ctrl


# Note we use a mix of forward and back-slashes in the key to test escaping.
file = File(
    "my_bucket",
    "me_the_user",
    "me_the_user/path/to\\file\\my_file.baz",
    {"LastModified": datetime(2021, 5, 1, 7, 56, 23), "ETag": "v1.5"},
)
file_2 = File(
    "my_bucket",
    "me_the_user",
    "me_the_user/another/path/to\\file\\my_file.baz",
    {"LastModified": datetime(2021, 6, 9, 12, 29, 23), "ETag": "v1.5"},
)


def test_successful_file_ingestion(controller):
    # ACT
    file_id = controller.start_file_ingestion(file)

    # ASSERT
    assert file_id == 0

    # ACT
    controller.set_file_ingestion_complete(file_id)

    # ASSERT
    controller.verify_ingestion_status()


def test_file_ingestion_error(spark: SparkSession):
    # ACT
    temp_db = "temp_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {temp_db}")
    tolerant_ingestion_controller = IngestionController(
        spark, temp_db, 1, CONTROL_TABLE
    )
    tolerant_ingestion_controller._spark = Mock()
    tolerant_ingestion_controller._get_next_file_id = Mock(side_effect=[0, 1])
    tolerant_ingestion_controller._update_file_status = Mock()
    tolerant_ingestion_controller.list_failed_files = Mock(
        side_effect=[
            [Row(file_id=0, file_path="a", file_version="0")],
            [
                Row(file_id=0, file_path="a", file_version="0"),
                Row(file_id=1, file_path="b", file_version="1"),
            ],
        ]
    )

    file_id_1 = tolerant_ingestion_controller.start_file_ingestion(file)
    file_id_2 = tolerant_ingestion_controller.start_file_ingestion(file_2)

    # ASSERT
    assert file_id_1 == 0
    assert file_id_2 == 1

    tolerant_ingestion_controller._spark.sql.assert_called()
    assert tolerant_ingestion_controller._spark.sql.call_count == 2

    tolerant_ingestion_controller._spark.reset_mock()

    # ACT
    # Note we include the file path to test escaping.
    error_msg = f'It\'s an error for file {file.relative_path}, with "double quotes"'

    # Tolerant controller should not raise an exception after the first error.
    tolerant_ingestion_controller.set_file_ingestion_error(file_id_1, error_msg)
    tolerant_ingestion_controller._update_file_status.assert_called_once()

    # After the second error, controller should raise an exception.
    with pytest.raises(Exception) as context:
        tolerant_ingestion_controller.set_file_ingestion_error(file_id_2, error_msg)

    assert tolerant_ingestion_controller._update_file_status.call_count == 2
    assert tolerant_ingestion_controller.list_failed_files.call_count == 2
    # ASSERT
    expected_exception = (
        "Cannot proceed with file ingestion."
        " Unrecoverable errors exist for file ids: [0, 1]."
        " Please resolve these manually before retrying."
    )
    assert expected_exception == str(context.value), context.value
