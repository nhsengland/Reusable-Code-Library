import pytest

from nhs_reusable_code_library.resuable_codes.csv_parser import CsvParser


def test_extracts_table_name_from_snapshot_path() -> None:
    file_path = "dbo/69fddda4-c55f-4c7a-8cbf-0746d6c31bea/__RefactorLog/LOAD00000001.parquet"

    actual = ParquetParser.get_dms_table_name(file_path)

    assert "refactorlog" == actual


def test_extracts_table_name_when_no_prefix() -> None:
    file_path = "dbo/__RefactorLog/LOAD00000001.parquet"

    actual = ParquetParser.get_dms_table_name(file_path)

    assert "refactorlog" == actual


def test_throws_error_when_unexpected_path() -> None:
    # Not enough directories in path.
    file_path = "you_are_not_going_to_like_this/LOAD00000001.parquet"

    with pytest.raises(Exception) as excinfo:
        ParquetParser.get_dms_table_name(file_path)

    assert f"Unexpected DMS parquet file path: {file_path}" == str(excinfo.value)
