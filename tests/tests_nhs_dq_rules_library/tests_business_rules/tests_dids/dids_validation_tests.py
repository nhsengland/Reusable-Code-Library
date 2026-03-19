import datetime
import os

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ByteType, LongType, IntegerType
import dsp.validations.validator
from dsp import pipelines
from nhs_dq_rules_library.business_rules.dids import before_local_pds_cohort
from dsp.pipeline.models import PipelineContext
from dsp.validations.validator import match_schema, match_row_count, validate_all_records_present, \
    compare_column_values, validate_all_records_present_by_hash
from dsp.shared.common.test_helpers import smart_upload, basic_metadata, scenario_input_path, \
    scenario_expected_path, scenario_expected_dq_path
from dsp.shared.constants import PATHS, DS, FT
from dsp.shared import local_path

@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


test_data_1 = [('123456', 1, 'LS14HR'),
               ('987654', 2, 'LS14HR'),
               ('999888', 2, 'LS14HR')]

test_data_2 = [('123456', 1, 'LS14HR'),
               ('987654', 2, 'LS14HR'),
               ('999888', 2, 'LS14HR')]

test_data_3 = [('123456', 1, 'LS14HR'),
               ('987654', 2, 'LS14HR')]

test_data_4 = [('123456', 1, 'LS14HR'),
               ('987654', 2, 'LS14HR'),
               ('999999', 2, 'LS14HR')]

test_data_5 = [('123456', 1, 'LS14HR'),
               ('987654', 2, 'LS14HR'),
               ('999888', 2, 'LS101LG')]

test_data_6 = [('123456', 1, 'LS14HR'),
               ('987654', 2, 'LS14HR'),
               ('999888', 2, 'LS14HR'),
               ('999888', 2, 'LS14HR')]

test_data_nested_1 = [('123456', 1, 'LS14HR', (1, 1)),
                      ('987654', 2, 'LS14HR', (1, 2)),
                      ('999888', 2, 'LS14HR', (1, 3))]

test_data_nested_2 = [('123456', 1, 'LS14HR', (2, 1)),
                      ('987654', 2, 'LS14HR', (2, 2)),
                      ('999888', 2, 'LS14HR', (2, 3))]

schema_1 = StructType([
    StructField("NHS_NUMBER", StringType(), True),
    StructField("NHS_NUMBER_TYPE", ByteType(), True),
    StructField("POSTCODE", StringType(), True)
])

schema_2 = StructType([
    StructField("NHS_NUMBER", StringType(), True),
    StructField("NHS_NUMBER_TYPE", LongType(), True),
    StructField("POSTCODE", StringType(), True)
])

schema_nested = StructType([
    StructField("NHS_NUMBER", StringType(), True),
    StructField("NHS_NUMBER_TYPE", ByteType(), True),
    StructField("POSTCODE", StringType(), True),
    StructField("META", StructType([
        StructField("SUBMISSION_ID", StringType(), True),
        StructField("RECORD_INDEX", IntegerType(), True)]))
])


@pytest.fixture()
def test_data_1_schema_1_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_1, schema_1)

    return df


@pytest.fixture()
def test_data_2_schema_1_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_2, schema_1)

    return df


@pytest.fixture()
def test_data_2_schema_2_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_2, schema_2)

    return df


@pytest.fixture()
def test_data_3_schema_1_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_3, schema_1)

    return df


@pytest.fixture()
def test_data_4_schema_1_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_4, schema_1)

    return df


@pytest.fixture()
def test_data_5_schema_1_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_5, schema_1)

    return df


@pytest.fixture()
def test_data_6_schema_1_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_6, schema_1)

    return df


@pytest.fixture()
def test_data_nested_1_schema_nested_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_nested_1, schema_nested)

    return df


@pytest.fixture()
def test_data_nested_2_schema_nested_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(test_data_nested_2, schema_nested)

    return df


def test_match_schema(test_data_1_schema_1_df: DataFrame, test_data_2_schema_1_df: DataFrame) -> None:
    passed, messages = match_schema(test_data_1_schema_1_df, test_data_2_schema_1_df, [])
    assert passed, '\n'.join(messages)


def test_mismatch_schema(test_data_1_schema_1_df: DataFrame, test_data_2_schema_2_df: DataFrame) -> None:
    passed, messages = match_schema(test_data_1_schema_1_df, test_data_2_schema_2_df, [])
    assert not passed, '\n'.join(messages)


def test_match_row_count(test_data_1_schema_1_df: DataFrame, test_data_2_schema_1_df: DataFrame) -> None:
    passed, messages = match_row_count(test_data_1_schema_1_df, test_data_2_schema_1_df)
    assert passed, '\n'.join(messages)


def test_mismatch_row_count(test_data_1_schema_1_df: DataFrame, test_data_3_schema_1_df: DataFrame) -> None:
    passed, messages = match_row_count(test_data_1_schema_1_df, test_data_3_schema_1_df)
    assert not passed, '\n'.join(messages)


def test_validate_all_records_present(test_data_1_schema_1_df: DataFrame, test_data_2_schema_1_df: DataFrame) -> None:
    passed, messages = validate_all_records_present(test_data_1_schema_1_df, test_data_2_schema_1_df, ['NHS_NUMBER'],
                                                    "Dummy")
    assert passed, '\n'.join(messages)


def test_validate_not_all_records_present(test_data_1_schema_1_df: DataFrame,
                                          test_data_4_schema_1_df: DataFrame) -> None:
    passed, messages = validate_all_records_present(test_data_1_schema_1_df, test_data_4_schema_1_df, ['NHS_NUMBER'],
                                                    "Dummy")
    assert not passed, '\n'.join(messages)


def test_validate_all_column_value_match(test_data_1_schema_1_df: DataFrame,
                                         test_data_2_schema_1_df: DataFrame) -> None:
    passed, messages = compare_column_values(
        test_data_1_schema_1_df, test_data_2_schema_1_df, ['NHS_NUMBER'], [], [], [])
    assert passed, '\n'.join(messages)


def test_validate_all_column_value_mismatch(test_data_1_schema_1_df: DataFrame,
                                            test_data_5_schema_1_df: DataFrame) -> None:
    passed, messages = compare_column_values(
        test_data_1_schema_1_df, test_data_5_schema_1_df, ['NHS_NUMBER'], [], [], [])
    assert not passed, '\n'.join(messages)


def test_validate_all_column_value_mismatch_ignored(test_data_1_schema_1_df: DataFrame,
                                                    test_data_5_schema_1_df: DataFrame) -> None:
    passed, messages = compare_column_values(
        test_data_1_schema_1_df, test_data_5_schema_1_df, ['NHS_NUMBER'], [], ['LS14HR'], [])
    assert passed, '\n'.join(messages)


def test_validate_all_column_value_mismatch_not_ignored(test_data_1_schema_1_df: DataFrame,
                                                        test_data_5_schema_1_df: DataFrame) -> None:
    passed, messages = compare_column_values(
        test_data_1_schema_1_df, test_data_5_schema_1_df, ['NHS_NUMBER'], [], ['LS101LG'], [])
    assert not passed, '\n'.join(messages)


def test_validate_nested_column_mismatch_ignored(test_data_nested_1_schema_nested_df: DataFrame,
                                                 test_data_nested_2_schema_nested_df: DataFrame) -> None:
    passed, messages = compare_column_values(test_data_nested_1_schema_nested_df, test_data_nested_2_schema_nested_df,
                                             ['NHS_NUMBER'], [], [], ["META.SUBMISSION_ID"])
    assert passed, '\n'.join(messages)


def test_validate_nested_column_mismatch_not_ignored(test_data_nested_1_schema_nested_df: DataFrame,
                                                     test_data_nested_2_schema_nested_df: DataFrame) -> None:
    passed, messages = compare_column_values(test_data_nested_1_schema_nested_df, test_data_nested_2_schema_nested_df,
                                             ['NHS_NUMBER'], [], [], ["META.RECORD_INDEX"])
    assert not passed, '\n'.join(messages)



@pytest.fixture()
def temp_dir() -> str:
    return local_path('testdata/dids_test_data/dids')

# #to fix later -04-03-2026

# def test_xml_validate(spark, temp_dir):
#     scenario = '00002_simple'
#     dataset_id = DS.DIDS
#     file_type = FT.XML

#     metadata = basic_metadata(
#         working_folder=temp_dir,
#         dataset_id=dataset_id,
#         file_type=file_type,
#         submitted_timestamp=datetime.datetime(2018, 8, 1)
#     )
#     metadata['sender_id'] = 'RR8'
#     raw = os.path.join(temp_dir, PATHS.RAW_DATA)
#     actual = os.path.join(temp_dir, PATHS.OUT, PATHS.OUT)
#     expected = scenario_expected_path(dataset_id, file_type, scenario)

#     smart_upload(scenario_input_path(dataset_id, file_type, scenario), os.path.join(temp_dir, raw))

#     before_local_pds_cohort(spark)
#     pipeline_context = PipelineContext.from_metadata(metadata)
#     pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

#     assert dsp.validation.validator.validate_expected_result(spark, actual, expected, metadata)


# def test_csv_validate(spark, temp_dir):
#     scenario = '00003_simple'
#     dataset_id = DS.DIDS
#     file_type = FT.CSV

#     metadata = basic_metadata(
#         working_folder=temp_dir,
#         dataset_id=dataset_id,
#         file_type=file_type,
#         submitted_timestamp=datetime.datetime(2018, 8, 1)
#     )
#     metadata['sender_id'] = 'RR8'
#     raw = os.path.join(temp_dir, PATHS.RAW_DATA)
#     actual = os.path.join(temp_dir, PATHS.OUT, PATHS.OUT)
#     expected = scenario_expected_path(dataset_id, file_type, scenario)

#     smart_upload(scenario_input_path(dataset_id, file_type, scenario), os.path.join(temp_dir, raw))
#     before_local_pds_cohort(spark)
#     pipeline_context = PipelineContext.from_metadata(metadata)
#     pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

#     assert dsp.validation.validator.validate_expected_result(spark, actual, expected, metadata)


# def test_csv_validate_dq_errs(spark: object, temp_dir: object) -> object:
#     scenario = '00004_dq_errors'
#     dataset_id = DS.DIDS
#     file_type = FT.CSV

#     metadata = basic_metadata(
#         working_folder=temp_dir,
#         dataset_id=dataset_id,
#         file_type=file_type,
#         submitted_timestamp=datetime.datetime(2018, 8, 1)
#     )

#     raw = os.path.join(temp_dir, PATHS.RAW_DATA)
#     actual = os.path.join(temp_dir, PATHS.DQ)
#     dq_expected = scenario_expected_dq_path(dataset_id, file_type, scenario)

#     smart_upload(scenario_input_path(dataset_id, file_type, scenario), os.path.join(temp_dir, raw))

#     pipeline_context = PipelineContext.from_metadata(metadata)
#     pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

#     assert dsp.validation.validator.validate_dq_result(spark, actual, dq_expected, metadata)


def test_validation_by_hash(test_data_1_schema_1_df):
    result, messages = validate_all_records_present_by_hash(test_data_1_schema_1_df, test_data_1_schema_1_df, "Dummy")
    assert result


def test_validation_by_hash_schema_mismatch(test_data_1_schema_1_df, test_data_2_schema_2_df):
    result, messages = validate_all_records_present_by_hash(test_data_1_schema_1_df, test_data_2_schema_2_df, "Dummy")
    assert not result


def test_validation_by_hash_column_value_mismatch(test_data_1_schema_1_df, test_data_5_schema_1_df):
    result, messages = validate_all_records_present_by_hash(test_data_1_schema_1_df, test_data_5_schema_1_df, "Dummy")
    assert not result


def test_validation_by_hash_row_count_mismatch(test_data_1_schema_1_df, test_data_3_schema_1_df):
    result, messages = validate_all_records_present_by_hash(test_data_1_schema_1_df, test_data_3_schema_1_df, "Dummy")
    assert not result


def test_validation_by_hash_not_all_records_present(test_data_1_schema_1_df, test_data_4_schema_1_df):
    result, messages = validate_all_records_present_by_hash(test_data_1_schema_1_df, test_data_4_schema_1_df, "Dummy")
    assert not result


def test_validation_by_hash_invalid_repeated_row(test_data_1_schema_1_df, test_data_6_schema_1_df):
    result, messages = validate_all_records_present_by_hash(test_data_1_schema_1_df, test_data_6_schema_1_df, "Dummy")
    assert not result
