from os.path import join
from shutil import copy

from pyspark.sql import SparkSession

from dsp.pipeline.stages.loader import CSVLoaderPipelineStage
from dsp.pipeline.test_helpers import run_single_stage
from shared import local_path
from shared.constants import PATHS
from shared.models import PipelineStatus
from shared.common.test_helpers import gzip_file_copy, smart_copy


def test_csv_loader_pipeline_stage_wrong_number_of_columns(spark: SparkSession, temp_dir: str):
    copy(local_path("testdata", "_abstract", "00001_csv", "input.csv"), join(temp_dir, PATHS.RAW_DATA))

    expected_columns = ["COLUMN_1", "COLUMN_2", "COLUMN_3"]
    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": temp_dir},
        {},
        output_dataframe_name="df",
        expected_cols=expected_columns,
    )

    assert stage_result.status == PipelineStatus.Rejected


def test_csv_loader_pipeline_stage_incorrect_column_names(spark: SparkSession, temp_dir: str):
    copy(local_path("testdata", "_abstract", "00001_csv", "input.csv"), join(temp_dir, PATHS.RAW_DATA))

    expected_columns = ["COLUMN_ONE", "COLUMN_TWO"]
    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": temp_dir},
        {},
        output_dataframe_name="df",
        expected_cols=expected_columns,
    )

    assert stage_result.status == PipelineStatus.Rejected


def test_csv_loader_pipeline_stage_correct_columns(spark: SparkSession, temp_dir: str):
    copy(local_path("testdata", "_abstract", "00001_csv", "input.csv"), join(temp_dir, PATHS.RAW_DATA))

    expected_columns = ["COLUMN_1", "COLUMN_2"]
    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": temp_dir},
        {},
        output_dataframe_name="df",
        expected_cols=expected_columns,
    )

    assert stage_result.status == PipelineStatus.Success

    df = context.dataframes['df'].df

    assert df.count() == 1

    row = df.collect()[0]
    assert row['COLUMN_1'] == 'one'
    assert row['COLUMN_2'] == 'two'
    assert row['RowNumber'] == 0


def test_csv_loader_pipeline_stage_no_header(spark: SparkSession, temp_dir: str):
    copy(local_path("testdata", "_abstract", "00002_csv_no_header", "input.csv"), join(temp_dir, PATHS.RAW_DATA))

    expected_columns = ["COLUMN_1", "COLUMN_2"]
    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": temp_dir},
        {},
        output_dataframe_name="df",
        expected_cols=expected_columns,
        has_header=False
    )

    assert stage_result.status == PipelineStatus.Success

    df = context.dataframes['df'].df

    assert df.count() == 1

    row = df.collect()[0]
    assert row['COLUMN_1'] == 'one'
    assert row['COLUMN_2'] == 'two'
    assert row['RowNumber'] == 0


def test_csv_loader_pipeline_stage_no_headers_no_expected_columns(spark: SparkSession, temp_dir: str):
    copy(local_path("testdata", "_abstract", "00002_csv_no_header", "input.csv"), join(temp_dir, PATHS.RAW_DATA))

    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": temp_dir},
        {},
        output_dataframe_name="df",
        expected_cols=None,
        has_header=False
    )

    assert stage_result.status == PipelineStatus.Success

    df = context.dataframes['df'].df

    assert df.count() == 1

    row = df.collect()[0]
    assert row['_c0'] == 'one'
    assert row['_c1'] == 'two'
    assert row['RowNumber'] == 0


def test_csv_loader_pipeline_stage_with_header_no_expected_columns(spark: SparkSession, temp_dir: str):
    copy(local_path("testdata", "_abstract", "00001_csv", "input.csv"), join(temp_dir, PATHS.RAW_DATA))

    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": temp_dir},
        {},
        output_dataframe_name="df",
        expected_cols=None,
        has_header=True
    )

    assert stage_result.status == PipelineStatus.Success

    df = context.dataframes['df'].df

    assert df.count() == 1

    row = df.collect()[0]
    assert row['COLUMN_1'] == 'one'
    assert row['COLUMN_2'] == 'two'
    assert row['RowNumber'] == 0


def prepare_gzip_test_file(scenario: str, temp_dir: str, s3_temp, dataset_id: str = '_abstract'):
    _, s3_folder = s3_temp   # pylint:disable=unused-variable
    local_gzipped = join(temp_dir, PATHS.RAW_DATA)
    gzip_file_copy(local_path('testdata', dataset_id, scenario, 'input.csv'), local_gzipped)
    smart_copy(local_gzipped, join(s3_folder, PATHS.RAW_DATA))
    return s3_folder


def test_csv_gz_loader_pipeline_stage_wrong_number_of_columns(spark: SparkSession, temp_dir: str, s3_temp):
    s3_folder = prepare_gzip_test_file("00001_csv", temp_dir, s3_temp)

    expected_columns = ["COLUMN_1", "COLUMN_2", "COLUMN_3"]
    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": s3_folder},
        {},
        output_dataframe_name="df",
        expected_cols=expected_columns,
    )

    assert stage_result.status == PipelineStatus.Rejected


def test_csv_gz_loader_pipeline_stage_incorrect_column_names(spark: SparkSession, temp_dir: str, s3_temp):
    s3_folder = prepare_gzip_test_file("00001_csv", temp_dir, s3_temp)

    expected_columns = ["COLUMN_ONE", "COLUMN_TWO"]
    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": s3_folder},
        {},
        output_dataframe_name="df",
        expected_cols=expected_columns,
    )

    assert stage_result.status == PipelineStatus.Rejected


def test_csv_gz_loader_pipeline_stage_correct_columns(spark: SparkSession, temp_dir: str, s3_temp):
    s3_folder = prepare_gzip_test_file("00001_csv", temp_dir, s3_temp)

    expected_columns = ["COLUMN_1", "COLUMN_2"]
    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": s3_folder},
        {},
        output_dataframe_name="df",
        expected_cols=expected_columns,
    )

    assert stage_result.status == PipelineStatus.Success

    df = context.dataframes['df'].df

    assert df.count() == 1

    row = df.collect()[0]
    assert row['COLUMN_1'] == 'one'
    assert row['COLUMN_2'] == 'two'
    assert row['RowNumber'] == 0


def test_csv_gz_loader_pipeline_stage_no_header(spark: SparkSession, temp_dir: str, s3_temp):
    s3_folder = prepare_gzip_test_file("00002_csv_no_header", temp_dir, s3_temp)

    expected_columns = ["COLUMN_1", "COLUMN_2"]
    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": s3_folder},
        {},
        output_dataframe_name="df",
        expected_cols=expected_columns,
        has_header=False
    )

    assert stage_result.status == PipelineStatus.Success

    df = context.dataframes['df'].df

    assert df.count() == 1

    row = df.collect()[0]
    assert row['COLUMN_1'] == 'one'
    assert row['COLUMN_2'] == 'two'
    assert row['RowNumber'] == 0


def test_csv_gz_loader_pipeline_stage_no_headers_no_expected_columns(spark: SparkSession, temp_dir: str, s3_temp):
    s3_folder = prepare_gzip_test_file("00002_csv_no_header", temp_dir, s3_temp)

    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": s3_folder},
        {},
        output_dataframe_name="df",
        expected_cols=None,
        has_header=False
    )

    assert stage_result.status == PipelineStatus.Success

    df = context.dataframes['df'].df

    assert df.count() == 1

    row = df.collect()[0]
    assert row['_c0'] == 'one'
    assert row['_c1'] == 'two'
    assert row['RowNumber'] == 0


def test_csv_gz_loader_pipeline_stage_with_header_no_expected_columns(spark: SparkSession, temp_dir: str, s3_temp):
    s3_folder = prepare_gzip_test_file("00001_csv", temp_dir, s3_temp)

    stage_result, context = run_single_stage(
        spark,
        CSVLoaderPipelineStage,
        {"working_folder": s3_folder},
        {},
        output_dataframe_name="df",
        expected_cols=None,
        has_header=True
    )

    assert stage_result.status == PipelineStatus.Success

    df = context.dataframes['df'].df

    assert df.count() == 1

    row = df.collect()[0]
    assert row['COLUMN_1'] == 'one'
    assert row['COLUMN_2'] == 'two'
    assert row['RowNumber'] == 0
