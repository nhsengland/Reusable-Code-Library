import os
from pprint import pprint
from typing import Tuple
import pytest
from pyspark.sql import SparkSession
from dsp.dam import dataset_loader
from dsp import pipelines
from nhs_dq_rules_library.dids import before_local_pds_cohort
from dsp.datasets.models.uplift.model_uplift import uplift
from dsp.pipeline.dq_merge import create_dq_results
from dsp.pipeline.models import PipelineContext
from dsp.shared.common.test_helpers import smart_exists, smart_upload, basic_metadata, scenario_input_path, smart_open
from dsp.shared.constants import DS, FT, PATHS
from dsp.shared.models import PipelineStatus
from dsp.shared import local_path

@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


@pytest.fixture()
def temp_dir() -> str:
    return local_path('testdata/dids_test_data/dids')


def test_parse_dids_xml(spark, temp_dir):
    raw_file_path = os.path.join(temp_dir, PATHS.RAW_DATA)

    smart_upload(scenario_input_path(DS.DIDS, FT.XML, '00001_example'), raw_file_path)

    df, _ = dataset_loader(DS.DIDS, FT.XML)(spark, raw_file_path, {})

    assert 2 == df.count()


def test_dataset_loader_csv_passed_as_xml_filetype(spark: SparkSession, temp_dir: str):
    raw_file_path = os.path.join(temp_dir, PATHS.RAW_DATA)
    smart_upload(scenario_input_path(DS.DIDS, FT.CSV, '00003_simple'), raw_file_path)

    pipeline_context = PipelineContext.from_metadata(basic_metadata(temp_dir, DS.DIDS, FT.XML))
    result = pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

    assert result.status == PipelineStatus.Rejected


def test_dataset_loader_xml_passed_as_csv_filetype(spark: SparkSession, temp_dir: str):
    raw_file_path = os.path.join(temp_dir, PATHS.RAW_DATA)
    smart_upload(scenario_input_path(DS.DIDS, FT.XML, '00001_example'), raw_file_path)

    pipeline_context = PipelineContext.from_metadata(basic_metadata(temp_dir, DS.DIDS, FT.CSV))
    result = pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

    assert result.status == PipelineStatus.Rejected


def test_file_received_csv_dids(spark: SparkSession, temp_dir: str):
    smart_upload(scenario_input_path(DS.DIDS, FT.CSV, '00002_example_500_4_errors'),
                 os.path.join(temp_dir, PATHS.RAW_DATA))

    pipeline_context = PipelineContext.from_metadata(basic_metadata(temp_dir, DS.DIDS, FT.CSV, result_scope='bob'))
    pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

    scoped_output_path = os.path.join(temp_dir, 'bob')

    assert smart_exists(os.path.join(scoped_output_path, PATHS.DQ))

    df_dq = spark.read.parquet(os.path.join(scoped_output_path, PATHS.DQ))

    dq_fails = create_dq_results(df_dq, 123, DS.DIDS).collect()

    try:
        assert len(dq_fails) == 16
    except AssertionError:
        for row in dq_fails:
            pprint(row.asDict())
        assert False

    assert not smart_exists(os.path.join(scoped_output_path, PATHS.OUT))


def test_file_invalid_received_csv_dids(spark: SparkSession, temp_dir: str):
    with smart_open(os.path.join(temp_dir, PATHS.RAW_DATA), 'w+') as f:
        f.write('invalid csv')

    pipeline_context = PipelineContext.from_metadata(basic_metadata(temp_dir, DS.DIDS, FT.CSV))
    result = pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

    assert result.status == PipelineStatus.Rejected


def test_file_received_xml_dids(spark: SparkSession, temp_dir: str):
    smart_upload(scenario_input_path(DS.DIDS, FT.XML, '00001_example'), os.path.join(temp_dir, PATHS.RAW_DATA))
    before_local_pds_cohort(spark)
    pipeline_context = PipelineContext.from_metadata(basic_metadata(temp_dir, DS.DIDS, FT.XML))
    pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

    assert smart_exists(os.path.join(temp_dir, PATHS.OUT, PATHS.OUT))

    records_out_df = spark.read.parquet(os.path.join(temp_dir, PATHS.OUT, PATHS.OUT))
    records_out_df = uplift(spark, records_out_df, DS.DIDS, PATHS.OUT)

    records_out_count = records_out_df.count()

    assert 2 == records_out_count


@pytest.mark.skip
@pytest.mark.slow
def test_file_received_csv_dids_s3(spark: SparkSession, s3_temp: Tuple[object, str]):
    bucket, temp_dir = s3_temp

    smart_upload(scenario_input_path(DS.DIDS, FT.CSV, '00002_example_500_4_errors'),
                 os.path.join(temp_dir, PATHS.RAW_DATA))

    pipeline_context = PipelineContext.from_metadata(basic_metadata(temp_dir, DS.DIDS, FT.CSV))
    pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)

    assert smart_exists(os.path.join(temp_dir, 'dq_fail', '_SUCCESS'))

    dq_fails = spark.read.parquet(os.path.join(temp_dir, 'dq_fail')).collect()

    assert len(dq_fails) == 4

    assert smart_exists(os.path.join(temp_dir, PATHS.OUT, '_SUCCESS'))


def test_mps_response_and_dids_csv(spark: SparkSession, temp_dir: str):
    smart_upload(scenario_input_path(DS.DIDS, FT.CSV, '00002_example_500_4_errors'),
                 os.path.join(temp_dir, PATHS.RAW_DATA))

    pipeline_context = PipelineContext.from_metadata(basic_metadata(temp_dir, DS.DIDS, FT.CSV))
    pipelines.get_pipeline(pipeline_context).run(spark, pipeline_context)
