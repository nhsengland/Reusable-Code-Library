import pytest
import datetime

from pyspark.sql import SparkSession, DataFrame

from dsp.pipeline.stages import AddTraceabilityColumnsStage
from dsp.pipeline.test_helpers import run_single_stage
from shared.models import PipelineStatus
from shared.constants import DS


@pytest.fixture
def temp_dataframe_meta(spark):
    return spark.createDataFrame([('Carl', '23456'), ('Simon', '98766'), ('bird', 'squawk')], ['name', 'META'])


def test_reserved_meta(spark: SparkSession, temp_dataframe_meta: DataFrame):

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    stage_result, context = run_single_stage(
        spark,
        AddTraceabilityColumnsStage,
        {'working_folder': '', 'submission_id': 10, 'submitted_timestamp': timestamp, 'dataset_id': DS.GENERIC},
        {DS.GENERIC: temp_dataframe_meta},
        {DS.GENERIC}
    )

    assert stage_result.status == PipelineStatus.Rejected
