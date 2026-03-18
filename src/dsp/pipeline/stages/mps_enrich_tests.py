from mock import Mock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pytest import raises

from dsp.integration.mps.constants import SpineResponseCodes
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.stages.mps_enrich import MPSEnrichStage

from dsp.datasets.mps.response import Fields as MpsResponseFields


def test_rejects_mps_response_with_invalid_records(spark: SparkSession):
    mps_dataframe = spark.createDataFrame([
        (SpineResponseCodes.UNEXPECTED_ERROR,)
    ], StructType([
        StructField(MpsResponseFields.ERROR_SUCCESS_CODE, StringType())
    ]))

    context_mock = Mock(PipelineContext)
    context_mock.dataframes = {
        'mps_result': DataFrameInfo(mps_dataframe)
    }

    with raises(ValueError):
        MPSEnrichStage([])._run(spark, context_mock)


def test_applies_enirchments_to_trace_dataframe(spark: SparkSession):
    enrichment_1 = Mock()
    enrichment_2 = Mock()

    submission_id = 123

    mps_dataframe = spark.createDataFrame([
        (SpineResponseCodes.SUCCESS,)
    ], StructType([
        StructField(MpsResponseFields.ERROR_SUCCESS_CODE, StringType())
    ]))

    initial_trace_dataframe_mock = Mock(DataFrame)

    context_mock = Mock(PipelineContext)
    context_mock.primitives = {
        'submission_id': submission_id
    }
    context_mock.dataframes = {
        'mps_result': DataFrameInfo(mps_dataframe),
        'df': DataFrameInfo(initial_trace_dataframe_mock)
    }

    context = MPSEnrichStage([enrichment_1, enrichment_2])._run(spark, context_mock)

    enrichment_1.assert_called_once_with(submission_id, initial_trace_dataframe_mock, mps_dataframe)
    enrichment_2.assert_called_once_with(submission_id, enrichment_1.return_value, mps_dataframe)

    assert context.dataframes['df'].df == enrichment_2.return_value
