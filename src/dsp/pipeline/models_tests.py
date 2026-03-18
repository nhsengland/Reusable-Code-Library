from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pytest import fixture

from dsp.common.spark_helpers import dataframes_equal
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.shared.aws import s3_build_uri


@fixture
def df_simple(spark: SparkContext) -> DataFrame:
    return spark.createDataFrame(
        [(n,) for n in range(10)],
        ['number']
    )


def test_pipeline_context_serialize_without_dataframes(df_simple: DataFrame, temp_dir: str):
    context = PipelineContext(
        temp_dir,
        {'number': 1, 'string': 'foo bar'},
        {'simple': DataFrameInfo(df_simple, 'simple_view')}
    )
    context_serialized = context.serialize(with_dataframes=False)

    assert context_serialized['working_folder'] == temp_dir
    assert context_serialized['primitives'] == {'number': 1, 'string': 'foo bar'}
    assert 'dataframes' not in context_serialized


def test_pipeline_context_serialize_with_dataframes(df_simple: DataFrame, temp_dir: str):
    work_dir_uri = s3_build_uri("local-testing", temp_dir[1:])
    context = PipelineContext(
        work_dir_uri,
        {'number': 1, 'string': 'foo bar'},
        {'simple': DataFrameInfo(df_simple, 'simple_view')}
    )
    context_serialized = context.serialize(with_dataframes=True)

    assert context_serialized['working_folder'] == work_dir_uri
    assert context_serialized['primitives'] == {'number': 1, 'string': 'foo bar'}
    assert context_serialized['dataframes'] == {'simple': ['{}/suspended/simple'.format(work_dir_uri), 'simple_view']}


def test_pipeline_context_deserialize_without_dataframes(spark: SparkSession, temp_dir: str):
    context = PipelineContext.deserialize(spark, {
        'working_folder': temp_dir,
        'primitives': {'number': 1, 'string': 'foo bar'},
    })
    assert context.working_folder == temp_dir
    assert context.primitives == {'number': 1, 'string': 'foo bar'}
    assert context.dataframes == {}


def test_pipeline_context_deserialize_with_dataframes(spark: SparkSession, df_simple: DataFrame, temp_dir: str):
    df_simple.write.parquet('{}/suspended/simple'.format(temp_dir))

    context = PipelineContext.deserialize(spark, {
        'working_folder': temp_dir,
        'primitives': {'number': 1, 'string': 'foo bar'},
        'dataframes': {'simple': ['{}/suspended/simple'.format(temp_dir), 'simple_view']}
    })
    assert context.working_folder == temp_dir
    assert context.primitives == {'number': 1, 'string': 'foo bar'}
    assert set(context.dataframes.keys()) == {'simple'}
    assert dataframes_equal(context.dataframes['simple'].df, df_simple)
    assert dataframes_equal(spark.sql('SELECT * FROM simple_view'), df_simple)
