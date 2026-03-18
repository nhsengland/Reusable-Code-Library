from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.validation.validator import compare_results
from .sgss_filter_non_positives import SGSSFilterNonPositivesStage


def test_sgss_filter_non_positives(spark: SparkSession):
    input_schema = StructType([
        StructField(Fields.CDR_Specimen_Request_SK, StringType()),
        StructField(Fields.Organism_Species_Name, StringType()),
    ])

    input_data = [
        (10200703032021003, 'SARS-CoV-2 CORONAVIRUS (Covid-19) NEGATIVE'),
        (10200803032021003, 'SARS-CoV-2 CORONAVIRUS (Covid-19)')
    ]

    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), input_schema)

    expected_data = [
        (10200803032021003, 'SARS-CoV-2 CORONAVIRUS (Covid-19)'),
    ]

    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), input_schema)

    context = PipelineContext('', dataframes={'input_df': DataFrameInfo(input_df)})
    stage = SGSSFilterNonPositivesStage('input_df', 'output_df')
    stage._run(spark, context)

    output_df = context.dataframes['output_df'].df

    assert compare_results(output_df, expected_df, join_columns=[Fields.CDR_Specimen_Request_SK])
