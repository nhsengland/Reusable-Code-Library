from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

from dsp.datasets.ingestions.sgss.config import Fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.validations.validator import compare_results
from dsp.pipeline.stages.trim_column_contents import TrimColumnContentsStage


def test_trim_column_contents(spark: SparkSession):
    input_schema = StructType([
        StructField(Fields.CDR_Specimen_Request_SK, StringType()),
        StructField(Fields.Patient_PostCode, StringType())
    ])

    input_data = [
        (' 10200103032021003', ' BD20 5RB ')
    ]
    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), input_schema)

    expected_data = [
        ('10200103032021003', 'BD20 5RB')
    ]
    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), input_schema)

    context = PipelineContext('', dataframes={'input_df': DataFrameInfo(input_df)})
    stage = TrimColumnContentsStage(input_dataframe_name='input_df')
    stage._run(spark, context)

    output_df = context.dataframes['input_df'].df
    assert compare_results(output_df, expected_df, join_columns=[Fields.CDR_Specimen_Request_SK])
