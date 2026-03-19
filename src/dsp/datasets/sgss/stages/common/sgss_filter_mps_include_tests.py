from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.validations.validator import compare_results
from .sgss_filter_mps_include import SGSSFilterMPSIncludeStage


def test_sgss_filter_mps_include(spark: SparkSession):
    input_schema = StructType([
        StructField(Fields.CDR_Specimen_Request_SK, StringType()),
        StructField(Fields.PERSON_ID, StringType()),
        StructField(Fields.Patient_NHS_Number, StringType()),
        StructField(Fields.Reporting_Lab, StringType()),
    ])

    input_data = [
        (10200403032021003, '9987654321', '9987654321', 'PILLAR 2 TESTING'),
        (10200503032021003, None, None, 'PILLAR 2 TESTING'),
        (10200603032021003, '9152346789', '9152346789', 'RESPIRATORY DATA MART'),
        (10200703032021003, '9123456789', '9123456789', 'RESPIRATORY DATA MART'),
    ]

    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), input_schema)

    expected_data = [
        (10200403032021003, '9987654321', '9987654321', 'PILLAR 2 TESTING'),
        (10200603032021003, '9152346789', '9152346789', 'RESPIRATORY DATA MART'),
        (10200703032021003, '9123456789', '9123456789', 'RESPIRATORY DATA MART'),
    ]
    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), input_schema)

    context = PipelineContext('', dataframes={'input_df': DataFrameInfo(input_df)})
    stage = SGSSFilterMPSIncludeStage('input_df', 'output_df', passthrough_dataframes=set())
    stage._run(spark, context)

    output_df = context.dataframes['output_df'].df

    assert compare_results(output_df, expected_df, join_columns=[Fields.CDR_Specimen_Request_SK])

