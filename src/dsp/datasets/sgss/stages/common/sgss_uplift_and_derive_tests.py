from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, ShortType

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.pipeline.models import DataFrameInfo, PipelineContext
from dsp.validation.validator import compare_results
from .sgss_uplift_and_derive import SGSSUpliftAndDeriveStage


def test_sgss_uplift_and_derive(spark: SparkSession):
    input_schema = StructType([
        StructField(Fields.CDR_Specimen_Request_SK, StringType()),
        StructField(Fields.Specimen_Date, StringType()),
        StructField(Fields.Lab_Report_Date, StringType()),
        StructField(Fields.Patient_Date_Of_Birth, StringType()),
        StructField(Fields.Age_in_Years, StringType()),
        StructField(Fields.Patient_PostCode, StringType()),
    ])

    input_data = [
        ('10200103032021003', '01/03/2021', '01/03/2021', '28/08/1898', None, 'BD20 5RB')
    ]

    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), input_schema)

    expected_schema = StructType([
        StructField(Fields.CDR_Specimen_Request_SK, StringType()),
        StructField(Fields.Specimen_Date, DateType()),
        StructField(Fields.Lab_Report_Date, DateType()),
        StructField(Fields.Patient_Date_Of_Birth, DateType()),
        StructField(Fields.Age_in_Years, ShortType()),
        StructField(Fields.Patient_PostCode, StringType()),
        StructField(Fields.pcds, StringType()),
        StructField(Fields.pcds_sector, StringType()),
        StructField(Fields.latitude, FloatType()),
        StructField(Fields.longitude, FloatType())
    ])

    expected_data = [
        ('10200103032021003', datetime.strptime('2021-03-01', '%Y-%m-%d'),
         datetime.strptime('2021-03-01', '%Y-%m-%d'), datetime.strptime('1898-08-28', '%Y-%m-%d'),
         None, 'BD20 5RB', 'BD20 5RB', 'BD20 5', 53.888626, -1.865558)
    ]

    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), expected_schema)

    context = PipelineContext('', dataframes={'input_df': DataFrameInfo(input_df)})
    stage = SGSSUpliftAndDeriveStage(input_dataframe_name='input_df', output_dataframe_name='output_df')
    stage._run(spark, context)

    output_df = context.dataframes['output_df'].df

    assert compare_results(output_df, expected_df, join_columns=[Fields.CDR_Specimen_Request_SK])
