import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from .sgss_reject_on_duplicates import SGSSRejectOnDuplicatesStage


def test_sgss_reject_on_duplicates(spark: SparkSession):
    input_schema = StructType([
        StructField(Fields.CDR_Specimen_Request_SK, StringType()),
    ])

    input_data = [
        (10200803032021003,),
        (10200803032021003,)
    ]

    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), input_schema)

    context = PipelineContext('', dataframes={'input_df': DataFrameInfo(input_df)})
    stage = SGSSRejectOnDuplicatesStage('input_df')

    with pytest.raises(ValueError) as e:
        stage._run(spark, context)

    assert str(e.value) == f'Submission contains duplicate {Fields.CDR_Specimen_Request_SK} values.'
