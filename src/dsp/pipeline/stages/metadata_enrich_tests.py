from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ByteType

from dsp.pipeline.stages.metadata_enrich import MetadataEnrichStage
from dsp.pipeline.test_helpers import run_single_stage
from shared.common.test_helpers import basic_metadata


def test_enrich_with_meta(spark: SparkSession):
    test_data_1 = [('123456', 1, 'LS14HR'),
                   ('987654', 2, 'LS14HR'),
                   ('999888', 2, 'LS14HR')]

    schema_1 = StructType([
        StructField("NHS_NUMBER", StringType(), True),
        StructField("NHS_NUMBER_TYPE", ByteType(), True),
        StructField("POSTCODE", StringType(), True)
    ])

    expected_records_1 = [
        Row(SUBMITTER_ORGANISATION='TEST'),
        Row(SUBMITTER_ORGANISATION='TEST'),
        Row(SUBMITTER_ORGANISATION='TEST')
    ]

    df = spark.createDataFrame(test_data_1, schema_1)
    metadata = basic_metadata(working_folder='test', dataset_id='dids', file_type='csv')

    _, pipeline_context = run_single_stage(spark, MetadataEnrichStage, metadata, {'df': df})

    assert expected_records_1 == pipeline_context.dataframes['df'].df.select('SUBMITTER_ORGANISATION').collect()
