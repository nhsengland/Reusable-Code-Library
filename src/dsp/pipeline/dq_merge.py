from uuid import uuid4

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from dsp.dq_files.output import Fields as DQFields, Order as DQOrder


def execute(spark: SparkSession, submission_id: int, dataset_id: str, uri: str):
    df = spark.read.parquet(uri)

    df_dq = create_dq_results(df, submission_id, dataset_id)

    _temp_table = "{}_dq_merge_{}".format(dataset_id, uuid4().hex)

    df_dq.createOrReplaceTempView(_temp_table)

    spark.sql('INSERT INTO dq.dq SELECT ' + ', '.join(c for c in DQOrder) + ' FROM ' + _temp_table)


def create_dq_results(df: DataFrame, submission_id: int, dataset_id: str) -> DataFrame:
    df_dq = df.withColumn(DQFields.SUBMISSION_ID, lit(submission_id)) \
        .withColumn(DQFields.DATASET, lit(dataset_id)) \
        .select(DQOrder)

    return df_dq
