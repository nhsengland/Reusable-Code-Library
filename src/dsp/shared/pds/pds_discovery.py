from pyspark.sql import DataFrame, SparkSession

_pds_data = None


def set_pds_source(df: DataFrame):
    global _pds_data
    _pds_data = df


def get_pds_data(spark: SparkSession) -> DataFrame:
    global _pds_data
    return _pds_data if _pds_data else spark.table('pds.pds')
