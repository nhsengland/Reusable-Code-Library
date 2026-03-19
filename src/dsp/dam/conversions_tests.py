import datetime

from pyspark.sql.session import SparkSession

from dsp.dam.conversions import to_timestamp_from_time_string, to_date_from_string


def test_to_timestamp_from_time_string(spark: SparkSession):
    df = spark.createDataFrame([('10:01:02',), (None,)], ['tick'])
    uplifted = df.withColumn('tick', to_timestamp_from_time_string(df.tick)).collect()
    assert uplifted[0].tick == datetime.datetime(1970, 1, 1, 10, 1, 2)
    assert uplifted[1].tick is None

def test_to_date_from_string(spark: SparkSession):
    df = spark.createDataFrame([('2019-03-19',)], ['tick'])
    uplifted = df.withColumn('tick', to_date_from_string('tick')).collect()
    assert uplifted[0].tick == datetime.date(2019, 3, 19)
