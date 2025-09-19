
from nhs_reusable_code_library.standard_data_validations import pyspark
import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()


def test_mod11_check(spark):
    nhsNumber = spark.createDataFrame([{"nhsNumber":'8429141456'}],)
    expected = spark.createDataFrame([{"mod11_check_is_valid": True}],["mod11_check_is_valid"])
    
    df = nhsNumber.withColumn("mod11_check_is_valid",
        pyspark.mod11_check(F.col("nhsNumber"))
    )

    assertDataFrameEqual(expected, df.select("mod11_check_is_valid"))