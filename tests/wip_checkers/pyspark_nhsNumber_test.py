
from src.nhs_reusable_code_library.standard_data_validations import pyspark
from src.nhs_reusable_code_library.standard_data_validations.nhsNumberValidation import mod11_check
import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
#import pyspark.testing
#from pyspark.testing import assertDataFrameEqual
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.functions import col, regexp_replace

# @pytest.fixture(scope="session")
# def spark():
#     return SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("pyspark_test").getOrCreate()
    yield spark

from pyspark.sql.functions import col, regexp_replace

# Remove additional spaces in name
def remove_extra_spaces(df, column_name):
    # Remove extra spaces from the specified column
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))

    return df_transformed


# def test_mod11_check(spark):
#     nhsNumber = spark.createDataFrame([{"nhsNumber":'8429141456'}],)
#     expected = spark.createDataFrame([{"mod11_check_is_valid": True}],["mod11_check_is_valid"])
    
#     df = nhsNumber.withColumn("mod11_check_is_valid",
#         mod11_check(F.col("nhsNumber"))
#     )

#     assertDataFrameEqual(expected, df.select("mod11_check_is_valid"))
    
    
def wip_test_mod11_check(spark_fixture):
    sample_data = [{"nhsNumber": "84 29141 456"}]

    # Create a Spark DataFrame
    original_df = spark_fixture.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = remove_extra_spaces(original_df, "nhsNumber")

    expected_data = [{"nhsNumber": "8429141456"}]
    print(transformed_df)
    expected_df = spark_fixture.createDataFrame(expected_data)

    assertDataFrameEqual(transformed_df, expected_df)
    
