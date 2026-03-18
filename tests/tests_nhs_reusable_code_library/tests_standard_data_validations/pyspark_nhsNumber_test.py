
from nhs_reusable_code_library.standard_data_validations import pyspark
from nhs_reusable_code_library.standard_data_validations.nhsNumberValidation import mod11_check
import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from nhs_reusable_code_library.standard_data_validations.nhsNumberValidation.mod11_check import mod11_check
#import pyspark.testing
#from pyspark.testing import assertDataFrameEqual
from pyspark_test import assert_pyspark_df_equal
from pyspark.sql.functions import col, regexp_replace, trim
#from pyspark.sql.functions import 

# @pytest.fixture(scope="session")
# def spark():
#     return SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()

@pytest.fixture
def spark():
    spark = SparkSession.builder.appName("pyspark_test").getOrCreate()
    yield spark

# Remove additional spaces in name
def remove_extra_spaces(df, column_name):
    # Remove extra spaces from the specified column
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))

    return df_transformed

def remove_all_spaces(df):
    # Remove extra spaces from the specified column
    df = df.withColumn("nhsNumber", regexp_replace(col("nhsNumber"), " ", ""))

    return df

    
def test_mod11_check(spark):
    sample_data = [{"nhsNumber": "84 29141 456"}]

    # Create a Spark DataFrame
    original_df = spark.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = remove_all_spaces(original_df)

    expected_data = [{"nhsNumber": "8429141456"}]
    print(transformed_df)
    expected_df = spark.createDataFrame(expected_data)

    assert_pyspark_df_equal(transformed_df, expected_df)
    
