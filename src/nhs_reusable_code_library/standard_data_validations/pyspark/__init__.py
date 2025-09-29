import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import BooleanType
import src.nhs_reusable_code_library.standard_data_validations.nhsNumberValidation.nhsNumberValidation as nhsNumberValidation

def mod11_check(nhs_number_col : F.col) -> F.col:
    """
    Validates NHS numbers using the Modulus 11 algorithm in PySpark

    Parameters:
    nhs_number_col (Column): A PySpark column containing NHS numbers as strings.

    Returns:
    Column: A PySpark column with boolean values indicating the validity of each NHS number, named 'mod11_check_is_valid'.
    """

    mod11_check_udf = F.udf(nhsNumberValidation.mod11_check, BooleanType())
    return mod11_check_udf(nhs_number_col).alias("mod11_check_is_valid")