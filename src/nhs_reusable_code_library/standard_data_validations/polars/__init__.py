import polars as pl
import src.nhs_reusable_code_library.standard_data_validations.nhsNumberValidation.nhsNumberValidation as nhsNumberValidation

def mod11_check(nhs_number_col : pl.col) -> pl.col:
    """
    Validates NHS numbers using the Modulus 11 algorithm in Polars

    Parameters:
    nhs_number_col (pl.col): A Polars column containing NHS numbers as strings.

    Returns:
    pl.col("mod11_check_is_valid"): A Polars column with boolean values indicating the validity of each NHS number.
    """
    return nhs_number_col.map_elements(nhsNumberValidation.mod11_check, 
                                        return_dtype=pl.Boolean
                                        ).alias("mod11_check_is_valid")