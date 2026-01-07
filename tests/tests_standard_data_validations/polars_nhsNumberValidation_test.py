
from nhs_reusable_code_library.standard_data_validations import polars
import pytest
import polars as pl
from polars.testing import assert_frame_equal


@pytest.mark.parametrize(
    "nhsNumber, expected",
    [
        (
            pl.DataFrame({"nhsNumber":["8429141456"]}), 
            pl.DataFrame({"mod11_check_is_valid":[True]})
        ),
    ],
)
def test_mod11_check(nhsNumber, expected):
    df = nhsNumber.with_columns(
        polars.mod11_check(pl.col("nhsNumber"))
    )

    assert_frame_equal(expected, df[["mod11_check_is_valid"]])