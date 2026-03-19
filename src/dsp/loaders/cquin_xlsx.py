from functools import reduce
from typing import Tuple, Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from dsp.datasets.cquin.submitted import Order
from dsp.loaders import LoaderRejectionError
from dsp.loaders.xlsx import load_sheet
from dsp.pipeline import ValidationResult

_NUMERIC_REGEX = r"^[-+]?[0-9]*.?[0-9]+$"


def load(spark: SparkSession, path: str, _) -> Tuple[DataFrame, Optional[ValidationResult]]:
    """Returns a dids DataFrames from the xlsx.

       Args:
           spark (SparkSession):
           path (str): the path to the xlsx file

       Returns:
           df_with_cquin (DataFrame)
       """

    df_orig = load_sheet(spark, path, 'ExtractSheet', use_header=True)

    df = df_orig.where(df_orig.Data.rlike(_NUMERIC_REGEX))

    submitted_cols = df.schema.names
    cquin_columns = Order
    if len(submitted_cols) != len(Order):
        raise LoaderRejectionError from IndexError(
            "There was a problem loading the file. Number of columns in the don't match the expected number"
        )

    df_renamed = reduce(
        lambda _df, idx: _df.withColumnRenamed(submitted_cols[idx], cquin_columns[idx]), range(len(submitted_cols)), df
    )

    return df_renamed, None
