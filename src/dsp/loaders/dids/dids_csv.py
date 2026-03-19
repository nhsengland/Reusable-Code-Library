from functools import reduce
from typing import Tuple, Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from dsp.outputs.dids.submitted import Order
from dsp.loaders import LoaderRejectionError
from dsp.pipeline import ValidationResult


def csv_loader(spark: SparkSession, path: str, _) -> Tuple[DataFrame, Optional[ValidationResult]]:
    """Returns a dids DataFrames from the csv. Raises an IndexError if the no. of columns don't match the expected

       Args:
           spark (SparkSession):
           path (str): the path to the csv file

       Returns:
           df_with_dids_columns (DataFrame)
       """
    df = spark.read.option("mode", "FAILFAST").csv(path)

    try:
        df.collect()
    except:
        raise LoaderRejectionError

    submitted_cols = df.schema.names
    dids_columns = Order
    if len(submitted_cols) != len(Order):
        raise LoaderRejectionError

    df_with_dids_columns = reduce(
        lambda _df, idx: _df.withColumnRenamed(submitted_cols[idx], dids_columns[idx]), range(len(submitted_cols)), df
    )

    return df_with_dids_columns, None
