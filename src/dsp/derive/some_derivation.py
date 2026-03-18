from typing import Callable, Sequence, Tuple

from pyspark.sql import Column, DataFrame

from dsp.common import canonical_name


def derive(df: DataFrame, derivations: Sequence[Tuple[str, Callable[[], Column]]]) -> DataFrame:
    derived_df = df

    for col_out, derivation in derivations:
        col_out = canonical_name(col_out)

        derived_df = derived_df.withColumn(col_out, derivation())

    return derived_df


def selection(df: DataFrame, selections: Sequence[Tuple[str, Callable[[], Column]]]) -> DataFrame:
    def _enum_selections():
        for col_name, col_factory in selections:
            try:
                yield col_factory().alias(col_name)
            except Exception as e:
                raise IndexError("There was a problem creating the result column {}".format(col_name)) from e

    cols = [c for c in _enum_selections()]

    return df.select(*cols)
