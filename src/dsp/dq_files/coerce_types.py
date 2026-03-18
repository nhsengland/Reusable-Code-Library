from typing import Callable, Tuple, List

from pyspark.sql import DataFrame, Column

from dsp.common import canonical_name
from dsp.shared.logger import log_action


@log_action()
def coerce_data_types(df: DataFrame, coercions: List[Tuple[str, Callable[[str], Column], Callable[[str], Column]]],
                      revert: bool = False) -> DataFrame:
    coerced_df = df

    for col_name, coercion, reversion in coercions:
        c_name = canonical_name(col_name)

        transform = reversion if revert else coercion

        coerced_df = coerced_df.withColumn(c_name, transform(c_name))

    return coerced_df
