from typing import Tuple, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import Row, StructType

from dsp.datasets.common import Fields as CommonFields


def filetype_uplift(
        df: DataFrame,
        version_from: int,
        version_to: int,
        target_schema: StructType
) -> Tuple[int, DataFrame]:
    def uplift_row(row: Row) -> Row:
        row_dict = row.asDict(recursive=True)
        assert row_dict[CommonFields.META][CommonFields.RECORD_VERSION] == version_from
        filetype_field = row_dict['Header']['FileType']
        if isinstance(filetype_field, str):
            row_dict['Header']['FileType'] = 1 if filetype_field == 'Primary' else 2
        row_dict[CommonFields.META][CommonFields.RECORD_VERSION] = version_to
        return Row(**row_dict)

    uplifted_rdd = df.rdd.map(uplift_row)
    df = uplifted_rdd.toDF(target_schema)

    return version_to, df
