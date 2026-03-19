from decimal import Decimal
from typing import Optional, Dict, Iterable, FrozenSet, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, Row

from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.models.uplift.common import simple_uplift, rename_fields, add_fields, remove_fields
from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.msds.msd901.version_1 import schema as MSDS_V1
from dsp.datasets.models.uplift.msds.msd901.version_2 import schema as MSDS_V2
from dsp.datasets.models.uplift.msds.msd901.version_3 import schema as MSDS_V3
from dsp.datasets.models.uplift.msds.msd901.version_4 import schema as MSDS_V4, new_fields as MSDS_V4_NEW_FIELDS
from dsp.datasets.models.uplift.msds.msd901.version_5 import schema as MSDS_V5
from dsp.datasets.models.uplift.msds.msd901.version_6 import schema as MSDS_V6


def v5_v6_uplift_decimal_fields(
        df: DataFrame,
        version_from: int,
        version_to: int,
        target_schema: StructType,
        field_mapping: Optional[Dict[str, str]] = None,
        new_fields: Iterable[str] = None,
        fields_to_remove: Optional[FrozenSet[str]] = None,
        add_if_missing_fields: Iterable[str] = None,
) -> Tuple[int, DataFrame]:
    if new_fields is None:
        new_fields = []

    if add_if_missing_fields:
        missing_fields = [field for field in add_if_missing_fields if field not in df.columns]
        new_fields = list(new_fields) + missing_fields

    def uplift_row(row: Row) -> Row:
        row_dict = row.asDict(recursive=True)
        if field_mapping:
            row_dict = rename_fields(row_dict, field_mapping)
        if new_fields:
            row_dict = add_fields(row_dict, new_fields)
        if fields_to_remove:
            row_dict = remove_fields(row_dict, fields_to_remove)

        # Convert to a Decimal
        msd000_id = row_dict['Header']['MSD000_ID']
        msd901_id = row_dict['MSD901_ID']
        if msd000_id is not None:
            row_dict['Header']['MSD000_ID'] = Decimal(msd000_id)
        if msd901_id is not None:
            row_dict['MSD901_ID'] = Decimal(msd901_id)

        assert row_dict[CommonFields.META][CommonFields.RECORD_VERSION] == version_from
        row_dict[CommonFields.META][CommonFields.RECORD_VERSION] = version_to
        return Row(**row_dict)

    uplifted_rdd = df.rdd.map(uplift_row)
    df = uplifted_rdd.toDF(target_schema)

    return version_to, df

UPLIFTS = {
    0: lambda df: simple_uplift(df, 0, 1, MSDS_V1),
    1: lambda df: simple_uplift(df, 1, 2, MSDS_V2),
    2: lambda df: simple_uplift(df, 2, 3, MSDS_V3),
    3: lambda df: simple_uplift(df, 3, 4, MSDS_V4, new_fields=MSDS_V4_NEW_FIELDS),
    4: lambda df: simple_uplift(df, 4, 5, MSDS_V5),
    5: lambda df: v5_v6_uplift_decimal_fields(df, 5, 6, MSDS_V6),
}
