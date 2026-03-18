from decimal import Decimal
from typing import Optional, Dict, Iterable, FrozenSet, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, Row, ArrayType, IntegerType, LongType
from pyspark.sql.functions import col

from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.models.uplift.common import simple_uplift, rename_fields, add_fields, remove_fields
from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.msds.msd101.version_1 import schema as MSDS_V1
from dsp.datasets.models.uplift.msds.msd101.version_2 import schema as MSDS_V2
from dsp.datasets.models.uplift.msds.msd101.version_3 import schema as MSDS_V3, field_mapping as MSDS_V3_MAPPING
from dsp.datasets.models.uplift.msds.msd101.version_4 import schema as MSDS_V4
from dsp.datasets.models.uplift.msds.msd101.version_5 import schema as MSDS_V5, new_fields as MSDS_V5_NEW_FIELDS
from dsp.datasets.models.uplift.msds.msd101.version_6 import schema as MSDS_V6


def v5_v6_uplift_decimal_fields(
        df: DataFrame,
        version_from: int,
        version_to: int,
        source_schema: StructType,
        target_schema: StructType,
) -> Tuple[int, DataFrame]:

    def transform_field(
            row: Row, target_field: str, from_schema: StructType, to_schema: StructType):

        if target_field in from_schema.names:
            source_type = from_schema[target_field].dataType
        else:
            source_type = None
        target_type = to_schema[target_field].dataType

        if source_type:
            source_value = row[target_field]
        else:
            source_value = None

        if isinstance(source_type, ArrayType) and source_value:
            if not isinstance(source_type.elementType, StructType):
                if target_field in ['MSD203_ID', 'MSD202_ID', 'MSD201_ID', 'MSD104_ID', 'MSD103_ID', 'MSD106_ID',
                                    'MSD108_ID', 'MSD109_ID', 'MSD000_ID', 'MSD504_ID', 'MSD502_ID', 'MSD501_ID',
                                    'MSD503_ID', 'MSD406_ID', 'MSD405_ID', 'MSD404_ID', 'MSD401_ID', 'MSD402_ID',
                                    'MSD403_ID', 'MSD302_ID', 'MSD301_ID', 'MSD101_ID', 'MSD102_ID', 'MSD107_ID',
                                    'MSD002_ID', 'MSD001_ID', 'MSD004_ID', 'MSD003_ID', 'MSD105_ID', 'MSD602_ID',
                                    'MSD601_ID', 'MSD901_ID'] and (
                        isinstance(source_type, IntegerType) or isinstance(source_type, LongType)
                ):
                    return Decimal(source_value)
                return source_value
            return [
                transform_row(val, source_type.elementType, target_type.elementType) for val in source_value
            ]

        if isinstance(source_type, StructType) and source_value:
            return transform_row(source_value, source_type, target_type)

        if target_field in ['MSD203_ID', 'MSD202_ID', 'MSD201_ID', 'MSD104_ID', 'MSD103_ID', 'MSD106_ID',
                            'MSD108_ID', 'MSD109_ID', 'MSD000_ID', 'MSD504_ID', 'MSD502_ID', 'MSD501_ID',
                            'MSD503_ID', 'MSD406_ID', 'MSD405_ID', 'MSD404_ID', 'MSD401_ID', 'MSD402_ID',
                            'MSD403_ID', 'MSD302_ID', 'MSD301_ID', 'MSD101_ID', 'MSD102_ID', 'MSD107_ID',
                            'MSD002_ID', 'MSD001_ID', 'MSD004_ID', 'MSD003_ID', 'MSD105_ID', 'MSD602_ID',
                            'MSD601_ID', 'MSD901_ID'] and (
                isinstance(source_type, IntegerType) or isinstance(source_type, LongType)):
            return Decimal(source_value)

        if target_field in ['RECORD_VERSION']:
            return version_to

        return source_value


    def transform_row(row: Row, from_schema: StructType, to_schema: StructType) -> Row:
        return Row(*[transform_field(row, f, from_schema, to_schema) for f in to_schema.names])


    def transform_row_base(row: Row) -> Row:
        return transform_row(row, source_schema, target_schema)

    assert  df.filter(col(CommonFields.META)[CommonFields.RECORD_VERSION] == version_from).count() == 1
    df_final = df.rdd.map(transform_row_base).toDF(target_schema)
    assert df_final.filter(col(CommonFields.META)[CommonFields.RECORD_VERSION] == version_to).count() == 1

    return version_to, df_final

UPLIFTS = {
    0: lambda df: simple_uplift(df, 0, 1, MSDS_V1),
    1: lambda df: simple_uplift(df, 1, 2, MSDS_V2),
    2: lambda df: simple_uplift(df, 2, 3, MSDS_V3, field_mapping=MSDS_V3_MAPPING),
    3: lambda df: simple_uplift(df, 3, 4, MSDS_V4),
    4: lambda df: simple_uplift(df, 4, 5, MSDS_V5, new_fields=MSDS_V5_NEW_FIELDS),
    5: lambda df: v5_v6_uplift_decimal_fields(df, 5, 6, MSDS_V5, MSDS_V6),
}
