from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, TimestampType

from dsp.dq_files.output import Fields as DQFields

_COMMON_DQ_FIELDS = [
    StructField(DQFields.TABLE, StringType(), nullable=True),
    StructField(DQFields.ROW_INDEX, StringType(), nullable=True),
    StructField(DQFields.ATTRIBUTE, StringType(), nullable=True),
    StructField(DQFields.FIELDS, ArrayType(StringType()), nullable=False),
    StructField(DQFields.CODE, StringType(), nullable=False),
    StructField(DQFields.MESSAGE, StringType(), nullable=False),
    StructField(DQFields.FIELD_VALUES, MapType(StringType(), StringType()), nullable=False),
    StructField(DQFields.TYPE, StringType(), nullable=False),
    StructField(DQFields.ACTION, StringType(), nullable=True),
]

DQ_MESSAGE_SCHEMA = StructType(_COMMON_DQ_FIELDS)
"""
Schema for DQ messages, without DQ timestamp. May be redundant at this stage.
"""

DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA = StructType([
    *_COMMON_DQ_FIELDS,
    StructField(DQFields.DQ_TS, TimestampType(), nullable=False),
])
"""
Schema for DQ messages including timestamp
"""
