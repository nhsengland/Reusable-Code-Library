from typing import Iterable

from dsp.pipeline.deidentify.deidentify import DeidFields
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, lit, explode
from pyspark.sql.types import StructType

from dsp.datasets.models.deid import SCHEMA as DEID_SCHEMA
from dsp.integration.de_identify import NHS_NUMBER, LOCAL_PATIENT_ID, PERSON_ID


_DE_ID_EPMAWS2 = {
     'NHSorCHINumber': NHS_NUMBER,
}


class DeidFieldsEPMAWS2(DeidFields):
    def de_id_fields(self, spark: SparkSession, df_in: DataFrame,
                     extra_column_names: Iterable[str] = frozenset()) -> DataFrame:
        """
        @param extra_column_names: names of columns to pass through unchanged
        """
        extra_columns = [field for field in df_in.schema.fields if field.name in extra_column_names]
        assert len(extra_columns) == len(extra_column_names), set(extra_column_names) - {column.name for column in
                                                                                         extra_columns}

        deid_schema = StructType([*DEID_SCHEMA.fields, *extra_columns])
        df_out = spark.createDataFrame([], deid_schema)

        for field_name, deid_type in _DE_ID_EPMAWS2.items():
            df_out = df_out.union(
                df_in
                    .select(field_name, *extra_column_names)
                    .withColumnRenamed(field_name, 'clear')
                    .filter(col("clear").isNotNull() & (col("clear").cast('string') != "") & (col("clear") != lit('Removed')))
                    .withColumn("correlation_id", expr("uuid()"))
                    .withColumn("pseudo_type", lit(deid_type))
                    .select('correlation_id', 'clear', 'pseudo_type', *extra_column_names)
            )

        df_out = df_out.dropDuplicates(['clear', 'pseudo_type', *extra_column_names])
        return df_out
