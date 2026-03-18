from typing import Iterable

from dsp.integration.de_identify import NHS_NUMBER, PERSON_ID
from dsp.pipelines.deidentify.deidentify import DeidFields
from pyspark.sql import SparkSession, DataFrame

_DE_IDENTIFICATIONS = {
    'Patient_NHS_Number': NHS_NUMBER,
    'PERSON_ID': PERSON_ID
}


class DeidFieldsSGSSCommon(DeidFields):
    def de_id_fields(self, spark: SparkSession, df_in: DataFrame,
                     extra_column_names: Iterable[str] = frozenset()) -> DataFrame:

        df_out = super().deid_out_data(
            spark=spark, df_in=df_in, _de_identifications=_DE_IDENTIFICATIONS,
            extra_column_names=extra_column_names)

        return df_out
