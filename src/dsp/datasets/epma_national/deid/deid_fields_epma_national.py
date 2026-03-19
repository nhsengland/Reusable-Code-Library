from typing import Iterable

from dsp.integration.de_identify import NHS_NUMBER, PERSON_ID
from dsp.pipeline.deidentify.deidentify import DeidFields
from pyspark.sql import SparkSession, DataFrame
from dsp.datasets.epma_national.constants import (
    EPMANationalPrescriptionDerivedColumns,
    EPMARedactionValues
)

from pyspark.sql.functions import col, lit

_DE_IDENTIFICATIONS = dict([
    (EPMANationalPrescriptionDerivedColumns.NHS_NUMBER_LEGALLY_RESTRICTED, NHS_NUMBER),
    (EPMANationalPrescriptionDerivedColumns.PERSON_ID, PERSON_ID)
])


class DeidEpmaNational(DeidFields):
    def de_id_fields(self, spark: SparkSession, df_in: DataFrame,
                     extra_column_names: Iterable[str] = frozenset()) -> DataFrame:

        df_in = df_in.filter(col(EPMANationalPrescriptionDerivedColumns.NHS_NUMBER_LEGALLY_RESTRICTED) != lit(EPMARedactionValues.REMOVED))

        df_out = super().deid_out_data(
            spark=spark, df_in=df_in, _de_identifications=_DE_IDENTIFICATIONS,
            extra_column_names=extra_column_names)

        return df_out
