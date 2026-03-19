from typing import Any

from itertools import chain
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import create_map, lit, col, when, current_date

from dsp.datasets.postcode.common import normalised_postcode
from dsp.datasets.sgss.ingestion.sgss.config import Fields, PostprocessingFields, ExtractCountry
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class SGSSMapRecordToCountryStage(PipelineStage):
    """
    Maps a given SGSS record to its corresponding country based on the Patient_PostCode provided.
    This mapping is additionally logged under sgss.record_to_country for auditing purposes.
    """
    name = "sgss_map_record_to_country"

    AUDIT_TABLE = 'sgss.record_to_country'

    def __init__(self, input_dataframe_name: str, output_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={output_dataframe_name},
        )
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    @staticmethod
    def _add_normalised_postcode(df):
        return normalised_postcode(df, Fields.Patient_PostCode, PostprocessingFields.NormalisedPostcode)

    @staticmethod
    def _add_postcode_country(spark: SparkSession, df: DataFrame) -> DataFrame:
        ctry_to_code = {'E92000001': ExtractCountry.ENGLAND, 'S92000003': ExtractCountry.SCOTLAND,
                        'W92000004': ExtractCountry.WALES, 'N92000002': ExtractCountry.NORTHERN_IRELAND}
        ctry_to_code_map = create_map([lit(kv) for kv in chain(*ctry_to_code.items())])

        df_refdata_postcode = spark.table('dss_corporate.postcode').where(col('record_end_date').isNull())
        df_postcode_to_country = df.alias('lhs').join(df_refdata_postcode.alias('rhs'),
                                                      on=(df[PostprocessingFields.NormalisedPostcode] ==
                                                          df_refdata_postcode['pcds']),
                                                      how='inner') \
            .withColumn('PostcodeCountry', when(ctry_to_code_map.getItem(col('CTRY')).isNotNull(),
                                                ctry_to_code_map.getItem(col('CTRY'))).otherwise(lit(ExtractCountry.UNMATCHED))) \
            .select([f'lhs.{col_name}' for col_name in df.columns] + [PostprocessingFields.PostcodeCountry])

        df_unmatched_postcode = df.alias('lhs') \
            .join(df_postcode_to_country.alias('rhs'),
                  on=(col('lhs.META.EVENT_ID') == col('rhs.META.EVENT_ID')),
                  how='left_anti').withColumn(PostprocessingFields.PostcodeCountry, lit(ExtractCountry.UNMATCHED))

        return df_postcode_to_country.union(df_unmatched_postcode)

    def _append_in_country_to_record_audit_table(self, df: DataFrame):
        audit_df = df.select(['META.EVENT_ID', PostprocessingFields.PostcodeCountry])
        audit_df = audit_df.withColumn('processing_date', current_date())
        audit_df.write.format("delta").mode('append').saveAsTable(self.AUDIT_TABLE)

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        df = self._add_normalised_postcode(df)
        df = self._add_postcode_country(spark, df)

        self._append_in_country_to_record_audit_table(df)

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(df)

        return context
