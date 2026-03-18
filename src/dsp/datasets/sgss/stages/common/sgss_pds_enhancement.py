from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class SGSSPDSEnhancementStage(PipelineStage):
    """
    Enhances a given SGSS record with PDS information.
    """
    name = "sgss_pds_enhancement"

    def __init__(self, input_dataframe_name: str, output_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={output_dataframe_name},
        )
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    @staticmethod
    def _enrich_with_pds(spark: SparkSession, df: DataFrame) -> DataFrame:
         # Exclude Invalid (I), Sensitive (S) and Legacy (Y) records.
        df_valid_pds = spark.table('pds.pds').filter(
            expr("""
                    CASE WHEN parsed_record.confidentiality IS NULL THEN TRUE
                    ELSE
                        size(
                          filter(
                              parsed_record.confidentiality,
                              x -> x.val IN ('I','S','Y') 
                                   AND x.from <= int(date_format(current_date(),'yyyyMMdd')) 
                                   AND (x.to IS NULL OR int(date_format(current_date(),'yyyyMMdd')) < x.to)
                          )
                        ) = 0
                    END
            """)
        )

        contact_fields = [
            "p.date_of_death AS date_of_death",
            "p.death_status AS death_status",
            "concat(array_join(p.address.lines, ', '), ', ', p.address.postcode) as address",
            "p.emailAddress AS email_address",
            "p.mobilePhone AS mobile_phone",
            "p.telephone AS telephone",
            "p.gp.code AS gp_code"
        ]

        df = df.alias('s').join(df_valid_pds.alias('p'),
                                on=(df[Fields.PERSON_ID] == df_valid_pds['nhs_number']),
                                how='left') \
            .select([col(column) for column in df.columns]
                    + [expr(contact_field) for contact_field in contact_fields])

        return df

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        df = self._enrich_with_pds(spark, df)

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(df)

        return context
