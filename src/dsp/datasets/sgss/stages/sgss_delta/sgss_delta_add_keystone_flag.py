from typing import Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, upper, substring, to_timestamp, to_date

from dsp.datasets.sgss.ingestion.sgss.config import Fields, PostprocessingFields, INDETERMINATE_ORGANISM_SPECIES_NAME
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage
from dsp.shared.aws import ssm_parameter

#from dsp.datasets.covid19_testing.schema.shared.util import clean_email, clean_uppercase_transform, clean_mobile_number, \
#    clean_gender
#from dsp.datasets.covid19_testing.pipelines.covid19_testing_common.common import normalise_postcode


class SGSSDeltaAddKeystoneFlagStage(PipelineStage):
    """
    Adds a SendToKeystone flag field to the final DataFrame to denote whether a given record should be picked up for
    sending to Keystone.
    """
    name = 'sgss_delta_add_keystone_flag'

    def __init__(self, input_dataframe_name: str, output_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={output_dataframe_name},
        )
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    @staticmethod
    def _remove_invalid_mps_records(df: DataFrame) -> DataFrame:
        non_null_df = df.filter((col("MPSConfidence").isNotNull()))

        return non_null_df.filter(
            (
                    (col("MPSConfidence")['MATCHED_ALGORITHM_INDICATOR'] == '3') &
                    (
                            (col(PostprocessingFields.NormalisedPostcode).isNotNull()) &
                            (col(PostprocessingFields.NormalisedPostcode) != '') &
                            ((col(PostprocessingFields.NormalisedPostcode) ==
                              normalise_postcode("MPSPOSTCODE")) |
                             (col("MPSConfidence")['ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC'] == lit(100.00)))
                    )
            ) |
            (
                    (
                            (col("MPSConfidence")['MATCHED_ALGORITHM_INDICATOR'] == '1') |
                            (col("MPSConfidence")['MATCHED_ALGORITHM_INDICATOR'] == '4')
                    ) &
                    (
                            (col(Fields.Patient_Forename).isNotNull()) &
                            (col(Fields.Patient_Forename) != '') &
                            (substring(clean_uppercase_transform(Fields.Patient_Forename), 1, 3) ==
                             substring(clean_uppercase_transform("MPSGIVEN_NAME"), 1, 3))
                    ) &
                    (
                            (col(Fields.Patient_Surname).isNotNull()) &
                            (col(Fields.Patient_Surname) != '') &
                            (clean_uppercase_transform(Fields.Patient_Surname) ==
                             clean_uppercase_transform("MPSFAMILY_NAME"))
                    ) &
                    (
                            ((upper(col(Fields.Patient_Sex)) == 'MALE') | (
                                        upper(col(Fields.Patient_Sex)) == 'FEMALE')) &
                            (clean_gender(Fields.Patient_Sex) == col("MPSGENDER"))
                    ) &
                    (
                            (col(Fields.Patient_Date_Of_Birth).isNotNull()) &
                            (col(Fields.Patient_Date_Of_Birth) == to_date(col("MPSDATE_OF_BIRTH").cast("string"),
                                                                          'yyyyMMdd'))
                    ) &
                    (
                            (
                                    (col(PostprocessingFields.NormalisedPostcode).isNotNull()) &
                                    (col(PostprocessingFields.NormalisedPostcode) != '') &
                                    ((col(PostprocessingFields.NormalisedPostcode) ==
                                      normalise_postcode("MPSPOSTCODE")) |
                                     (col("MPSConfidence")['ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC'] == lit(100.00)))
                            ) |
                            (
                                    (col(Fields.P2_email).isNotNull()) &
                                    (col(Fields.P2_email) != '') &
                                    (clean_email(Fields.P2_email) == clean_email(
                                        "MPSEMAIL_ADDRESS"))
                            ) |
                            (
                                    (col(Fields.P2_mobile).isNotNull()) &
                                    (col(Fields.P2_mobile) != '') &
                                    (clean_mobile_number(Fields.P2_mobile) == clean_mobile_number(
                                        "MPSMOBILE_NUMBER"))
                            )
                    )
            )
        )

    @staticmethod
    def _add_keystone_flag(input_df: DataFrame) -> DataFrame:
        allowed_test_types = [test_type.strip() for test_type
                              in ssm_parameter('/core/sgss_delta_keystone/test_types').split(',')]
        pillar_1_only = (upper(col(Fields.Lab_Pillar)) == lit('PILLAR 1'))
        test_type_filter = ((col(Fields.test_type).isNotNull())
                            & (col(Fields.test_type).isin(allowed_test_types)))
        exculde_non_positive_lft = (
            ~((col(Fields.test_type).isin(11, 25)) & (col(Fields.test_result) != lit('SARS-CoV-2-ANGY'))))
        exclude_ons_react = ((upper(col(Fields.Reporting_Lab)) != lit('REACT'))
                             & (upper(col(Fields.Reporting_Lab)) != lit('IQVIA')))
        gp_code_not_null = (col(Fields.gp_code).isNotNull())
        gp_code_not_welsh = (~(upper(col(Fields.gp_code)).startswith('W')))
        person_id_not_null = (col(Fields.PERSON_ID).isNotNull())

        matched_algorithm_filter = (col(Fields.MPSConfidence)['MATCHED_ALGORITHM_INDICATOR'].isin(1, 3, 4))
        matched_algorithm_perc_filter = (col(Fields.MPSConfidence)['MATCHED_CONFIDENCE_PERCENTAGE'] == lit(100.00))

        non_null_pdscrosscheck_filter = (col(Fields.PdsCrossCheckCondition).isNotNull())
        non_mps_pdscrosscheck_filter = (upper(col(Fields.PdsCrossCheckCondition)) != lit('MPS'))
        non_null_or_mps_pdscrosscheck_filter = (non_null_pdscrosscheck_filter & non_mps_pdscrosscheck_filter)

        pds_match_filter = (
                non_null_or_mps_pdscrosscheck_filter | (matched_algorithm_filter & matched_algorithm_perc_filter))

        non_null_test_result = (col(Fields.test_result).isNotNull())

        should_send_to_keystone = (
                    pillar_1_only & test_type_filter & exculde_non_positive_lft & exclude_ons_react & gp_code_not_null
                    & gp_code_not_welsh & person_id_not_null & pds_match_filter & non_null_test_result)

        return input_df.withColumn(Fields.SendToKeystone, should_send_to_keystone)

    def _join_on_keystone_record(self, df: DataFrame):
        keystone_specimens_df = self._remove_invalid_mps_records(df.filter("SendToKeystone = true"))

        valid_mps_df = keystone_specimens_df.select('META')

        non_keystone_specimens_df = df.alias('lhs').join(valid_mps_df.alias('rhs'),
                                                         on=col('lhs.META.EVENT_ID') == col('rhs.META.EVENT_ID'),
                                                         how='left_anti') \
            .select(*df.schema.names).withColumn(Fields.SendToKeystone, lit(False))

        return non_keystone_specimens_df.union(keystone_specimens_df)

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df
        df = self._add_keystone_flag(df)

        df = self._join_on_keystone_record(df)

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(df)

        return context
