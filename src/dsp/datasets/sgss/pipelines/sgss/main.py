from dsp.pipelines.mps_check import MPSCheckStage
from dsp.datasets.sgss.enrichments.sgss_enrich_mps import enrich_sgss_with_pds_mps
from dsp.datasets.sgss.stages.common.deid.deid_fields import DeidFieldsSGSSCommon
from dsp.datasets.sgss.stages.common.sgss_filter_mps_include import SGSSFilterMPSIncludeStage
from dsp.datasets.sgss.stages.common.sgss_data_writer import SGSSDataWriterStage
from dsp.datasets.sgss.stages.common.sgss_map_record_to_country import SGSSMapRecordToCountryStage
from dsp.datasets.sgss.stages.common.sgss_map_result_to_description import SGSSMapResultToDescriptionStage
from dsp.datasets.sgss.stages.common.sgss_mps_enrich_stage import SGSSMPSEnrichStage
from dsp.datasets.sgss.stages.common.sgss_queue_delta_extract import SGSSQueueDeltaExtractStage
from dsp.datasets.sgss.stages.common.sgss_pds_enhancement import SGSSPDSEnhancementStage
from dsp.datasets.sgss.stages.common.sgss_reject_on_duplicates import SGSSRejectOnDuplicatesStage
from dsp.datasets.sgss.stages.common.sgss_reject_on_non_enriched import SGSSRejectOnNonEnrichedStage
from dsp.datasets.sgss.stages.common.sgss_uplift_and_derive import SGSSUpliftAndDeriveStage
from dsp.datasets.sgss.stages.sgss.sgss_filter_non_positives import SGSSFilterNonPositivesStage
from dsp.pipeline.stages.collect_mps_response import CollectMPSResponseStage
from dsp.pipeline.stages.deidentify import DeIdentifyStage
from dsp.pipeline.stages.feature_toggle_suspend import FeatureToggleSuspendStage
from dsp.pipeline.stages.loader import CSVLoaderPipelineStage
from dsp.pipeline.stages import AddTraceabilityColumnsStage
from dsp.pipeline.stages.trim_column_contents import TrimColumnContentsStage
from dsp.pipeline.stages.pds_cross_check import PDSCrossCheckStage
from dsp.pipeline.stages.total_records import TotalRecordsStage
from dsp.shared.aws import ssm_parameter, get_feature_toggle

from dsp.shared.constants import DS, FeatureToggles, MPS_LOCAL_ID_PREFIXES

# Note: Any fields to be excluded from the main sgss.full_non_de_dupe asset should be added
#       to the sgss_excluded_fields list in src/dsp/datasets/sgss/delta_merge/main.py

SGSS_DERIVED_DF = "sgss_derived_df"
SGSS_POSITIVES_ONLY_DF = "sgss_positives_only_df"
SGSS_MPS_INPUT_DF = "sgss_mps_input_df"
SGSS_PDS_MATCHED_DF = "sgss_pds_matched_df"
SGSS_PDS_UNMATCHED_DF = "sgss_pds_unmatched_df"
SGSS_PDS_MPS_MATCHED_DF = "sgss_pds_mps_matched_df"
SGSS_NULL_PERSON_ID_TO_BLANK_DF = "sgss_null_person_id_to_blank_df"
SGSS_POST_PROCESSING_FIELDS_DF = "sgss_post_processing_fields_df"
SGSS_TEST_DESC_MAPPED_DF = "sgss_test_desc_mapped_df"
SGSS_PDS_ENHANCED_DF = "sgss_pds_enhanced_df"

TARGET_TABLE_NAME = "sgss.sgss"

STAGES = [
    CSVLoaderPipelineStage(output_dataframe_name=DS.SGSS, add_row_number=False),

    TotalRecordsStage(key_name='total_submitted_records', dataframe_names={DS.SGSS}),

    TrimColumnContentsStage(input_dataframe_name=DS.SGSS),
    SGSSRejectOnDuplicatesStage(input_dataframe_name=DS.SGSS),
    AddTraceabilityColumnsStage(trace_dataframes={DS.SGSS}),
    SGSSUpliftAndDeriveStage(input_dataframe_name=DS.SGSS, output_dataframe_name=SGSS_DERIVED_DF),
    SGSSFilterNonPositivesStage(input_dataframe_name=SGSS_DERIVED_DF,
                                output_dataframe_name=SGSS_POSITIVES_ONLY_DF),
    SGSSFilterMPSIncludeStage(input_dataframe_name=SGSS_POSITIVES_ONLY_DF,
                              output_dataframe_name=SGSS_MPS_INPUT_DF,
                              passthrough_dataframes={SGSS_POSITIVES_ONLY_DF}),

    # MPS/PDS stages
    FeatureToggleSuspendStage(feature_name=FeatureToggles.INGESTION_MPS,
                              passthrough_dataframes={SGSS_MPS_INPUT_DF, SGSS_POSITIVES_ONLY_DF}),
    PDSCrossCheckStage(trace_dataframe_name=SGSS_MPS_INPUT_DF, matched_output_dataframe_name=SGSS_PDS_MATCHED_DF,
                       unmatched_output_dataframe_name=SGSS_PDS_UNMATCHED_DF,
                       passthrough_dataframes={SGSS_POSITIVES_ONLY_DF}),

    # Note that mailbox_from is overwritten as to override the MPS steps to PDS checks only
    MPSCheckStage(trace_dataframe_name=SGSS_MPS_INPUT_DF, matched_output_dataframe_name=SGSS_PDS_MATCHED_DF,
                  unmatched_output_dataframe_name=SGSS_PDS_UNMATCHED_DF, local_id_prefix=MPS_LOCAL_ID_PREFIXES.SGSS,
                  mailbox_from=lambda:
                  ssm_parameter('/core/mps_core_sender_mailbox') if get_feature_toggle(FeatureToggles.SGSS_RUN_FULL_MPS)
                  else ssm_parameter('/core/pds_only_mps_core_sender_mailbox'),
                  passthrough_dataframes={SGSS_POSITIVES_ONLY_DF}),

    CollectMPSResponseStage(matched_dataframe_name=SGSS_PDS_MATCHED_DF,
                            unmatched_dataframe_name=SGSS_PDS_UNMATCHED_DF,
                            mps_result_dataframe_name=SGSS_PDS_MPS_MATCHED_DF,
                            passthrough_dataframe_names={SGSS_POSITIVES_ONLY_DF}),

    SGSSMPSEnrichStage(enrichments=[enrich_sgss_with_pds_mps],
                       trace_dataframe_name=SGSS_POSITIVES_ONLY_DF, mps_result_dataframe_name=SGSS_PDS_MPS_MATCHED_DF),

    SGSSRejectOnNonEnrichedStage(input_dataframe_name=SGSS_POSITIVES_ONLY_DF,
                                 output_dataframe_name=SGSS_NULL_PERSON_ID_TO_BLANK_DF),

    DeIdentifyStage(de_identified_dataframe_name=SGSS_NULL_PERSON_ID_TO_BLANK_DF,
                    deid_fields=DeidFieldsSGSSCommon()),

    SGSSMapRecordToCountryStage(input_dataframe_name=SGSS_NULL_PERSON_ID_TO_BLANK_DF,
                                output_dataframe_name=SGSS_POST_PROCESSING_FIELDS_DF),
    SGSSMapResultToDescriptionStage(input_dataframe_name=SGSS_POST_PROCESSING_FIELDS_DF,
                                    output_dataframe_name=SGSS_TEST_DESC_MAPPED_DF),

    SGSSPDSEnhancementStage(input_dataframe_name=SGSS_TEST_DESC_MAPPED_DF, output_dataframe_name=SGSS_PDS_ENHANCED_DF),

    SGSSDataWriterStage(input_dataframe_name=SGSS_PDS_ENHANCED_DF),

    SGSSQueueDeltaExtractStage(country_categories_ssm_parameter='/core/sgss_extract_country_categories')
]
