from dsp.pipelines.mps_check import MPSCheckStage
from dsp.datasets.sgss.enrichments.sgss_delta_enrich_mps import enrich_sgss_with_pds_mps
from dsp.datasets.sgss.stages.common.deid.deid_fields import DeidFieldsSGSSCommon
from dsp.datasets.sgss.stages.common.sgss_collect_mps_response import SGSSCollectMPSResponseStage
from dsp.datasets.sgss.stages.common.sgss_filter_mps_include import SGSSFilterMPSIncludeStage
from dsp.datasets.sgss.stages.common.sgss_data_writer import SGSSDataWriterStage
from dsp.datasets.sgss.stages.common.sgss_map_record_to_country import SGSSMapRecordToCountryStage
from dsp.datasets.sgss.stages.common.sgss_map_result_to_description import SGSSMapResultToDescriptionStage
from dsp.datasets.sgss.stages.common.sgss_mps_enrich_stage import SGSSMPSEnrichStage
from dsp.datasets.sgss.stages.common.sgss_pds_cross_check import SGSSPDSCrossCheckStage
from dsp.datasets.sgss.stages.common.sgss_pds_enhancement import SGSSPDSEnhancementStage
from dsp.datasets.sgss.stages.common.sgss_queue_delta_extract import SGSSQueueDeltaExtractStage
from dsp.datasets.sgss.stages.common.sgss_reject_on_duplicates import SGSSRejectOnDuplicatesStage
from dsp.datasets.sgss.stages.common.sgss_reject_on_non_enriched import SGSSRejectOnNonEnrichedStage
from dsp.datasets.sgss.stages.common.sgss_uplift_and_derive import SGSSUpliftAndDeriveStage
from dsp.datasets.sgss.stages.sgss_delta.sgss_delta_add_keystone_flag import SGSSDeltaAddKeystoneFlagStage
from dsp.datasets.sgss.stages.sgss_delta.sgss_delta_queue_keystone_extract import SGSSDeltaQueueKeystoneExtractStage
from dsp.pipeline.stages.deidentify import DeIdentifyStage
from dsp.pipeline.stages.feature_toggle_suspend import FeatureToggleSuspendStage
from dsp.pipeline.stages.loader import CSVLoaderPipelineStage
from dsp.pipeline.stages import AddTraceabilityColumnsStage
from dsp.pipeline.stages.trim_column_contents import TrimColumnContentsStage
from dsp.pipeline.stages.total_records import TotalRecordsStage
from dsp.shared.aws import ssm_parameter, get_feature_toggle

from dsp.shared.constants import DS, FeatureToggles, MPS_LOCAL_ID_PREFIXES

# Note: Any fields to be excluded from the main sgss.full_non_de_dupe asset should be added
#       to the sgss_excluded_fields list in src/dsp/datasets/sgss/delta_merge/main.py

SGSS_DELTA_DERIVED_DF = "sgss_delta_derived_df"
SGSS_DELTA_MPS_INPUT_DF = "sgss_delta_mps_input_df"
SGSS_DELTA_PDS_MATCHED_DF = "sgss_delta_pds_matched_df"
SGSS_DELTA_PDS_UNMATCHED_DF = "sgss_delta_pds_unmatched_df"
SGSS_DELTA_PDS_MPS_MATCHED_DF = "sgss_delta_pds_mps_matched_df"
SGSS_DELTA_NULL_PERSON_ID_TO_BLANK_DF = "sgss_delta_null_person_id_to_blank_df"
SGSS_DELTA_POST_PROCESSING_FIELDS_DF = "sgss_delta_post_processing_fields_df"
SGSS_DELTA_TEST_DESC_MAPPED_DF = "sgss_delta_test_desc_mapped_df"
SGSS_DELTA_PDS_ENHANCED_DF = "sgss_delta_pds_enhanced_df"
SGSS_DELTA_PDS_ENHANCED_KEYSTONE_DF = "sgss_delta_pds_enhanced_keystone_df"

TARGET_TABLE_NAME = "sgss.full_non_de_dupe"

STAGES = [
    CSVLoaderPipelineStage(output_dataframe_name=DS.SGSS_DELTA, add_row_number=False),

    TotalRecordsStage(key_name='total_submitted_records', dataframe_names={DS.SGSS_DELTA}),

    TrimColumnContentsStage(input_dataframe_name=DS.SGSS_DELTA),
    SGSSRejectOnDuplicatesStage(input_dataframe_name=DS.SGSS_DELTA),
    AddTraceabilityColumnsStage(trace_dataframes={DS.SGSS_DELTA}),
    SGSSUpliftAndDeriveStage(input_dataframe_name=DS.SGSS_DELTA, output_dataframe_name=SGSS_DELTA_DERIVED_DF),
    SGSSFilterMPSIncludeStage(input_dataframe_name=SGSS_DELTA_DERIVED_DF,
                              output_dataframe_name=SGSS_DELTA_MPS_INPUT_DF,
                              passthrough_dataframes={SGSS_DELTA_DERIVED_DF}),

    # MPS/PDS stages
    FeatureToggleSuspendStage(feature_name=FeatureToggles.INGESTION_MPS,
                              passthrough_dataframes={SGSS_DELTA_MPS_INPUT_DF, SGSS_DELTA_DERIVED_DF}),
    SGSSPDSCrossCheckStage(trace_dataframe_name=SGSS_DELTA_MPS_INPUT_DF,
                           matched_output_dataframe_name=SGSS_DELTA_PDS_MATCHED_DF,
                           unmatched_output_dataframe_name=SGSS_DELTA_PDS_UNMATCHED_DF,
                           passthrough_dataframes={SGSS_DELTA_DERIVED_DF}),

    # Note that mailbox_from is overwritten as to override the MPS steps to PDS checks only
    MPSCheckStage(trace_dataframe_name=SGSS_DELTA_MPS_INPUT_DF, matched_output_dataframe_name=SGSS_DELTA_PDS_MATCHED_DF,
                  unmatched_output_dataframe_name=SGSS_DELTA_PDS_UNMATCHED_DF, local_id_prefix=MPS_LOCAL_ID_PREFIXES.SGSS_DELTA,
                  mailbox_from=lambda:
                  ssm_parameter('/core/mps_core_sender_mailbox') if get_feature_toggle(FeatureToggles.SGSS_DELTA_RUN_FULL_MPS)
                  else ssm_parameter('/core/pds_only_mps_core_sender_mailbox'),
                  passthrough_dataframes={SGSS_DELTA_DERIVED_DF}),

    SGSSCollectMPSResponseStage(matched_dataframe_name=SGSS_DELTA_PDS_MATCHED_DF,
                                unmatched_dataframe_name=SGSS_DELTA_PDS_UNMATCHED_DF,
                                mps_result_dataframe_name=SGSS_DELTA_PDS_MPS_MATCHED_DF,
                                passthrough_dataframe_names={SGSS_DELTA_DERIVED_DF}),

    SGSSMPSEnrichStage(enrichments=[enrich_sgss_with_pds_mps],
                       trace_dataframe_name=SGSS_DELTA_DERIVED_DF,
                       mps_result_dataframe_name=SGSS_DELTA_PDS_MPS_MATCHED_DF),

    SGSSRejectOnNonEnrichedStage(input_dataframe_name=SGSS_DELTA_DERIVED_DF,
                                 output_dataframe_name=SGSS_DELTA_NULL_PERSON_ID_TO_BLANK_DF),
    DeIdentifyStage(de_identified_dataframe_name=SGSS_DELTA_NULL_PERSON_ID_TO_BLANK_DF,
                    deid_fields=DeidFieldsSGSSCommon()),

    SGSSMapRecordToCountryStage(input_dataframe_name=SGSS_DELTA_NULL_PERSON_ID_TO_BLANK_DF,
                                output_dataframe_name=SGSS_DELTA_POST_PROCESSING_FIELDS_DF),
    SGSSMapResultToDescriptionStage(input_dataframe_name=SGSS_DELTA_POST_PROCESSING_FIELDS_DF,
                                    output_dataframe_name=SGSS_DELTA_TEST_DESC_MAPPED_DF),

    SGSSPDSEnhancementStage(input_dataframe_name=SGSS_DELTA_TEST_DESC_MAPPED_DF,
                            output_dataframe_name=SGSS_DELTA_PDS_ENHANCED_DF),

    SGSSDeltaAddKeystoneFlagStage(input_dataframe_name=SGSS_DELTA_PDS_ENHANCED_DF,
                                  output_dataframe_name=SGSS_DELTA_PDS_ENHANCED_KEYSTONE_DF),

    SGSSDataWriterStage(input_dataframe_name=SGSS_DELTA_PDS_ENHANCED_KEYSTONE_DF),

    SGSSQueueDeltaExtractStage(country_categories_ssm_parameter='/core/sgss_delta_extract_country_categories'),
    SGSSDeltaQueueKeystoneExtractStage()
]
