from typing import Any, AbstractSet
from dsp.datasets.sgss.stages.sgss_delta.sgss_delta_extract_request import request_sgss_delta_extract
from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage
from pyspark.sql import SparkSession

from dsp.shared.aws import ssm_parameter, get_feature_toggle
from dsp.shared.constants import FeatureToggles, MESH_WORKFLOW_ID
from dsp.shared.logger import add_fields


class SGSSQueueDeltaExtractStage(PipelineStage):
    """
    Queues a delta for the current submission
    """
    name = 'sgss_queue_delta_extract'

    def __init__(self, country_categories_ssm_parameter: str, passthrough_dataframes: AbstractSet[str] = frozenset()):
        self.country_categories_ssm_parameter = country_categories_ssm_parameter
        super(SGSSQueueDeltaExtractStage, self).__init__(name=self.name,
                                                         provided_output_dataframes=passthrough_dataframes)

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        submission_workflow_id = context.primitives['request']['extra']['workflow_id']

        if submission_workflow_id == MESH_WORKFLOW_ID.PHE_COVID19_P1_HISTORY:
            return context

        if get_feature_toggle(FeatureToggles.SGSS_SEND_DELTA_EXTRACTS):
            submission_working_folder = context.working_folder
            submission_id = context.primitives['submission_id']
            submission_workflow_id = context.primitives['request']['extra']['workflow_id']
            submission_file_name = context.primitives['request']['extra']['filename']
            submission_local_id = context.primitives['request']['extra']['local_id']
            submission_sender_mailbox = context.primitives['request']['extra'].get('sending_mailbox', 'none')
            country_categories = [country_category.strip() for country_category in
                                  ssm_parameter(self.country_categories_ssm_parameter).split(',')]

            extract_id = request_sgss_delta_extract(submission_id, submission_working_folder, submission_file_name,
                                                    submission_workflow_id, submission_sender_mailbox,
                                                    submission_local_id, country_categories)

            add_fields(submission_id=submission_id, extract_id=extract_id)

            if not context.primitives.get('extract_ids', None):
                context.primitives['extract_ids'] = {}
            context.primitives['extract_ids']['delta'] = extract_id

        return context
