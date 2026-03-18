from typing import Any, AbstractSet

from dsp.datasets.sgss.stages.sgss_delta.sgss_delta_keystone_extract_request import request_sgss_delta_keystone_extract
from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage
from pyspark.sql import SparkSession

from dsp.shared.aws import get_feature_toggle
from dsp.shared.constants import FeatureToggles, MESH_WORKFLOW_ID
from dsp.shared.logger import add_fields


class SGSSDeltaQueueKeystoneExtractStage(PipelineStage):
    """
    Queues a Keystone extract for the given sgss_delta submission
    """
    name = 'sgss_delta_queue_keystone_extract'

    def __init__(self, passthrough_dataframes: AbstractSet[str] = frozenset()):
        super().__init__(name=self.name, provided_output_dataframes=passthrough_dataframes)

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        if context.primitives['request']['extra']['workflow_id'] == MESH_WORKFLOW_ID.PHE_COVID19_P1_HISTORY:
            return context

        if get_feature_toggle(FeatureToggles.SGSS_DELTA_SEND_KEYSTONE_EXTRACTS):
            submission_working_folder = context.working_folder
            submission_id = context.primitives['submission_id']

            extract_id = request_sgss_delta_keystone_extract(submission_id, submission_working_folder)

            add_fields(submission_id=submission_id, extract_id=extract_id)

            if not context.primitives.get('extract_ids', None):
                context.primitives['extract_ids'] = {}
            context.primitives['extract_ids']['keystone'] = extract_id

        return context
