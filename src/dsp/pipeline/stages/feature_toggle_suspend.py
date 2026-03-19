from typing import AbstractSet, Any

from pyspark.sql.session import SparkSession

from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage
from dsp.shared.aws import get_toggle


class FeatureToggleSuspendStage(PipelineStage):

    def __init__(self, feature_name: str, passthrough_dataframes: AbstractSet[str] = frozenset()):
        super().__init__(name="feature_toggle_{}".format(feature_name),
                         required_input_dataframes=passthrough_dataframes,
                         provided_output_dataframes=passthrough_dataframes)
        self._feature_name = feature_name

    def _is_feature_enabled(self):
        return get_toggle('feature', self._feature_name)

    def _preconditions_met(self, spark: SparkSession, context: PipelineContext) -> bool:
        return self._is_feature_enabled()

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        return context
