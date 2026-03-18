from pyspark.sql import DataFrame

from dsp.pipeline.stages.mps_enrich import MPSEnrichStage
from dsp.shared.aws import get_feature_toggle
from dsp.shared.constants import FeatureToggles


class SGSSMPSEnrichStage(MPSEnrichStage):
    @staticmethod
    def _validate_contents(dataframe: DataFrame):
        if not get_feature_toggle(FeatureToggles.SGSS_SOFT_FAIL_ON_RECORD_LEVEL_MPS_FAILURE):
            return MPSEnrichStage._validate_contents(dataframe)
