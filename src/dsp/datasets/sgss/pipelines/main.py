from dsp.pipelines.pipeline import PipelineKey
from dsp.datasets.sgss.pipelines.sgss.main import STAGES as SGSS_STAGES
from dsp.datasets.sgss.pipelines.sgss_delta.main import STAGES as SGSS_DELTA_STAGES
from dsp.pipeline.pipeline import Pipeline
from dsp.shared.constants import DS


def get_dataset_pipelines():
    return {
        PipelineKey.init(DS.SGSS): Pipeline(SGSS_STAGES,
                                            undefined_suspension_stage_handler=lambda: 'feature_toggle_ingestion_mps'),
        PipelineKey.init(DS.SGSS_DELTA): Pipeline(SGSS_DELTA_STAGES,
                                            undefined_suspension_stage_handler=lambda: 'feature_toggle_ingestion_mps'),
    }
