from dsp.datasets.models.epmawspc import EPMAWellSkyPrescriptionModel, _EPMAWellSkyPrescriptionModel
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.shared.constants import DS, PATHS

DELTA_JOIN_COLUMNS = [str(_EPMAWellSkyPrescriptionModel.ODS),
                      str(_EPMAWellSkyPrescriptionModel.ReportingPeriodStartDate),
                      str(_EPMAWellSkyPrescriptionModel.ReportingPeriodEndDate)]

DELTA_MERGES = (
    DeltaMergeParameters(
        DS.EPMAWSPC,
        PATHS.OUT,  # Source directory in this context?
        'epma.{dataset_id}'.format(dataset_id=DS.EPMAWSPC),
        DELTA_JOIN_COLUMNS,
        EPMAWellSkyPrescriptionModel.get_field_names()
    ),
)
