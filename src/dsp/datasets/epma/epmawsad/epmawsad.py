from dsp.datasets.models.epmawsad import EPMAWellSkyAdministrationModel, _EPMAWellSkyAdministrationModel
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.shared.constants import DS, PATHS

DELTA_JOIN_COLUMNS = [str(_EPMAWellSkyAdministrationModel.ODS),
                      str(_EPMAWellSkyAdministrationModel.ReportingPeriodStartDate),
                      str(_EPMAWellSkyAdministrationModel.ReportingPeriodEndDate)]

DELTA_MERGES = (
    DeltaMergeParameters(
        DS.EPMAWSAD,
        PATHS.OUT,  # Source directory in this context?
        'epma.{dataset_id}'.format(dataset_id=DS.EPMAWSAD),
        DELTA_JOIN_COLUMNS,
        EPMAWellSkyAdministrationModel.get_field_names()
    ),
)
