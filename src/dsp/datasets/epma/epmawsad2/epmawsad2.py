from dsp.datasets.models.epmawsad2 import EPMAWellSkyAdministrationModel2, _EPMAWellSkyAdministrationModel2
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.shared.constants import DS, PATHS

EXCLUDED_FIELDS = [str(_EPMAWellSkyAdministrationModel2.NHS), str(_EPMAWellSkyAdministrationModel2.NHSlrRemoved)]
DELTA_JOIN_COLUMNS = [str(_EPMAWellSkyAdministrationModel2.AdministrationPrimaryKey)]

DELTA_MERGES = (
    DeltaMergeParameters(
        DS.EPMAWSAD2,
        PATHS.OUT,
        'epma.{dataset_id}'.format(dataset_id=DS.EPMAWSAD2),
        DELTA_JOIN_COLUMNS,
        sorted(field for field in EPMAWellSkyAdministrationModel2.get_field_names() if field not in EXCLUDED_FIELDS)
    ),
)
