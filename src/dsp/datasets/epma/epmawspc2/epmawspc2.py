from dsp.datasets.models.epmawspc2 import EPMAWellSkyPrescriptionModel2, _EPMAWellSkyPrescriptionModel2
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.shared.constants import DS, PATHS

EXCLUDED_FIELDS = [str(_EPMAWellSkyPrescriptionModel2.NHS), str(_EPMAWellSkyPrescriptionModel2.NHSlrRemoved)]

DELTA_JOIN_COLUMNS = [str(_EPMAWellSkyPrescriptionModel2.PrescriptionPrimaryKey)]

DELTA_MERGES = (
    DeltaMergeParameters(
        DS.EPMAWSPC2,
        PATHS.OUT,
        'epma.{dataset_id}'.format(dataset_id=DS.EPMAWSPC2),
        DELTA_JOIN_COLUMNS,
        sorted(field for field in EPMAWellSkyPrescriptionModel2.get_field_names() if
         field not in EXCLUDED_FIELDS)
    ),
)
