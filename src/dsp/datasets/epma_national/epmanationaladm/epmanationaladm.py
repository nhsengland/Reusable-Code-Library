from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.datasets.epma_national.epmanationaladm.output import Order as epmaadmorder
from dsp.datasets.epma_national.constants import EPMANationalDatabases
from dsp.datasets.models.epmanationaladm import _EPMAAdministrationModel
from dsp.shared.constants import DS, PATHS


DELTA_JOIN_COLUMNS = [
    str(_EPMAAdministrationModel.MedicationAdminIdentifier),
    str(_EPMAAdministrationModel.OrganisationSiteIdentifierOfTreatment)
]

DELTA_MERGES = (
    DeltaMergeParameters(
        DS.EPMANATIONALADM,
        PATHS.OUT,
        f'{EPMANationalDatabases.EPMA_NATIONAL}.{DS.EPMANATIONALADM}',
        DELTA_JOIN_COLUMNS,
        epmaadmorder
    ),
)
