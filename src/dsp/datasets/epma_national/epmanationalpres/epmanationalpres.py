from dsp.datasets.epma_national.epmanationalpres.output import Order as epmaprescorder # fix the order as it's currently out of order
from dsp.datasets.models.epmanationalpres import _EPMAPrescriptionModel
from dsp.datasets.epma_national.constants import EPMANationalDatabases
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.shared.constants import DS, PATHS

DELTA_JOIN_COLUMNS = [
    str(_EPMAPrescriptionModel.PrescribedItemIdentifier),
    str(_EPMAPrescriptionModel.OrganisationSiteIdentifierOfTreatment)
]

DELTA_MERGES = (
    DeltaMergeParameters(
        DS.EPMANATIONALPRES,
        PATHS.OUT,
        f'{EPMANationalDatabases.EPMA_NATIONAL}.{DS.EPMANATIONALPRES}',
        DELTA_JOIN_COLUMNS,
        epmaprescorder
    ),
)
