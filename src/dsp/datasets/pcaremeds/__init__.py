from dsp.datasets.models.pcaremeds import PrimaryCareMedicineModel, _PrimaryCareMedicineModel
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.shared.constants import DS, PATHS
from dsp.datasets.pcaremeds.enrichments.enrichments import mps_request, enrich_pcaremeds_with_pds

DECIMAL_PRECISION = 14, 5

DELTA_JOIN_COLUMNS = [
    str(_PrimaryCareMedicineModel.ProcessedPeriod),
]

DELTA_MERGES = (
    DeltaMergeParameters(
        DS.PCAREMEDS,
        PATHS.OUT,
        '{dataset_id}.{dataset_id}'.format(dataset_id=DS.PCAREMEDS),
        DELTA_JOIN_COLUMNS,
        PrimaryCareMedicineModel.get_field_names()
    ),
)  # type: Tuple[DeltaMergeParameters, ...]
