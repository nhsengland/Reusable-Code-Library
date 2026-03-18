from typing import Any, Set

from dsp.datasets.models.csds import ReferralToTreatment
from dsp.datasets.models.csds_base import _ReferralToTreatment
from dsp.datasets.models.filtering_extract_definition_decorator import FilteringExtractDefinitionDecorator
from dsp.pipeline.spark_state import feature_toggle
from dsp.shared.constants import FeatureToggles

CYP104RTTWaitingTimeDerivationFields = {
    _ReferralToTreatment.WaitingTime_Minutes,
    _ReferralToTreatment.WaitingTime_Overnight,
    _ReferralToTreatment.ResponseStandard_Met
}


DerivationFieldsByTable = {
    ReferralToTreatment.__table__: CYP104RTTWaitingTimeDerivationFields
}


class DefinitionWithWaitingTimeToggle(FilteringExtractDefinitionDecorator):

    def __init__(self, delegate_definition: Any):
        super().__init__(delegate_definition)

    def _should_apply_filter(self) -> bool:
        return not feature_toggle(FeatureToggles.CSDS_WAITING_TIME_DERIVATIONS)

    def _filtered_fields(self) -> Set[str]:
        return {str(field) for field in DerivationFieldsByTable.get(self.__name__, set())}
