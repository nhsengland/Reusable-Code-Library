from typing import Any, Set

from dsp.datasets.models.filtering_extract_definition_decorator import FilteringExtractDefinitionDecorator
from dsp.datasets.models.msds_base import _PregnancyAndBookingDetails, _LabourAndDelivery
from dsp.pipeline.spark_state import feature_toggle
from dsp.shared.constants import FeatureToggles


class DefinitionWithLeadProviderToggle(FilteringExtractDefinitionDecorator):
    _LEAD_PROVIDER_AND_DERIVED_FIELDS = {
        str(field) for field in {
            _PregnancyAndBookingDetails.LeadAnteProvider,
            _PregnancyAndBookingDetails.OrgCodeAPLC,
            _LabourAndDelivery.LeadPostProvider,
            _LabourAndDelivery.OrgCodePPLC,
        }
    }

    def __init__(self, base_definition: Any):
        super().__init__(base_definition)

    def _should_apply_filter(self) -> bool:
        return not feature_toggle(FeatureToggles.INCLUDE_LEAD_PROVIDER_IN_EXTRACTS)

    def _filtered_fields(self) -> Set[str]:
        return self._LEAD_PROVIDER_AND_DERIVED_FIELDS
