import copy
from typing import Dict, Any, Type
import pytest

from dsp.common.structured_model import DSPStructuredModel
from dsp.datasets.models.mhsds_v5_tests.derivation_tests.input_submitted_attribute_values import *
from dsp.datasets.models.mhsds_v5_tests.derivation_tests.expected_derived_attribute_values import *
from dsp.datasets.models.mhsds_v5 import Referral, GroupSession, MentalHealthDropInContact, AnonymousSelfAssessment, \
    StaffDetails
from dsp.datasets.models.test_helpers import check_derived_values

@pytest.mark.parametrize(["model", "input_values", "expected_derivation_values"], [
    (
        Referral,
        referral_dict,
        referral_derivations_dict
    ),
    (
        Referral,
        referral_dict_2,
        referral_derivations_dict_2
    ),
    (
        GroupSession,
        group_session_dict,
        group_session_derivations_dict
    ),
    (
        MentalHealthDropInContact,
        mental_health_drop_in_contact_dict,
        mental_health_drop_in_contact_derivations_dict
    ),
    (
        AnonymousSelfAssessment,
        anonymous_self_assessment_dict,
        anonymous_self_assessment_derivations_dict
    ),
    (
        StaffDetails,
        staff_details_dict,
        staff_details_derivations_dict
    )
])
def test_all_derivations(
        model: Type[DSPStructuredModel], input_values: Dict[str, Any], expected_derivation_values: Dict[str, Any]
):
    input_values = copy.deepcopy(input_values)
    expected_derivation_values = copy.deepcopy(expected_derivation_values)
    m = model(input_values)
    check_derived_values(m, expected_derivation_values)
