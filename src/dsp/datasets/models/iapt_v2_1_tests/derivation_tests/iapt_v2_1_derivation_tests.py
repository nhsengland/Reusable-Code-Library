import copy
from typing import Dict, Any, Type
import pytest
from datetime import date

from dsp.common.structured_model import DSPStructuredModel
from dsp.datasets.models.iapt_v2_1_tests.derivation_tests.input_submitted_attribute_values import referral_dict
from dsp.datasets.models.iapt_v2_1_tests.derivation_tests.expected_derived_attribute_values import referral_derivations_dict
from dsp.datasets.models.iapt_v2_1 import Referral
from dsp.datasets.models.test_helpers import check_derived_values


@pytest.mark.parametrize(["model", "input_values", "expected_derivation_values"], [
    (
        Referral,
        referral_dict,
        referral_derivations_dict,
    ),
])
def test_all_derivations(
    model: Type[DSPStructuredModel], input_values: Dict[str, Any], expected_derivation_values: Dict[str, Any]
):
    input_values = copy.deepcopy(input_values)
    expected_derivation_values = copy.deepcopy(expected_derivation_values)
    m = model(input_values)
    check_derived_values(m, expected_derivation_values)


@pytest.mark.parametrize("rep_period_start_date, gp_practice_code, expected_org_id", [
    (date(2020, 1, 1), 'A81004', '00M'),
    (date(2020, 1, 1), 'bad practice code', None),
    (date(2020, 1, 1), None, None),
    (date(2023, 3, 31), 'A81017', '16C'),
    (date(2023, 4, 1), 'A81017', None),
])
def test_ccg_from_gp_practice(rep_period_start_date, gp_practice_code, expected_org_id):
    record = referral_dict
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['GPPracticeRegistrations'][0]['GMPCodeReg'] = gp_practice_code
    new_referral = Referral(record)  # creates a table
    assert expected_org_id == new_referral.Patient.GPPracticeRegistrations[0].OrgID_CCG_GP


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_ccg_res", [
    (date(2015, 10, 3), 'EH4 9DX', None),
    (date(2020, 5, 1), None, None),
    (date(2022, 5, 1), 'bad postcode', None),
    (date(2023, 3, 31), 'LS14JL', '15F'),
    (date(2023, 4, 1), 'LS14JL', None),
])
def test_ccg_res_from_patient_postcode(rep_period_start_date, patient_post_code, expected_ccg_res):
    record = referral_dict
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = patient_post_code
    new_referral = Referral(record)  # creates a table
    assert expected_ccg_res == new_referral.Patient.OrgID_CCG_Residence
