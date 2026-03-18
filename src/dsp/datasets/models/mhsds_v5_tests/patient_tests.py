import pytest

from dsp.datasets.models.mhsds_v5 import Referral, MentalHealthActLegalStatusClassificationAssignmentPeriod

# this import is to make pytest fixture available
from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import referral, \
    mental_health_act_legal_status_classification_assignment_periods


@pytest.mark.parametrize("ethnicity, expected", [
    ('A1', 'A'),
    ('A0', 'A'),
    ('A', 'A'),
    ('X', '-1'),
    (None, None),
    ('', None),
    (' ', None),
    ('  ', None),
    ('99', '99'),
    ('100', '-1'),
    ('?', '-1'),
    ('D5', 'D'),
    ('0', '-1'),
    ('00', '-1'),
    ('01', '-1'),
    ('0A', '-1'),
])
def test_ethnicity(referral, ethnicity: str, expected: str):
    referral['Patient']['EthnicCategory'] = ethnicity
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.NHSDEthnicity


@pytest.mark.parametrize("legal_status_code, expected", [
    (None, None),
    ('', None),
    (' ', None),
    ('  ', None),
    ('01', '01'),
    ('15', '15'),
    ('99', '99'),
    ('10', '10'),
    ('1', '01'),
    ('9', '09'),
    ('0', '-1'),
    ('00', '-1'),
    ('11', '-1'),
    ('21', '-1'),
    ('96', '-1')
])
def test_legal_status(mental_health_act_legal_status_classification_assignment_periods, legal_status_code: str, expected: str):
    mental_health_act_legal_status_classification_assignment_periods['LegalStatusCode'] = legal_status_code
    new_mh_legal_status = MentalHealthActLegalStatusClassificationAssignmentPeriod(mental_health_act_legal_status_classification_assignment_periods)
    actual = new_mh_legal_status.NHSDLegalStatus
    assert actual == expected


def test_phase_2_literal_defaults(referral):
    new_referral = Referral(referral)
    assert not new_referral.Patient.PatMRecInRP
