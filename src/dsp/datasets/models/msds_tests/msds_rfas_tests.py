from datetime import date

import pytest
from dateutil.relativedelta import relativedelta

from dsp.datasets.models.msds import PregnancyAndBookingDetails, AnonFindings, AnonSelfAssessment
from dsp.shared.constants import ReasonsForAccess
from dsp.datasets.models.msds_tests.msds_helper_tests import pregnancy_and_booking_details as root, anonymous_finding, anonymous_self_assessment


@pytest.mark.parametrize("org_code, rp_start_date, expected", [
    ('RVAQU', date(1900, 1, 1), 'RVAQU'),
    ('RVAQU', date(2008, 3, 1), 'RVA')
])
def test_resolve_provider_is_normalised_when_required(root, org_code: str, rp_start_date: date, expected: str):
    root['Header']['OrgIDProvider'] = org_code
    root['Header']['RPStartDate'] = rp_start_date
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.ProviderCode, expected) in new_root.RFAs


@pytest.mark.parametrize("org_code, rp_start_date, expected", [
    ('RVAQU', date(1900, 1, 1), 'RVAQU'),
    ('RVAQU', date(2008, 3, 1), 'RVA'),
    ('RVAQU', None, 'RVAQU')
])
def test_resolve_sender_is_normalised_when_required(root, org_code: str, rp_start_date: date, expected: str):
    root['Header']['OrgIDSubmit'] = org_code
    root['Header']['RPStartDate'] = rp_start_date
    root['Header']['RPEndDate'] = rp_start_date + relativedelta(months=1) if rp_start_date else None
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.SenderIdentity, expected) in new_root.RFAs


@pytest.mark.parametrize("org_code, rp_start_date, expected", [
    ('RVAQU', date(1900, 1, 1), 'RVAQU'),
    ('RVAQU', date(2008, 3, 1), 'RVA'),
    ('RVAQU', None, 'RVAQU')
])
def test_resolve_root_commissioner_is_normalised_when_required(root, org_code: str, rp_start_date: date, expected: str):
    root['Header']['OrgIDSubmit'] = None
    root['Header']['OrgIDProvider'] = None
    root['Header']['RPStartDate'] = rp_start_date
    root['OrgIDComm'] = org_code
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, expected) in new_root.RFAs


@pytest.mark.parametrize("org_code, rp_start_date, expected", [
    ('RVAQU', date(1900, 1, 1), 'RVAQU'),
    ('RVAQU', date(2008, 3, 1), 'RVA'),
])
def test_resolve_hsp_commissioner_code_is_normalised_when_required(root, org_code: str, rp_start_date: date, expected: str):
    hpsc = root['HospitalProviderSpells'][0]['HospitalSpellCommissioners'][0]
    hpsc['OrgIDComm'] = org_code
    root['Header']['RPStartDate'] = rp_start_date
    root['Header']['RPEndDate'] = rp_start_date + relativedelta(months=1)
    hpsc['StartDateOrgCodeComm'] = rp_start_date
    hpsc['EndDateOrgCodeComm'] = rp_start_date + relativedelta(months=1)
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, expected) in new_root.RFAs


@pytest.mark.parametrize("org_code, rp_start_date, expected", [
    ('RVAQU', date(1900, 1, 1), 'RVAQU'),
    ('RVAQU', date(2008, 3, 1), 'RVA'),
])
def test_resolve_care_contact_commissioner_code_is_normalised_when_required(root, org_code: str, rp_start_date: date, expected: str):
    root['Header']['RPStartDate'] = rp_start_date
    root['Header']['RPEndDate'] = rp_start_date + relativedelta(months=1)
    root['CareContacts'][0]['OrgIDComm'] = org_code
    root['CareContacts'][0]['CContactDate'] = rp_start_date
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, expected) in new_root.RFAs

def test_multiple_care_contacts(root):
    root['Header']['RPStartDate'] = date.today()
    root['Header']['RPEndDate'] = date.today()
    root['CareContacts'][0]['CContactDate'] = date.today()
    root['CareContacts'] = [
        root['CareContacts'][0],
        root['CareContacts'][0].copy(),
        root['CareContacts'][0].copy()
    ]
    root['CareContacts'][0]['OrgIDComm'] = 'RVA'
    root['CareContacts'][1]['OrgIDComm'] = 'X26'
    root['CareContacts'][2]['OrgIDComm'] = 'X26'
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'RVA') in new_root.RFAs
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'X26') in new_root.RFAs
    assert len([r for r in new_root.RFAs if r == ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'X26')]) == 1


def test_multiple_hps_commissioners(root):
    root['Header']['RPStartDate'] = date.today()
    root['Header']['RPEndDate'] = date.today()
    hps = root['HospitalProviderSpells'][0]
    hpsc = hps['HospitalSpellCommissioners'][0]
    hpsc['StartDateOrgCodeComm'] = date.today()
    hpsc['EndDateOrgCodeComm'] = None
    hps['HospitalSpellCommissioners'] = [
        hpsc,
        hpsc.copy(),
        hpsc.copy()
    ]
    hps['HospitalSpellCommissioners'][0]['OrgIDComm'] = 'RVA'
    hps['HospitalSpellCommissioners'][1]['OrgIDComm'] = 'X26'
    hps['HospitalSpellCommissioners'][2]['OrgIDComm'] = 'X26'
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'RVA') in new_root.RFAs
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'X26') in new_root.RFAs
    assert len([r for r in new_root.RFAs if r == ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'X26')]) == 1


def test_multiple_hospital_provider_spells(root):
    root['Header']['RPStartDate'] = date.today()
    root['Header']['RPEndDate'] = date.today()
    hps = root['HospitalProviderSpells'][0]
    hps['HospitalSpellCommissioners'][0]['StartDateOrgCodeComm'] = date.today()
    hps['HospitalSpellCommissioners'][0]['EndDateOrgCodeComm'] = None
    hpsc = hps['HospitalSpellCommissioners'][0].copy()

    root['HospitalProviderSpells'] = [
        hps,
        hps.copy()
    ]
    root['HospitalProviderSpells'][1]['HospitalSpellCommissioners'] = [hpsc]
    root['HospitalProviderSpells'][0]['HospitalSpellCommissioners'][0]['OrgIDComm'] = 'RVA'
    root['HospitalProviderSpells'][1]['HospitalSpellCommissioners'][0]['OrgIDComm'] = 'X26'
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'RVA') in new_root.RFAs
    assert ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'X26') in new_root.RFAs


@pytest.mark.parametrize("rp_start, rp_end, act_date, expected", [
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 1), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 31), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 2, 1), False),
    (date(1900, 1, 1), date(1900, 1, 31), date(1899, 12, 31), False),
    (None, date(1900, 1, 31), date(1900, 1, 1), False),
    (date(1900, 1, 1), None, date(1900, 1, 1), False),
    (date(1900, 1, 1), date(1900, 1, 31), None, False),
])
def test_anon_findings_rfas(anonymous_finding, rp_start: date, rp_end: date, act_date: date, expected: bool):

    anonymous_finding['OrgIDComm'] = 'X26'
    anonymous_finding['Header']['RPStartDate'] = rp_start
    anonymous_finding['Header']['RPEndDate'] = rp_end
    anonymous_finding['ClinInterDate'] = act_date
    new_group_session = AnonFindings(anonymous_finding)
    assert (
            ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'X26') in new_group_session.RFAs
    ) == expected


@pytest.mark.parametrize("rp_start, rp_end, act_date, expected", [
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 1), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 31), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 2, 1), False),
    (date(1900, 1, 1), date(1900, 1, 31), date(1899, 12, 31), False),
    (None, date(1900, 1, 31), date(1900, 1, 1), False),
    (date(1900, 1, 1), None, date(1900, 1, 1), False),
    (date(1900, 1, 1), date(1900, 1, 31), None, False),
])
def test_anonymous_self_assessment_rfas(anonymous_self_assessment, rp_start: date, rp_end: date, act_date: date, expected: bool):

    anonymous_self_assessment['OrgIDComm'] = 'X26'

    anonymous_self_assessment['Header']['RPStartDate'] = rp_start
    anonymous_self_assessment['Header']['RPEndDate'] = rp_end
    anonymous_self_assessment['CompDate'] = act_date
    new_anonymous_self_assessment = AnonSelfAssessment(anonymous_self_assessment)
    assert (
        ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'X26') in new_anonymous_self_assessment.RFAs
    ) == expected


@pytest.mark.parametrize("rp_start, rp_end, act_date, expected", [
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 1), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 31), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 2, 1), False),
    (date(1900, 1, 1), date(1900, 1, 31), date(1899, 12, 31), False),
    (None, date(1900, 1, 31), date(1900, 1, 1), False),
    (date(1900, 1, 1), None, date(1900, 1, 1), False),
    (date(1900, 1, 1), date(1900, 1, 31), None, False),
])
def test_care_contacts_inclusion(root, rp_start, rp_end, act_date, expected):

    root['Header']['RPStartDate'] = rp_start
    root['Header']['RPEndDate'] = rp_end
    root['CareContacts'][0]['CContactDate'] = act_date
    root['CareContacts'][0]['OrgIDComm'] = 'RVA'

    new_root = PregnancyAndBookingDetails(root)
    assert (
       ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'RVA') in new_root.RFAs
    ) == expected


@pytest.mark.parametrize("rp_start, rp_end, com_start_date, com_end_date, expected", [
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 1), date(1900, 1, 1), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 31), date(1900, 1, 31), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 2, 1), date(1900, 2, 1), False),
    (date(1900, 1, 1), date(1900, 1, 31), date(1899, 12, 31), date(1899, 12, 31), False),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 2, 1), None, False),
    (None, date(1900, 1, 31), date(1900, 1, 1), date(1900, 1, 1), False),
    (date(1900, 1, 1), None, date(1900, 1, 1), date(1900, 1, 1), False),
    (date(1900, 1, 1), date(1900, 1, 31), None, None, False),
    (date(1900, 1, 1), date(1900, 1, 31), date(1899, 12, 31), date(1900, 1, 30), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1899, 12, 31), date(1900, 2, 1), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1899, 12, 31), None, True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 2), date(1900, 2, 1), True),
    (date(1900, 1, 1), date(1900, 1, 31), date(1900, 1, 2), None, True),
    (None, None, None, None, False),
])
def test_hpsc_inclusion(root, rp_start, rp_end, com_start_date, com_end_date, expected):
    root['Header']['RPStartDate'] = rp_start
    root['Header']['RPEndDate'] = rp_end
    hps = root['HospitalProviderSpells'][0]
    hpsc = hps['HospitalSpellCommissioners'][0]
    hpsc['StartDateOrgCodeComm'] = com_start_date
    hpsc['EndDateOrgCodeComm'] = com_end_date
    hps['HospitalSpellCommissioners'][0]['OrgIDComm'] = 'RVA'
    new_root = PregnancyAndBookingDetails(root)
    assert (
                   ReasonsForAccess.format(ReasonsForAccess.CommissionerCode, 'RVA') in new_root.RFAs
           ) == expected


@pytest.mark.parametrize("org_code, rp_start_date, expected", [
    ('RVAQU', date(1900, 1, 1), 'RVAQU'),
    ('RVAQU', date(2008, 3, 1), 'RVA')
])
def test_resolve_residence_ccg_is_normalised_when_required(root, org_code: str, rp_start_date: date, expected: str):
    root['Header']['RPStartDate'] = rp_start_date
    root['Mother']['OrgIDResidenceResp'] = org_code
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.ResidenceCcg, expected) in new_root.RFAs


@pytest.mark.skip("no support for derivation caching")
@pytest.mark.parametrize("org_code, rp_start_date, expected", [
    ('RVAQU', date(2008, 3, 1), 'RVAQU'),
    ('RVAQU', date(2008, 3, 1), 'RVA')
])
def test_resolve_residence_from_postcode_ccg_is_normalised_when_required(root, org_code: str, rp_start_date: date, expected: str):
    root['Header']['RPStartDate'] = rp_start_date
    root['Mother']['CCGResidenceMother'] = org_code
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.ResidenceCcgFromPatientPostcode, expected) in new_root.RFAs


@pytest.mark.skip("no support for derivation caching")
def test_resolve_residence_from_postcode_ccg_is_cached(root):
    root['Mother']['CCGResidenceMother'] = 'BOB'
    new_root = PregnancyAndBookingDetails(root)

    new_root.Mother.values['Postcode'] = 'BL34QG'
    val1 = new_root.Mother.CCGResidenceMother

    new_root.Mother.values['Postcode'] = 'LS14JL'
    val2 = new_root.Mother.CCGResidenceMother
    assert val1 == val2


# todo: need caching support and need to be able to test get most recent gp
@pytest.mark.skip("no support for derivation caching")
@pytest.mark.parametrize("org_code, rp_start_date, expected", [
    ('RVAQU', date(1900, 1, 1), 'RVAQU'),
    ('RVAQU', date(2008, 3, 1), 'RVA')
])
def test_resolve_ccg_from_gp_is_normalised_when_required(root, org_code: str, rp_start_date: date, expected: str):
    root['Header']['RPStartDate'] = rp_start_date
    root['Mother']['GPPracticeRegistrations'][0]['OrgIDCCGGPPractice'] = org_code
    new_root = PregnancyAndBookingDetails(root)
    assert ReasonsForAccess.format(ReasonsForAccess.ResponsibleCcgFromGeneralPractice, expected) in new_root.RFAs
