from dsp.datasets.models.iapt import (
    CarePersonnelQualification,
    Referral,
)
from dsp.datasets.models.iapt_tests.iapt_helper_tests import (
    referral,
    care_personnel_qualification,
)
from dsp.shared.constants import ReasonsForAccess


def test_referral_unique_id():
    record = referral()
    record['META']['EVENT_ID'] = '69:1'
    record['Patient']['RowNumber'] = 123
    record['RowNumber'] = 4
    new_referral = Referral(record)  # type: Referral

    assert new_referral.Patient.RecordNumber == 69000000123
    assert new_referral.UniqueID_IDS101 == 69000000004


def test_record_number():
    record = referral()
    record['META']['EVENT_ID'] = '69:1'
    record['Patient']['RowNumber'] = 123
    new_referral = Referral(record)  # type: Referral

    assert new_referral.Patient.RecordNumber == 69000000123


def test_header_unique_id():
    record = referral()
    record['META']['EVENT_ID'] = '69:1'
    record['Patient']['RowNumber'] = 123
    record['Header']['RowNumber'] = 0
    new_referral = Referral(record)  # type: Referral

    assert new_referral.Header.UniqueID_IDS000 == 69000000000


def test_care_personnel_qualification_unique_id():
    cpq = care_personnel_qualification()
    cpq["META"]["EVENT_ID"] = "9101:1"
    cpq["RowNumber"] = 6101
    unique_id = CarePersonnelQualification(cpq)  # type: CarePersonnelQualification
    assert unique_id.UniqueID_IDS902 == 9101000006101


def test_reason_for_access_expression():
    rfa = referral()
    rfa_org_id = ReasonsForAccess.ProviderCode + ':' + rfa["Header"]["OrgIDProv"]
    rfa_org_id_submit = ReasonsForAccess.SenderIdentity + ':' + rfa["Header"]["OrgIDSubmit"]
    rfa_org_id_res = ReasonsForAccess.ResidenceCcg + ':' + rfa["Patient"]["OrgIDResidenceResp"]
    rfa_org_id_ccg = ReasonsForAccess.ResidenceCcgFromPatientPostcode + ':' + rfa["OrgID_CCG_Residence"]
    rfa_org_id_comm = ReasonsForAccess.CommissionerCode + ':' + rfa["OrgIDComm"]
    rfa_org_gp = ReasonsForAccess.ResponsibleCcgFromGeneralPractice + ':' + \
                 rfa['OrgID_CCG_GP']

    actual_rfa_values = ','.join(
        [rfa_org_id, rfa_org_id_submit, rfa_org_id_res, rfa_org_id_ccg, rfa_org_id_comm, rfa_org_gp])

    actual_rfa_values = convert(actual_rfa_values)
    actual_rfa_values = sorted(actual_rfa_values)
    new_referral = Referral(rfa)  # type: Referral
    expected_rfa_values = new_referral.RFAs
    assert actual_rfa_values == expected_rfa_values


def test_reason_for_access_expression_cpq():
    cpq = care_personnel_qualification()
    rfa_values = ReasonsForAccess.ProviderCode + ':' + cpq["Header"]["OrgIDProv"]
    actual_rfa_values = rfa_values.split()
    new_cpq = CarePersonnelQualification(cpq)  # type: CarePersonnelQualification
    expected_rfa_values = new_cpq.RFAs
    assert actual_rfa_values == expected_rfa_values


def convert(string):
    li = list(string.split(","))
    return li
