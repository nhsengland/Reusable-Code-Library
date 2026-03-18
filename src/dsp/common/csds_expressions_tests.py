from dsp.datasets.models.csds import MPI, AnonSelfAssessment, StaffDetails
from dsp.datasets.models.csds_tests.csds_helper_tests import mpi, anon_self_assessment, staff_details, mpi_dummy
from dsp.shared.constants import ReasonsForAccess


def test_reason_for_access_expression():
    """
    This is to test multiple MPI.referrals table which will be used in RFAs function call
    """
    mpi_table = mpi()
    rfa_org_id = ReasonsForAccess.ProviderCode + ':' + mpi_table["Header"]["OrgID_Provider"]
    rfa_org_id_submit = ReasonsForAccess.SenderIdentity + ':' + mpi_table["Header"]["OrgID_Submitter"]
    rfa_org_id_res = ReasonsForAccess.ResidenceCcg + ':' + mpi_table["OrgID_Responsible"]
    rfa_org_id_ccg = ReasonsForAccess.ResidenceCcgFromPatientPostcode + ':' + mpi_table["OrgID_CCG_Residence"]
    rfa_org_id_comm1 = ReasonsForAccess.CommissionerCode + ':' + mpi_table["Referrals"][0]["OrgID_Commissioner"]
    rfa_org_id_gp = ReasonsForAccess.ResponsibleCcgFromGeneralPractice + ':' + mpi_table['GPPracticeRegistrations'][0][
        "OrgID_CG_GP"]
    rfa_org_id_comm2 = ReasonsForAccess.CommissionerCode + ':' + mpi_table["Referrals"][1]["OrgID_Commissioner"]
    actual_rfa_values = [rfa_org_id, rfa_org_id_submit, rfa_org_id_res, rfa_org_id_ccg, rfa_org_id_comm1,
                         rfa_org_id_comm2, rfa_org_id_gp]
    new_mpi = MPI(mpi_table)  # type: MPI
    assert sorted(actual_rfa_values) == sorted(new_mpi.RFAs)


def test_rfas_no_referral():
    """
    This function to cover mpi table which doesnt contain any referral table or any other tables in it
    """
    mpi_table = mpi_dummy()
    actual_rfa_values = []
    new_mpi = MPI(mpi_table)  # type: MPI
    assert actual_rfa_values == new_mpi.RFAs


def test_reason_for_access_expression_anon_table():
    anon_table = anon_self_assessment()
    rfa_org_id = ReasonsForAccess.ProviderCode + ':' + anon_table["Header"]["OrgID_Provider"]
    rfa_org_id_submit = ReasonsForAccess.SenderIdentity + ':' + anon_table["Header"]["OrgID_Submitter"]
    actual_rfa_values = [rfa_org_id, rfa_org_id_submit]
    new_anon_table = AnonSelfAssessment(anon_table)  # type: AnonSelfAssessment
    assert sorted(actual_rfa_values) == new_anon_table.RFAs


def test_reason_for_access_expression_anon_staff_details():
    anon_staff_details = staff_details()
    rfa_values = ReasonsForAccess.ProviderCode + ':' + anon_staff_details["Header"]["OrgID_Provider"]
    actual_rfa_values = [rfa_values]
    new_anon_staff_details = StaffDetails(anon_staff_details)  # type: StaffDetails
    assert actual_rfa_values == new_anon_staff_details.RFAs

