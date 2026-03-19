from datetime import datetime, time, date

from dsp.datasets.models import csds

from dsp.datasets.models.csds_tests.csds_helper_tests import mpi


def test_at_given_date_sameday():
    record = mpi()
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_StartDate"] = datetime(2001, 1, 1)
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_EndDate"] = datetime(2001, 1, 1)
    record['Referrals'][0]["ReferralRequest_ReceivedDate"] = datetime(2002, 2, 2)
    record['Referrals'][0]['CareContacts'][0]["Contact_Date"] = datetime(2002, 2, 2)
    record['ChildProtectionPlans'][0]["CPP_StartDate"] = datetime(2003, 3, 3)
    record['ChildProtectionPlans'][0]["CPP_EndDate"] = datetime(2003, 3, 3)
    record['BloodSpotResults'][0]["CardCompletion_Date"] = datetime(2004, 4, 4)
    record['BloodSpotResults'][0]["TestResult_ReceivedDate"] = datetime(2004, 4, 4)

    new_patient = csds.MPI(record)
    assert new_patient.Referrals[0].ReferralsToTreatment[0].Days_RTTStart_to_End == 0
    assert new_patient.Referrals[0].CareContacts[0].Days_Referral_to_CareContact == 0
    assert new_patient.ChildProtectionPlans[0].Duration_SpentOnCPP == 1
    assert new_patient.BloodSpotResults[0].Days_ReceiveResults == 0


def test_at_given_date_fewdays():
    record = mpi()
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_StartDate"] = datetime(2001, 1, 1)
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_EndDate"] = datetime(2001, 1, 2)
    record['Referrals'][0]["ReferralRequest_ReceivedDate"] = datetime(2002, 2, 2)
    record['Referrals'][0]['CareContacts'][0]["Contact_Date"] = datetime(2002, 2, 4)
    record['ChildProtectionPlans'][0]["CPP_StartDate"] = datetime(2003, 3, 3)
    record['ChildProtectionPlans'][0]["CPP_EndDate"] = datetime(2003, 3, 6)
    record['BloodSpotResults'][0]["CardCompletion_Date"] = datetime(2004, 4, 4)
    record['BloodSpotResults'][0]["TestResult_ReceivedDate"] = datetime(2004, 4, 8)

    new_patient = csds.MPI(record)
    assert new_patient.Referrals[0].ReferralsToTreatment[0].Days_RTTStart_to_End == 1
    assert new_patient.Referrals[0].CareContacts[0].Days_Referral_to_CareContact == 2
    assert new_patient.ChildProtectionPlans[0].Duration_SpentOnCPP == 4
    assert new_patient.BloodSpotResults[0].Days_ReceiveResults == 4


def test_at_given_date_child():
    record = mpi()
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_StartDate"] = datetime(1965, 5, 5)
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_EndDate"] = datetime(1984, 9, 9)
    record['Referrals'][0]["ReferralRequest_ReceivedDate"] = datetime(1976, 7, 7)
    record['Referrals'][0]['CareContacts'][0]["Contact_Date"] = datetime(1995, 10, 10)
    record['ChildProtectionPlans'][0]["CPP_StartDate"] = datetime(1987, 9, 9)
    record['ChildProtectionPlans'][0]["CPP_EndDate"] = datetime(2006, 11, 11)
    record['BloodSpotResults'][0]["CardCompletion_Date"] = datetime(1998, 11, 22)
    record['BloodSpotResults'][0]["TestResult_ReceivedDate"] = datetime(2017, 12, 12)

    new_patient = csds.MPI(record)
    assert new_patient.Referrals[0].ReferralsToTreatment[0].Days_RTTStart_to_End == 7067
    assert new_patient.Referrals[0].CareContacts[0].Days_Referral_to_CareContact == 7034
    assert new_patient.ChildProtectionPlans[0].Duration_SpentOnCPP == 7004
    assert new_patient.BloodSpotResults[0].Days_ReceiveResults == 6960


def test_at_given_date_adult():
    record = mpi()
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_StartDate"] = datetime(1954, 6, 6)
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_EndDate"] = datetime(2019, 1, 1)
    record['Referrals'][0]["ReferralRequest_ReceivedDate"] = datetime(1943, 8, 8)
    record['Referrals'][0]['CareContacts'][0]["Contact_Date"] = datetime(2008, 2, 2)
    record['ChildProtectionPlans'][0]["CPP_StartDate"] = datetime(1932, 10, 10)
    record['ChildProtectionPlans'][0]["CPP_EndDate"] = datetime(1997, 3, 3)
    record['BloodSpotResults'][0]["CardCompletion_Date"] = datetime(1921, 12, 12)
    record['BloodSpotResults'][0]["TestResult_ReceivedDate"] = datetime(1986, 4, 4)

    new_patient = csds.MPI(record)
    assert new_patient.Referrals[0].ReferralsToTreatment[0].Days_RTTStart_to_End == 23585
    assert new_patient.Referrals[0].CareContacts[0].Days_Referral_to_CareContact == 23554
    assert new_patient.ChildProtectionPlans[0].Duration_SpentOnCPP == 23521
    assert new_patient.BloodSpotResults[0].Days_ReceiveResults == 23489


def test_at_given_date_elderly():
    record = mpi()
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_StartDate"] = datetime(1915, 4, 4)
    record['Referrals'][0]['ReferralsToTreatment'][0]["RTT_EndDate"] = datetime(2044, 5, 5)
    record['Referrals'][0]["ReferralRequest_ReceivedDate"] = datetime(1906, 2, 2)
    record['Referrals'][0]['CareContacts'][0]["Contact_Date"] = datetime(2035, 6, 6)
    record['ChildProtectionPlans'][0]["CPP_StartDate"] = datetime(1907, 1, 1)
    record['ChildProtectionPlans'][0]["CPP_EndDate"] = datetime(2036, 7, 7)
    record['BloodSpotResults'][0]["CardCompletion_Date"] = datetime(1918, 3, 3)
    record['BloodSpotResults'][0]["TestResult_ReceivedDate"] = datetime(2047, 8, 8)

    new_patient = csds.MPI(record)
    assert new_patient.Referrals[0].ReferralsToTreatment[0].Days_RTTStart_to_End == 47149
    assert new_patient.Referrals[0].CareContacts[0].Days_Referral_to_CareContact == 47241
    assert new_patient.ChildProtectionPlans[0].Duration_SpentOnCPP == 47306
    assert new_patient.BloodSpotResults[0].Days_ReceiveResults == 47275

