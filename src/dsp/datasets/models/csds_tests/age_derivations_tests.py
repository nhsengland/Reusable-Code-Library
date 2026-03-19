from datetime import datetime

from dsp.datasets.models import csds

from dsp.datasets.models.csds_tests.csds_helper_tests import mpi


def test_at_given_date_child():
    record = mpi()
    record["DateOfBirth"] = datetime(2000, 12, 21)
    record["Header"]["RP_StartDate"] = datetime(2002, 8, 1)
    record["Header"]["RP_EndDate"] = datetime(2003, 8, 30)
    record["DateOfDeath"] = datetime(2004, 8, 15)
    record["Referrals"][0]["ReferralRequest_ReceivedDate"] = datetime(2005, 7, 1)
    record["Referrals"][0]["Discharge_Date"] = datetime(2006, 7, 2)
    record["Referrals"][0]['CareContacts'][0]["Contact_Date"] = datetime(2007, 7, 3)
    record["AccommodationTypes"][0]["AccommStatus_Date"] = datetime(2008, 7, 4)
    record["Referrals"][0]["ServiceTypesReferredTo"][0]["Referral_ClosureDate"] = datetime(2009, 7, 5)
    record["Referrals"][0]["ServiceTypesReferredTo"][0]["Referral_RejectionDate"] = datetime(2010, 7, 6)
    record["Referrals"][0]["ReferralsToTreatment"][0]["RTT_StartDate"] = datetime(2011, 7, 7)
    record["Referrals"][0]["ReferralsToTreatment"][0]["RTT_EndDate"] = datetime(2012, 7, 8)
    record["ChildProtectionPlans"][0]["CPP_StartDate"] = datetime(2013, 7, 9)
    record["ChildProtectionPlans"][0]["CPP_EndDate"] = datetime(2014, 7, 10)
    record["CodedImmunisations"][0]["Immunisation_Date"] = datetime(2015, 7, 11)
    record["Immunisations"][0]["Immunisation_Date"] = datetime(2016, 7, 12)
    record["NewbornHearingScreenings"][0]["ServiceRequest_Date"] = datetime(2017, 7, 14)
    record["NewbornHearingScreenings"][0]["Procedure_Date"] = datetime(2018, 7, 15)
    record["BloodSpotResults"][0]["CardCompletion_Date"] = datetime(2019, 7, 16)
    record["InfantPhysicalExaminations"][0]["Examination_Date"] = datetime(2018, 8, 17)
    record["Referrals"][0]["CodedScoredAssessmentReferrals"][0]["AssessmentCompletion_Date"] = datetime(2017, 9, 18)

    new_patient = csds.MPI(record)

    assert new_patient.Age_RP_StartDate == 588
    assert new_patient.AgeYr_RP_StartDate == 1
    assert new_patient.AgeGroup_RP_StartDate == "0-18"
    assert new_patient.AgeBand_RP_StartDate == "0-4"

    assert new_patient.Age_RP_EndDate == 982
    assert new_patient.AgeYr_RP_EndDate == 2
    assert new_patient.AgeGroup_RP_EndDate == "0-18"
    assert new_patient.AgeBand_RP_EndDate == "0-4"

    assert new_patient.Age_Death == 1333
    assert new_patient.AgeYr_Death == 3
    assert new_patient.AgeGroup_Death == "0-18"
    assert new_patient.AgeBand_Death == "0-4"

    assert new_patient.Referrals[0].Age_Referral_ReceivedDate == 1653
    assert new_patient.Referrals[0].AgeYr_Referral_ReceivedDate == 4
    assert new_patient.Referrals[0].AgeGroup_Referral_ReceivedDate == "0-18"
    assert new_patient.Referrals[0].AgeBand_Referral_ReceivedDate == "0-4"

    assert new_patient.Referrals[0].Age_Service_Discharge_Date == 2019
    assert new_patient.Referrals[0].AgeYr_Service_DischargeDate == 5
    assert new_patient.Referrals[0].AgeGroup_Service_DischargeDate == "0-18"
    assert new_patient.Referrals[0].AgeBand_Service_DischargeDate == "5-9"

    assert new_patient.Referrals[0].CareContacts[0].Age_Contact_Date == 2385
    assert new_patient.Referrals[0].CareContacts[0].AgeYr_Contact_Date == 6
    assert new_patient.Referrals[0].CareContacts[0].AgeGroup_Contact_Date == "0-18"
    assert new_patient.Referrals[0].CareContacts[0].AgeBand_Contact_Date == "5-9"

    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[0].\
        Age_BreastFeedingStatus == 2385
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[0].\
        AgeYr_BreastFeedingStatus == 6
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[0].\
        AgeBand_BreastFeedingStatus == "5-9"
    # # No age group needed for this table

    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].CodedScoredAssessmentContacts[0].\
        Age_AssessmentTool_Contact_Date == 2385
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.AccommodationTypes[0].Age_AccommodationStatusDate == 2752
    assert new_patient.AccommodationTypes[0].AgeYr_AccommodationStatusDate == 7
    assert new_patient.AccommodationTypes[0].AgeGroup_AccommodationStatusDate == "0-18"
    assert new_patient.AccommodationTypes[0].AgeBand_AccommodationStatusDate == "5-9"

    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].Age_Referral_ClosureDate == 3118
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeYr_Referral_ClosureDate == 8
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeGroup_Referral_ClosureDate == "0-18"
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeBand_Referral_ClosureDate == "5-9"

    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].Age_Referral_RejectionDate == 3484
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeYr_Referral_RejectionDate == 9
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeGroup_Referral_RejectionDate == "0-18"
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeBand_Referral_RejectionDate == "5-9"

    assert new_patient.Referrals[0].ReferralsToTreatment[0].Age_RTT_StartDate == 3850
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeYr_RTT_StartDate == 10
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeGroup_RTT_StartDate == "0-18"
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeBand_RTT_StartDate == "10-14"

    assert new_patient.Referrals[0].ReferralsToTreatment[0].Age_RTT_EndDate == 4217
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeYr_RTT_EndDate == 11
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeGroup_RTT_EndDate == "0-18"
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeBand_RTT_EndDate == "10-14"

    assert new_patient.ChildProtectionPlans[0].Age_CPP_StartDate == 4583
    assert new_patient.ChildProtectionPlans[0].AgeYr_CPP_StartDate == 12
    # No age group needed for this table
    assert new_patient.ChildProtectionPlans[0].AgeBand_CPP_StartDate == "10-14"

    assert new_patient.ChildProtectionPlans[0].Age_CPP_EndDate == 4949
    assert new_patient.ChildProtectionPlans[0].AgeYr_CPP_EndDate == 13
    # No age group needed for this table
    assert new_patient.ChildProtectionPlans[0].AgeBand_CPP_EndDate == "10-14"

    assert new_patient.CodedImmunisations[0].Age_Immunisation_Date == 5315
    assert new_patient.CodedImmunisations[0].AgeYr_Immunisation_Date == 14
    assert new_patient.CodedImmunisations[0].AgeGroup_Immunisation_Date == "0-18"
    assert new_patient.CodedImmunisations[0].AgeBand_Immunisation_Date == "10-14"
    assert new_patient.CodedImmunisations[0].SchoolYear_Immunisation_Date == "Y9"

    assert new_patient.Immunisations[0].Age_Immunisation_Date == 5682
    assert new_patient.Immunisations[0].AgeYr_Immunisation_Date == 15
    # No age group needed for this table
    assert new_patient.Immunisations[0].AgeBand_Immunisation_Date == "15-19"
    assert new_patient.Immunisations[0].SchoolYear_Immunisation_Date == "Y10"

    assert new_patient.NewbornHearingScreenings[0].Age_ServiceRequest_Date == 6049
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.NewbornHearingScreenings[0].Age_Procedure_Date == 6415
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.BloodSpotResults[0].Age_CardCompletion_Date == 6781
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.InfantPhysicalExaminations[0].Age_Examination_Date == 6448
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].Age_AssessmentCompletion_Date == 6115
    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].AgeYr_AssessmentCompletion_Date == 16
    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].AgeGroup_AssessmentCompletion_Date == "0-18"
    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].AgeBand_AssessmentCompletion_Date == "15-19"

    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].Age_BMI_Observation == 2385
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].AgeYr_BMI_Observation == 6
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].AgeGroup_BMI_Observation == "0-18"
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].AgeBand_BMI_Observation == "5-9"
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].SchoolYear_BMI_Observation == "Y1"



def test_at_given_date_adult():

    record = mpi()
    record["DateOfBirth"] = datetime(1982, 11, 20)
    record["Header"]["RP_StartDate"] = datetime(2002, 8, 1)
    record["Header"]["RP_EndDate"] = datetime(2003, 8, 30)
    record["DateOfDeath"] = datetime(2004, 8, 15)
    record["Referrals"][0]["ReferralRequest_ReceivedDate"] = datetime(2005, 7, 1)
    record["Referrals"][0]["Discharge_Date"] = datetime(2006, 7, 2)
    record["Referrals"][0]['CareContacts'][0]["Contact_Date"] = datetime(2007, 7, 3)
    record["AccommodationTypes"][0]["AccommStatus_Date"] = datetime(2008, 7, 4)
    record["Referrals"][0]["ServiceTypesReferredTo"][0]["Referral_ClosureDate"] = datetime(2009, 7, 5)
    record["Referrals"][0]["ServiceTypesReferredTo"][0]["Referral_RejectionDate"] = datetime(2010, 7, 6)
    record["Referrals"][0]["ReferralsToTreatment"][0]["RTT_StartDate"] = datetime(2011, 7, 7)
    record["Referrals"][0]["ReferralsToTreatment"][0]["RTT_EndDate"] = datetime(2012, 7, 8)
    record["ChildProtectionPlans"][0]["CPP_StartDate"] = datetime(2013, 7, 9)
    record["ChildProtectionPlans"][0]["CPP_EndDate"] = datetime(2014, 7, 10)
    record["CodedImmunisations"][0]["Immunisation_Date"] = datetime(2015, 7, 11)
    record["Immunisations"][0]["Immunisation_Date"] = datetime(2016, 7, 12)
    record["NewbornHearingScreenings"][0]["ServiceRequest_Date"] = datetime(2017, 7, 14)
    record["NewbornHearingScreenings"][0]["Procedure_Date"] = datetime(2018, 7, 15)
    record["BloodSpotResults"][0]["CardCompletion_Date"] = datetime(2019, 7, 16)
    record["InfantPhysicalExaminations"][0]["Examination_Date"] = datetime(2018, 8, 17)
    record["Referrals"][0]["CodedScoredAssessmentReferrals"][0]["AssessmentCompletion_Date"] = datetime(2017, 9, 18)


    new_patient = csds.MPI(record)

    assert new_patient.Age_RP_StartDate == 7194
    assert new_patient.AgeYr_RP_StartDate == 19
    assert new_patient.AgeGroup_RP_StartDate == "19-64"
    assert new_patient.AgeBand_RP_StartDate == "15-19"

    assert new_patient.Age_RP_EndDate == 7588
    assert new_patient.AgeYr_RP_EndDate == 20
    assert new_patient.AgeGroup_RP_EndDate == "19-64"
    assert new_patient.AgeBand_RP_EndDate == "20-24"

    assert new_patient.Age_Death == 7939
    assert new_patient.AgeYr_Death == 21
    assert new_patient.AgeGroup_Death == "19-64"
    assert new_patient.AgeBand_Death == "20-24"

    assert new_patient.Referrals[0].Age_Referral_ReceivedDate == 8259
    assert new_patient.Referrals[0].AgeYr_Referral_ReceivedDate == 22
    assert new_patient.Referrals[0].AgeGroup_Referral_ReceivedDate == "19-64"
    assert new_patient.Referrals[0].AgeBand_Referral_ReceivedDate == "20-24"

    assert new_patient.Referrals[0].Age_Service_Discharge_Date == 8625
    assert new_patient.Referrals[0].AgeYr_Service_DischargeDate == 23
    assert new_patient.Referrals[0].AgeGroup_Service_DischargeDate == "19-64"
    assert new_patient.Referrals[0].AgeBand_Service_DischargeDate == "20-24"

    assert new_patient.Referrals[0].CareContacts[0].Age_Contact_Date == 8991
    assert new_patient.Referrals[0].CareContacts[0].AgeYr_Contact_Date == 24
    assert new_patient.Referrals[0].CareContacts[0].AgeGroup_Contact_Date == "19-64"
    assert new_patient.Referrals[0].CareContacts[0].AgeBand_Contact_Date == "20-24"

    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[
               0].Age_BreastFeedingStatus == 8991
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[
               0].AgeYr_BreastFeedingStatus == 24
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[
               0].AgeBand_BreastFeedingStatus == "20-24"
    # # No age group needed for this table

    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].CodedScoredAssessmentContacts[
               0].Age_AssessmentTool_Contact_Date == 8991
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.AccommodationTypes[0].Age_AccommodationStatusDate == 9358
    assert new_patient.AccommodationTypes[0].AgeYr_AccommodationStatusDate == 25
    assert new_patient.AccommodationTypes[0].AgeGroup_AccommodationStatusDate == "19-64"
    assert new_patient.AccommodationTypes[0].AgeBand_AccommodationStatusDate == "25-29"

    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].Age_Referral_ClosureDate == 9724
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeYr_Referral_ClosureDate == 26
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeGroup_Referral_ClosureDate == "19-64"
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeBand_Referral_ClosureDate == "25-29"

    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].Age_Referral_RejectionDate == 10090
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeYr_Referral_RejectionDate == 27
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeGroup_Referral_RejectionDate == "19-64"
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeBand_Referral_RejectionDate == "25-29"

    assert new_patient.Referrals[0].ReferralsToTreatment[0].Age_RTT_StartDate == 10456
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeYr_RTT_StartDate == 28
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeGroup_RTT_StartDate == "19-64"
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeBand_RTT_StartDate == "25-29"

    assert new_patient.Referrals[0].ReferralsToTreatment[0].Age_RTT_EndDate == 10823
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeYr_RTT_EndDate == 29
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeGroup_RTT_EndDate == "19-64"
    assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeBand_RTT_EndDate == "25-29"

    assert new_patient.ChildProtectionPlans[0].Age_CPP_StartDate == 11189
    assert new_patient.ChildProtectionPlans[0].AgeYr_CPP_StartDate == 30
    # assert new_patient.ChildProtectionPlans[0].AgeGroup_CPP_StartDate == "65_Plus"
    assert new_patient.ChildProtectionPlans[0].AgeBand_CPP_StartDate == "30-34"

    assert new_patient.ChildProtectionPlans[0].Age_CPP_EndDate == 11555
    assert new_patient.ChildProtectionPlans[0].AgeYr_CPP_EndDate == 31
    # assert new_patient.ChildProtectionPlans[0].AgeGroup_CPP_EndDate == "65_Plus"
    assert new_patient.ChildProtectionPlans[0].AgeBand_CPP_EndDate == "30-34"

    assert new_patient.CodedImmunisations[0].Age_Immunisation_Date == 11921
    assert new_patient.CodedImmunisations[0].AgeYr_Immunisation_Date == 32
    assert new_patient.CodedImmunisations[0].AgeGroup_Immunisation_Date == "19-64"
    assert new_patient.CodedImmunisations[0].AgeBand_Immunisation_Date == "30-34"
    assert new_patient.CodedImmunisations[0].SchoolYear_Immunisation_Date is None

    assert new_patient.Immunisations[0].Age_Immunisation_Date == 12288
    assert new_patient.Immunisations[0].AgeYr_Immunisation_Date == 33
    # No age group needed for this table
    assert new_patient.Immunisations[0].AgeBand_Immunisation_Date == "30-34"
    assert new_patient.Immunisations[0].SchoolYear_Immunisation_Date is None

    assert new_patient.NewbornHearingScreenings[0].Age_ServiceRequest_Date == 12655
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.NewbornHearingScreenings[0].Age_Procedure_Date == 13021
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.BloodSpotResults[0].Age_CardCompletion_Date == 13387
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.InfantPhysicalExaminations[0].Age_Examination_Date == 13054
    # No age yr needed for this table
    # No age group needed for this table
    # No age band needed for this table

    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].Age_AssessmentCompletion_Date == 12721
    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].AgeYr_AssessmentCompletion_Date == 34
    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].AgeGroup_AssessmentCompletion_Date == "19-64"
    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].AgeBand_AssessmentCompletion_Date == "30-34"

    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].Age_BMI_Observation == 8991
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].AgeYr_BMI_Observation == 24
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].AgeGroup_BMI_Observation == "19-64"
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].AgeBand_BMI_Observation == "20-24"
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].SchoolYear_BMI_Observation is None



def test_at_given_date_elderly():
        record = mpi()
        record["DateOfBirth"] = datetime(1936, 10, 22)
        record["Header"]["RP_StartDate"] = datetime(2002, 8, 1)
        record["Header"]["RP_EndDate"] = datetime(2003, 8, 30)
        record["DateOfDeath"] = datetime(2004, 8, 15)
        record["Referrals"][0]["ReferralRequest_ReceivedDate"] = datetime(2005, 7, 1)
        record["Referrals"][0]["Discharge_Date"] = datetime(2006, 7, 2)
        record["Referrals"][0]['CareContacts'][0]["Contact_Date"] = datetime(2007, 7, 3)
        record["AccommodationTypes"][0]["AccommStatus_Date"] = datetime(2008, 7, 4)
        record["Referrals"][0]["ServiceTypesReferredTo"][0]["Referral_ClosureDate"] = datetime(2009, 7, 5)
        record["Referrals"][0]["ServiceTypesReferredTo"][0]["Referral_RejectionDate"] = datetime(2010, 7, 6)
        record["Referrals"][0]["ReferralsToTreatment"][0]["RTT_StartDate"] = datetime(2011, 7, 7)
        record["Referrals"][0]["ReferralsToTreatment"][0]["RTT_EndDate"] = datetime(2012, 7, 8)
        record["ChildProtectionPlans"][0]["CPP_StartDate"] = datetime(2013, 7, 9)
        record["ChildProtectionPlans"][0]["CPP_EndDate"] = datetime(2014, 7, 10)
        record["CodedImmunisations"][0]["Immunisation_Date"] = datetime(2015, 7, 11)
        record["Immunisations"][0]["Immunisation_Date"] = datetime(2016, 7, 12)
        record["NewbornHearingScreenings"][0]["ServiceRequest_Date"] = datetime(2017, 7, 14)
        record["NewbornHearingScreenings"][0]["Procedure_Date"] = datetime(2018, 7, 15)
        record["BloodSpotResults"][0]["CardCompletion_Date"] = datetime(2019, 7, 16)
        record["InfantPhysicalExaminations"][0]["Examination_Date"] = datetime(2018, 8, 17)
        record["Referrals"][0]["CodedScoredAssessmentReferrals"][0]["AssessmentCompletion_Date"] = datetime(2017, 9, 18)


        new_patient = csds.MPI(record)

        assert new_patient.Age_RP_StartDate == 24024
        assert new_patient.AgeYr_RP_StartDate == 65
        assert new_patient.AgeGroup_RP_StartDate == "65_Plus"
        assert new_patient.AgeBand_RP_StartDate == "65-69"

        assert new_patient.Age_RP_EndDate == 24418
        assert new_patient.AgeYr_RP_EndDate == 66
        assert new_patient.AgeGroup_RP_EndDate == "65_Plus"
        assert new_patient.AgeBand_RP_EndDate == "65-69"

        assert new_patient.Age_Death == 24769
        assert new_patient.AgeYr_Death == 67
        assert new_patient.AgeGroup_Death == "65_Plus"
        assert new_patient.AgeBand_Death == "65-69"

        assert new_patient.Referrals[0].Age_Referral_ReceivedDate == 25089
        assert new_patient.Referrals[0].AgeYr_Referral_ReceivedDate == 68
        assert new_patient.Referrals[0].AgeGroup_Referral_ReceivedDate == "65_Plus"
        assert new_patient.Referrals[0].AgeBand_Referral_ReceivedDate == "65-69"

        assert new_patient.Referrals[0].Age_Service_Discharge_Date == 25455
        assert new_patient.Referrals[0].AgeYr_Service_DischargeDate == 69
        assert new_patient.Referrals[0].AgeGroup_Service_DischargeDate == "65_Plus"
        assert new_patient.Referrals[0].AgeBand_Service_DischargeDate == "65-69"

        assert new_patient.Referrals[0].CareContacts[0].Age_Contact_Date == 25821
        assert new_patient.Referrals[0].CareContacts[0].AgeYr_Contact_Date == 70
        assert new_patient.Referrals[0].CareContacts[0].AgeGroup_Contact_Date == "65_Plus"
        assert new_patient.Referrals[0].CareContacts[0].AgeBand_Contact_Date == "70-74"

        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[
                   0].Age_BreastFeedingStatus == 25821
        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[
                   0].AgeYr_BreastFeedingStatus == 70
        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[
                   0].AgeBand_BreastFeedingStatus == "70-74"
        # # No age group needed for this table

        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].CodedScoredAssessmentContacts[
                   0].Age_AssessmentTool_Contact_Date == 25821
        # No age yr needed for this table
        # No age group needed for this table
        # No age band needed for this table

        assert new_patient.AccommodationTypes[0].Age_AccommodationStatusDate == 26188
        assert new_patient.AccommodationTypes[0].AgeYr_AccommodationStatusDate == 71
        assert new_patient.AccommodationTypes[0].AgeGroup_AccommodationStatusDate == "65_Plus"
        assert new_patient.AccommodationTypes[0].AgeBand_AccommodationStatusDate == "70-74"

        assert new_patient.Referrals[0].ServiceTypesReferredTo[0].Age_Referral_ClosureDate == 26554
        assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeYr_Referral_ClosureDate == 72
        assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeGroup_Referral_ClosureDate == "65_Plus"
        assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeBand_Referral_ClosureDate == "70-74"

        assert new_patient.Referrals[0].ServiceTypesReferredTo[0].Age_Referral_RejectionDate == 26920
        assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeYr_Referral_RejectionDate == 73
        assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeGroup_Referral_RejectionDate == "65_Plus"
        assert new_patient.Referrals[0].ServiceTypesReferredTo[0].AgeBand_Referral_RejectionDate == "70-74"

        assert new_patient.Referrals[0].ReferralsToTreatment[0].Age_RTT_StartDate == 27286
        assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeYr_RTT_StartDate == 74
        assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeGroup_RTT_StartDate == "65_Plus"
        assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeBand_RTT_StartDate == "70-74"

        assert new_patient.Referrals[0].ReferralsToTreatment[0].Age_RTT_EndDate == 27653
        assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeYr_RTT_EndDate == 75
        assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeGroup_RTT_EndDate == "65_Plus"
        assert new_patient.Referrals[0].ReferralsToTreatment[0].AgeBand_RTT_EndDate == "75-79"

        assert new_patient.ChildProtectionPlans[0].Age_CPP_StartDate == 28019
        assert new_patient.ChildProtectionPlans[0].AgeYr_CPP_StartDate == 76
        # assert new_patient.ChildProtectionPlans[0].AgeGroup_CPP_StartDate == "65_Plus"
        assert new_patient.ChildProtectionPlans[0].AgeBand_CPP_StartDate == "75-79"

        assert new_patient.ChildProtectionPlans[0].Age_CPP_EndDate == 28385
        assert new_patient.ChildProtectionPlans[0].AgeYr_CPP_EndDate == 77
        # assert new_patient.ChildProtectionPlans[0].AgeGroup_CPP_EndDate == "65_Plus"
        assert new_patient.ChildProtectionPlans[0].AgeBand_CPP_EndDate == "75-79"

        assert new_patient.CodedImmunisations[0].Age_Immunisation_Date == 28751
        assert new_patient.CodedImmunisations[0].AgeYr_Immunisation_Date == 78
        assert new_patient.CodedImmunisations[0].AgeGroup_Immunisation_Date == "65_Plus"
        assert new_patient.CodedImmunisations[0].AgeBand_Immunisation_Date == "75-79"
        assert new_patient.CodedImmunisations[0].SchoolYear_Immunisation_Date is None

        assert new_patient.Immunisations[0].Age_Immunisation_Date == 29118
        assert new_patient.Immunisations[0].AgeYr_Immunisation_Date == 79
        # No age group needed for this table
        assert new_patient.Immunisations[0].AgeBand_Immunisation_Date == "75-79"
        assert new_patient.Immunisations[0].SchoolYear_Immunisation_Date is None

        assert new_patient.NewbornHearingScreenings[0].Age_ServiceRequest_Date == 29485
        # No age yr needed for this table
        # No age group needed for this table
        # No age band needed for this table

        assert new_patient.NewbornHearingScreenings[0].Age_Procedure_Date == 29851
        # No age yr needed for this table
        # No age group needed for this table
        # No age band needed for this table

        assert new_patient.BloodSpotResults[0].Age_CardCompletion_Date == 30217
        # No age yr needed for this table
        # No age group needed for this table
        # No age band needed for this table

        assert new_patient.InfantPhysicalExaminations[0].Age_Examination_Date == 29884
        # No age yr needed for this table
        # No age group needed for this table
        # No age band needed for this table

        assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].Age_AssessmentCompletion_Date == 29551
        assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].AgeYr_AssessmentCompletion_Date == 80
        assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[
                   0].AgeGroup_AssessmentCompletion_Date == "65_Plus"
        assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].AgeBand_AssessmentCompletion_Date == "80-84"

        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].Age_BMI_Observation == 25821
        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].AgeYr_BMI_Observation == 70
        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[
                   0].AgeGroup_BMI_Observation == "65_Plus"
        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[
                   0].AgeBand_BMI_Observation == "70-74"
        assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[
                   0].SchoolYear_BMI_Observation is None
