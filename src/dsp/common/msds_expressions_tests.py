from datetime import date
from typing import Dict, List
from unittest.mock import Mock

import pytest

from dsp.datasets.models.msds import (
    AnonSelfAssessment,
    AnonFindings,
    StaffDetails,
    PregnancyAndBookingDetails,
    LabourAndDelivery,
    CareActivity,
    CareContact,
    _MotherDemog
)
# noinspection PyUnresolvedReferences
from dsp.datasets.models.msds_tests.msds_helper_tests import (
    anonymous_finding,
    anonymous_self_assessment,
    care_activities,
    care_contacts,
    motherdemog,
    pregnancy_and_booking_details,
    staff_details,
    labours_and_deliveries,
)
from dsp.common.expressions import Select, ModelExpression
from dsp.common.msds_expressions import (CommIdsForMsdsTable, SnomedProcedureCodeMatches,
    DerivePostpartumBloodLoss)


def test_header_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1001:1"
    pregnancy_and_booking_details["Header"]["RowNumber"] = 123
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.Header.MSD000_ID == 1001000000123


def test_motherdemog_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1001:1"
    pregnancy_and_booking_details["Mother"]["RowNumber"] = 112
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.Mother.MSD001_ID == 1001000000112


def test_gp_prac_reg_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1001:1"
    pregnancy_and_booking_details["Mother"]["GPPracticeRegistrations"][0]["RowNumber"] = 114
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.Mother.GPPracticeRegistrations[0].MSD002_ID == 1001000000114


def test_soc_per_circum_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1001:1"
    pregnancy_and_booking_details["Mother"]["SocialAndPersonalCircumstances"][0]["RowNumber"] = 115
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.Mother.SocialAndPersonalCircumstances[0].MSD003_ID == 1001000000115


def test_overseas_vis_charge_cat_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1001:1"
    pregnancy_and_booking_details["Mother"]["OverseasVisitorChargingCategories"][0]["RowNumber"] = 116
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.Mother.OverseasVisitorChargingCategories[0].MSD004_ID == 1001000000116


def test_pregnancy_booking_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["RowNumber"] = 7117
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.MSD101_ID == 1101000007117


def test_maternity_care_plans_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["MaternityCarePlans"][0]["RowNumber"] = 7101
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.MaternityCarePlans[0].MSD102_ID == 1101000007101


def test_dating_scan_procedures_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["DatingScanProcedures"][0]["RowNumber"] = 7103
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.DatingScanProcedures[0].MSD103_ID == 1101000007103


def test_coded_scored_assessment_preg_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["CodedScoredAssessmentsPregnancy"][0]["RowNumber"] = 7104
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.CodedScoredAssessmentsPregnancy[0].MSD104_ID == 1101000007104


def test_prov_diag_preg_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["ProvisionalDiagnosisPregnancies"][0]["RowNumber"] = 7105
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.ProvisionalDiagnosisPregnancies[0].MSD105_ID == 1101000007105


def test_diag_preg_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["DiagnosisPregnancies"][0]["RowNumber"] = 7106
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.DiagnosisPregnancies[0].MSD106_ID == 1101000007106


def test_med_hist_prev_diag_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["MedicalHistoryPreviousDiagnoses"][0]["RowNumber"] = 7107
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.MedicalHistoryPreviousDiagnoses[0].MSD107_ID == 1101000007107


def test_family_hist_at_booking_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["FamilyHistoryAtBooking"][0]["RowNumber"] = 7108
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.FamilyHistoryAtBooking[0].MSD108_ID == 1101000007108


def test_finding_and_obs_mother_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "1101:1"
    pregnancy_and_booking_details["FindingsAndObservationsMother"][0]["RowNumber"] = 7109
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.FindingsAndObservationsMother[0].MSD109_ID == 1101000007109


def test_care_contact_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "2101:1"
    pregnancy_and_booking_details["CareContacts"][0]["RowNumber"] = 8101
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.CareContacts[0].MSD201_ID == 2101000008101


def test_care_activities_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "2101:1"
    pregnancy_and_booking_details["CareContacts"][0]["CareActivities"][0]["RowNumber"] = 8102
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.CareContacts[0].CareActivities[0].MSD202_ID == 2101000008102


def test_coded_scored_assessment_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "2101:1"
    pregnancy_and_booking_details["CareContacts"][0]["CareActivities"][0]["CodedScoredAssessments"][0][
        "RowNumber"] = 8103
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.CareContacts[0].CareActivities[0].CodedScoredAssessments[
               0].MSD203_ID == 2101000008103


def test_labours_and_deliveries_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "3101:1"
    pregnancy_and_booking_details["LaboursAndDeliveries"][0]["RowNumber"] = 9101
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.LaboursAndDeliveries[0].MSD301_ID == 3101000009101


def test_care_activity_labours_deliveries_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "3101:1"
    pregnancy_and_booking_details["LaboursAndDeliveries"][0]["CareActivityLaboursAndDeliveries"][0][
        "RowNumber"] = 9102
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.LaboursAndDeliveries[0].CareActivityLaboursAndDeliveries[
               0].MSD302_ID == 3101000009102


def test_baby_demographics_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "4101:1"
    pregnancy_and_booking_details["LaboursAndDeliveries"][0]["BabyDemographics"][0]["RowNumber"] = 4101
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.LaboursAndDeliveries[0].BabyDemographics[
               0].MSD401_ID == 4101000004101


def test_neonatal_admissions_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "4101:1"
    pregnancy_and_booking_details["LaboursAndDeliveries"][0]["BabyDemographics"][0][
        "NeonatalAdmissions"][0]["RowNumber"] = 4102
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.LaboursAndDeliveries[0].BabyDemographics[0].NeonatalAdmissions[
               0].MSD402_ID == 4101000004102


def test_prov_diag_neonatals_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "4101:1"
    pregnancy_and_booking_details["LaboursAndDeliveries"][0]["BabyDemographics"][0][
        "ProvisionalDiagnosisNeonatals"][0]["RowNumber"] = 9103
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert \
        unique_id.LaboursAndDeliveries[0].BabyDemographics[
            0].ProvisionalDiagnosisNeonatals[
            0].MSD403_ID == 4101000009103


def test_diag_neonatals_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "4101:1"
    pregnancy_and_booking_details["LaboursAndDeliveries"][0]["BabyDemographics"][0][
        "DiagnosisNeonatals"][0]["RowNumber"] = 9104
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.LaboursAndDeliveries[0].BabyDemographics[0].DiagnosisNeonatals[
               0].MSD404_ID == 4101000009104


def test_care_activities_baby_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "4101:1"
    pregnancy_and_booking_details["LaboursAndDeliveries"][0]["BabyDemographics"][0][
        "CareActivitiesBaby"][0]["RowNumber"] = 9105
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.LaboursAndDeliveries[0].BabyDemographics[0].CareActivitiesBaby[
               0].MSD405_ID == 4101000009105


def test_care_coded_assessments_baby_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "4101:1"
    pregnancy_and_booking_details["LaboursAndDeliveries"][0]["BabyDemographics"][0][
        "CareActivitiesBaby"][0]["CodedScoredAssessmentsBaby"][0]["RowNumber"] = 2106
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.LaboursAndDeliveries[0].BabyDemographics[0].CareActivitiesBaby[
               0].CodedScoredAssessmentsBaby[0].MSD406_ID == 4101000002106


def test_prov_hospital_provider_spells_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "5101:1"
    pregnancy_and_booking_details["HospitalProviderSpells"][0]["RowNumber"] = 3101
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.HospitalProviderSpells[0].MSD501_ID == 5101000003101


def test_hospital_spells_commissioners_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "5101:1"
    pregnancy_and_booking_details["HospitalProviderSpells"][0]["HospitalSpellCommissioners"][0][
        "RowNumber"] = 3102
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.HospitalProviderSpells[0].HospitalSpellCommissioners[
               0].MSD502_ID == 5101000003102


def test_ward_stays_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "5101:1"
    pregnancy_and_booking_details["HospitalProviderSpells"][0]["WardStays"][0]["RowNumber"] = 3103
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.HospitalProviderSpells[0].WardStays[0].MSD503_ID == 5101000003103


def test_assigned_care_professional_unique_number(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["META"]["EVENT_ID"] = "5101:1"
    pregnancy_and_booking_details["HospitalProviderSpells"][0]["AssignedCareProfessionals"][0][
        "RowNumber"] = 3104
    unique_id = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert unique_id.HospitalProviderSpells[0].AssignedCareProfessionals[
               0].MSD504_ID == 5101000003104


def test_anon_self_assessments_unique_number(anonymous_self_assessment: Dict):
    anonymous_self_assessment["META"]["EVENT_ID"] = "6101:1"
    anonymous_self_assessment["RowNumber"] = 5101
    unique_id = AnonSelfAssessment(anonymous_self_assessment)  # type: AnonSelfAssessment
    assert unique_id.MSD601_ID == 6101000005101


def test_anon_findings_unique_number(anonymous_finding: Dict):
    anonymous_finding["META"]["EVENT_ID"] = "6101:1"
    anonymous_finding["RowNumber"] = 5102
    unique_id = AnonFindings(anonymous_finding)  # type: AnonFindings
    assert unique_id.MSD602_ID == 6101000005102


def test_staff_details_unique_number(staff_details: Dict):
    staff_details["META"]["EVENT_ID"] = "9101:1"
    staff_details["RowNumber"] = 6101
    unique_id = StaffDetails(staff_details)  # type: StaffDetails
    assert unique_id.MSD901_ID == 9101000006101


def test_postcode_district_derivation(pregnancy_and_booking_details: Dict):
    pregnancy_and_booking_details["Mother"]["Postcode"] = "SE17 2UU"
    pregnancy_model = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails
    assert pregnancy_model.Mother.PostcodeDistrictMother == "SE17"


@pytest.mark.parametrize("est_date_del, last_period, expected", [
    (None, date(2000, 12, 1), 31),
    (date(2001, 1, 31), None, 40 * 7 - 30),
    (date(2001, 1, 31), date(2000, 12, 1), 40 * 7 - 30),
    (None, None, None),
])
def test_gestational_age(pregnancy_and_booking_details: dict, est_date_del: date, last_period: date, expected: int):
    pregnancy_and_booking_details['EDDAgreed'] = est_date_del
    pregnancy_and_booking_details['AntenatalAppDate'] = date(2001, 1, 1)
    pregnancy_and_booking_details['LastMenstrualPeriodDate'] = last_period
    gestational_age = PregnancyAndBookingDetails(pregnancy_and_booking_details).GestAgeBooking
    assert gestational_age == expected


@pytest.mark.parametrize("cig_per_day, expected", [
    ("0", "0"),
    ("0.5", "1-9"),
    ("1", "1-9"),
    ("9.9", "1-9"),
    ("19", "10-19"),
    ("19.5", "10-19"),
    ("100", "20 or more"),
    (None, None),
])
def test_cigarettes_band(pregnancy_and_booking_details: dict, care_contacts, care_activities: dict,
                         cig_per_day: str, expected: str):
    care_activities['CareConID'] = care_contacts['CareConID']
    care_activities['CigarettesPerDay'] = cig_per_day
    care_contacts['CContactDate'] = pregnancy_and_booking_details['AntenatalAppDate']
    care_contacts['CareActivities'] = [CareActivity(care_activities)]
    pregnancy_and_booking_details['CareContacts'] = [CareContact(care_contacts)]

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)

    assert preg_and_booking.CigarettesPerDayBand == expected


@pytest.mark.parametrize("units_per_week, expected", [
    ("0", "Less than 1 unit"),
    ("0.5", "Less than 1 unit"),
    ("1", "1 - less than 7 units"),
    ("6", "1 - less than 7 units"),
    ("7", "7 - less than 14 units"),
    ("13.5", "7 - less than 14 units"),
    ("14", "14 units and over"),
    ("14.5", "14 units and over"),
    ("100", "14 units and over"),
    (None, None),
])
def test_alcohol_band(pregnancy_and_booking_details: dict, care_contacts, care_activities: dict,
                      units_per_week: str, expected: str):
    care_activities['CareConID'] = care_contacts['CareConID']
    care_activities['AlcoholUnitsPerWeek'] = units_per_week
    care_contacts['CContactDate'] = pregnancy_and_booking_details['AntenatalAppDate']
    care_contacts['CareActivities'] = [CareActivity(care_activities)]
    pregnancy_and_booking_details['CareContacts'] = [CareContact(care_contacts)]

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)

    assert preg_and_booking.AlcoholUnitsPerWeekBand == expected


@pytest.mark.parametrize("alcohol_units, antenatal_appointment_date, care_contact_date, expected", [
    ('99', date(2000, 12, 1), date(2000, 12, 1), '99'),
    ('99', date(2000, 12, 2), date(2000, 12, 1), None),
    (None, date(2000, 12, 1), date(2000, 12, 1), None),
])
def test_alcohol_units_booking(pregnancy_and_booking_details: dict, care_contacts, care_activities: dict,
                               alcohol_units: str, antenatal_appointment_date: date, care_contact_date: date,
                               expected: str):
    care_activities['CareConID'] = care_contacts['CareConID']
    care_activities['AlcoholUnitsPerWeek'] = alcohol_units
    pregnancy_and_booking_details['AntenatalAppDate'] = antenatal_appointment_date
    care_contacts['CContactDate'] = care_contact_date
    care_contacts['CareActivities'] = [CareActivity(care_activities)]
    pregnancy_and_booking_details['CareContacts'] = [CareContact(care_contacts)]

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)

    assert preg_and_booking.AlcoholUnitsBooking == expected


def test_births_per_labour_and_delivery_derivation_for_two_babies(labours_and_deliveries: Dict):
    babies = [labours_and_deliveries['BabyDemographics'][0],
              labours_and_deliveries['BabyDemographics'][0]]
    labours_and_deliveries["BabyDemographics"] = babies
    unique_id = LabourAndDelivery(labours_and_deliveries)  # type: LabourAndDelivery
    assert unique_id.BirthsPerLabandDel == 2


def test_births_per_labour_and_delivery_derivation_for_three_babies(labours_and_deliveries: Dict):
    babies = [labours_and_deliveries['BabyDemographics'][0],
              labours_and_deliveries['BabyDemographics'][0],
              labours_and_deliveries['BabyDemographics'][0]]
    labours_and_deliveries["BabyDemographics"] = babies
    unique_id = LabourAndDelivery(labours_and_deliveries)  # type: LabourAndDelivery
    assert unique_id.BirthsPerLabandDel == len(babies)


def test_births_per_labour_and_delivery_derivation_for_no_babies(labours_and_deliveries: Dict):
    babies = []
    labours_and_deliveries["BabyDemographics"] = babies
    unique_id = LabourAndDelivery(labours_and_deliveries)  # type: LabourAndDelivery
    assert unique_id.BirthsPerLabandDel == len(babies)


def test_msd_comms_id_expression_duplication(pregnancy_and_booking_details):
    pregnancy_and_booking_details["CareContacts"][0]["OrgIDComm"] = '12D'
    pregnancy_and_booking_details["HospitalProviderSpells"][0]["HospitalSpellCommissioners"][0]["OrgIDComm"] = '12D'

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails

    commissioner_ids = CommIdsForMsdsTable(
        booking_comm_id_expr=Select(_MotherDemog.Root.OrgIDComm),
        care_contact_comm_id_expr=Select(_MotherDemog.Root.CareContacts),
        hospital_spell_commissioner_comm_id_expr=Select(
            _MotherDemog.Root.HospitalProviderSpells.HospitalSpellCommissioners)
    ).resolve_value(preg_and_booking)

    assert sorted(commissioner_ids) == ['08H', '12D']


def test_msd_comms_id_expression_all_select(pregnancy_and_booking_details):
    pregnancy_and_booking_details["CareContacts"][0]["OrgIDComm"] = '16C'

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails

    commissioner_ids = CommIdsForMsdsTable(
        booking_comm_id_expr=Select(_MotherDemog.Root.OrgIDComm),
        care_contact_comm_id_expr=Select(_MotherDemog.Root.CareContacts),
        hospital_spell_commissioner_comm_id_expr=Select(
            _MotherDemog.Root.HospitalProviderSpells.HospitalSpellCommissioners)
    ).resolve_value(preg_and_booking)

    assert sorted(commissioner_ids) == ['08H', '12D', '16C']


def test_msd_comms_id_expression_missing_match(pregnancy_and_booking_details):
    pregnancy_and_booking_details["CareContacts"][0]["OrgIDComm"] = '16C'
    pregnancy_and_booking_details["HospitalProviderSpells"] = []

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)  # type: PregnancyAndBookingDetails

    commissioner_ids = CommIdsForMsdsTable(
        booking_comm_id_expr=Select(_MotherDemog.Root.OrgIDComm),
        care_contact_comm_id_expr=Select(_MotherDemog.Root.CareContacts),
        hospital_spell_commissioner_comm_id_expr=Select(
            _MotherDemog.Root.HospitalProviderSpells.HospitalSpellCommissioners)
    ).resolve_value(preg_and_booking)

    assert sorted(commissioner_ids) == ['08H', '16C']


@pytest.mark.parametrize("procedure_code, values, expected", [
    ('1', [], False),
    (None, ['1'], False),
    ('1;2;3', ['1', '2', '3'], False),
    ('123', ['123', '456'], True),
    ('123:456=3', ['123', '456', '3'], False),
    ('', ['1'], False)
])
def test_snomed_procedure_code_matches(procedure_code, values: List[str], expected):
    procedure_code_exp = Mock(ModelExpression)
    procedure_code_exp.resolve_value.return_value = procedure_code

    predicate = SnomedProcedureCodeMatches(procedure_code_exp, values)
    actual = predicate.resolve_value(model={})
    assert actual == expected


@pytest.mark.parametrize("master_snomed_ct_obs_code, ucum_unit,"
                         "observation_value, expected_postpartum_bloodloss", [
    (719051004, 'ml', '69', '69'),  # test positive conditions
    (719051004, 'ML', '69', '69'),  # test case insensitivity
    (457486468, 'ml', '63', None),  # test incorrect snomed code
    (719051004, 'mb', '12', None),  # test incorrect UCUM
    (719051004, 'ml', '-9', None),  # test don't derive for negative obs value
    (719051004, 'ml', 'cant_float_this_naananana', None), # test don't derive for non numeric obs value
    (None, 'mb', '64', None),  # test blank snomed code
    (719051004, None, '64', None),  # test blank UCUM
    (719051004, 'mb', None, None),  # test blank snomed code
])
def test_post_partum_blood_loss_check(
        pregnancy_and_booking_details: dict, master_snomed_ct_obs_code: int,
        ucum_unit: str, observation_value: str,
        expected_postpartum_bloodloss: str):

    pregnancy_and_booking_details['LaboursAndDeliveries'][0][
        "CareActivityLaboursAndDeliveries"][0][
        "MasterSnomedCTObsCode"] = master_snomed_ct_obs_code
    pregnancy_and_booking_details['LaboursAndDeliveries'][0][
        "CareActivityLaboursAndDeliveries"][0][
        "ObsValue"] = observation_value
    pregnancy_and_booking_details['LaboursAndDeliveries'][0][
        "CareActivityLaboursAndDeliveries"][0][
        "UCUMUnit"] = ucum_unit

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)

    assert preg_and_booking.LaboursAndDeliveries[
               0].CareActivityLaboursAndDeliveries[
               0].PostpartumBloodLoss == expected_postpartum_bloodloss
