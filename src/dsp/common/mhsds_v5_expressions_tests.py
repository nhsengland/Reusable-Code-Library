from datetime import date, datetime
from typing import Dict
import pytest
from collections import namedtuple

from dsp.datasets.models.mhsds_v5_base import _CareContact, _AssignedCareProfessional
from dsp.common.expressions import Literal
from dsp.common.mhsds_v5_expressions import (
    DMSEthnicity, EthnicityHigher, EthnicityLow, ServiceTeamType, SpecialisedMHService,
    FirstAttendedContactInRPDate,
    NationalCodeDescription, WardStayBedType, NHSDLegalStatusExpression
)
from dsp.datasets.models.mhsds_v5 import Referral, AssignedCareProfessional
# this import is to make pytest fixture available
# noinspection PyUnresolvedReferences
from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import referral
from dsp.common.expressions import Select, SimpleCommIds
from dsp.common.mhsds_v5_expressions import CommIdsForTable, _is_valid_discharge_amh, _is_valid_ws_end_rp_amh
from dsp.common.structured_model import DSPStructuredModel, SubmittedAttribute, RepeatingSubmittedAttribute


class CareContact(_CareContact):
    __concrete__ = True

class AssignedCareProfessional(_AssignedCareProfessional):
    __concrete__ = True


@pytest.mark.parametrize("care_contacts, rp_start, rp_end, expected_date", [
    (
            [
                CareContact({'CareContDate': datetime(2021, 1, 1), 'AttendOrDNACode': '5'}),
                CareContact({'CareContDate': datetime(2021, 1, 10), 'AttendOrDNACode': '6'})
            ],
            datetime(2021, 1, 1),  # Reporting period start date
            datetime(2021, 2, 1),  # Reporting period end date
            datetime(2021, 1, 1)
    ),
    (
            [
                CareContact({'CareContDate': datetime(2020, 1, 1), 'AttendOrDNACode': '5'}),  # Outside of RP
                CareContact({'CareContDate': datetime(2021, 1, 10), 'AttendOrDNACode': '6'})
            ],
            datetime(2021, 1, 1),  # Reporting period start date
            datetime(2021, 2, 1),  # Reporting period end date
            datetime(2021, 1, 10)
    ),
    (
            [
                CareContact({'CareContDate': datetime(2021, 1, 1), 'AttendOrDNACode': '4'}),  # not 5 or 6
                CareContact({'CareContDate': datetime(2021, 1, 10), 'AttendOrDNACode': '6'})
            ],
            datetime(2021, 1, 1),  # Reporting period start date
            datetime(2021, 2, 1),  # Reporting period end date
            datetime(2021, 1, 10)
    )
])
def test_first_attended_contact_in_rp_date(care_contacts, rp_start, rp_end, expected_date):
    out_date = FirstAttendedContactInRPDate(
        Literal(care_contacts),
        Literal(rp_start),
        Literal(rp_end)
    ).resolve_value(None)
    assert out_date == expected_date


@pytest.mark.parametrize(
    'ic_ethnicity,expected',
    [
        ('A', 'British'),
        ('B', 'Irish'),
        ('C', 'Any other White background'),
        ('D', 'White and Black Caribbean'),
        ('E', 'White and Black African'),
        ('F', 'White and Asian'),
        ('G', 'Any other mixed background'),
        ('H', 'Indian'),
        ('J', 'Pakistani'),
        ('K', 'Bangladeshi'),
        ('L', 'Any other Asian background'),
        ('M', 'Caribbean'),
        ('N', 'African'),
        ('P', 'Any other Black background'),
        ('R', 'Chinese'),
        ('S', 'Any other ethnic group'),
        ('Z', 'Not stated'),
        ('99', 'Not Known'),
        ('X', 'Unknown'),
        (' ', 'Unknown'),
    ]
)
def test_ethnicity_low(ic_ethnicity: str, expected: str):
    ethnicity_low = EthnicityLow(
        DMSEthnicity(ethnic_category_expr=Literal(ic_ethnicity))
    ).resolve_value(None)
    assert type(ethnicity_low) == type(expected)
    assert ethnicity_low == expected


@pytest.mark.parametrize(
    'ic_ethnicity,expected',
    [
        ('A', 'White'),
        ('E', 'Mixed'),
        ('K', 'Asian or Asian British'),
        ('P', 'Black or Black British'),
        ('R', 'Other Ethnic Groups'),
        ('a', 'Unknown'),
        ('99', 'Unknown'),
        ('Z', 'Unknown'),
        (' ', 'Unknown'),
    ]
)
def test_ethnicity_higher(ic_ethnicity: str, expected: str):
    ethnicity_higher = EthnicityHigher(
        DMSEthnicity(ethnic_category_expr=Literal(ic_ethnicity))
    ).resolve_value(None)
    assert type(ethnicity_higher) == type(expected)
    assert ethnicity_higher == expected


@pytest.mark.parametrize("service_team_code, expected_name", [
    ("A01", "Day Care Service"),
    ("C01", "Autistim Service"),
    ("A21", "Crisis Café/Safe Haven/Sanctuary Service"),
    ("Z02", "Other Mental Health Service - out of scope of National Tariff Payment System"),
    ("NULL", None),
    ("", None)
])
def test_service_team_type_mapping(service_team_code, expected_name):
    assert ServiceTeamType(Literal(service_team_code)).resolve_value(None) == expected_name


@pytest.mark.parametrize("service_team_code, expected_name", [
    ("NCBPS23G", "ADULT ATAXIA TELANGIECTASIA SERVICES"),
    ("NCBPS27Z", "ADULT SPECIALIST ENDOCRINOLOGY SERVICES"),
    ("NCBPS32A", "COCHLEAR IMPLANTATION SERVICES"),
    ("99999999", "NON-NHS ENGLAND DIRECTLY-COMMISSIONED SERVICE"),
    ("NULL", None),
    ("", None),
    ("NCBPH07Z", "HIB-CONTAINING VACCINATION PROGRAMME"),
    ("NCBPH29Z", "SECTION 7A PUBLIC HEALTH SERVICES FOR CHILDREN AND ADULTS IN SECURE AND DETAINED SETTINGS IN ENGLAND"),
    ("NCBPHXXX", "NHS ENGLAND - PUBLIC HEALTH BUT NOT ATTRIBUTABLE"),
    ("NCBAFXXX", "NHS ENGLAND - ARMED FORCES")
])
def test_service_team_name_mapping(service_team_code, expected_name):
    assert SpecialisedMHService(Literal(service_team_code)).resolve_value(None) == expected_name

@pytest.mark.parametrize("national_code, expected_description", [
    ("10", "Acute adult mental health care"),
    ("13", "Adult Eating Disorders"),
    ("32", "Child and Young Person Medium Secure Learning Disabilities"),
    ("NULL", None),
    ("", None)
])
def test_national_code_description(national_code, expected_description):
    assert NationalCodeDescription(Literal(national_code)).resolve_value(None) == expected_description


@pytest.mark.parametrize("hosp_id, ward_sec, clin_care_code, ward_type, care_prof_list, expected_code", [
    ("HPI00000000000000001", "1", "54", "05",
     [AssignedCareProfessional({'HospProvSpellID': 'HPI00000000000000001', 'TreatFuncCodeMH': '715'})], 1),
    ("HPI00000000000000001", "7", "54", "05",
     [AssignedCareProfessional({'HospProvSpellID': 'HPI000000000000001', 'TreatFuncCodeMH': '724'})], 4),
    ("HPI00000000000000001", "7", "54", "06",
     [AssignedCareProfessional({'HospProvSpellID': 'HPI00000000000000001', 'TreatFuncCodeMH': '715'})], 3),
    ("HPI00000000000000001", "7", "54", "05",
     [AssignedCareProfessional({'HospProvSpellID': 'HPI00000000000000001', 'TreatFuncCodeMH': '715'})], 3),
    ("HPI00000000000000001", "7", "54", "05",
     [AssignedCareProfessional({'HospProvSpellID': 'HPI00000000000000001', 'TreatFuncCodeMH': '710'})], 2),
    ("HPI00000000000000001", "7", "51", "03",
     [AssignedCareProfessional({'HospProvSpellID': 'HPI00000000000000001', 'TreatFuncCodeMH': '717'})], 2),
    ("HPI00000000000000001", "7", "50", "07",
     [AssignedCareProfessional({'HospProvSpellID': 'HPI00000000000000001', 'TreatFuncCodeMH': '711'})], 4),
])
def test_ward_stay_bed_type_lookup(hosp_id, ward_sec, clin_care_code, ward_type, care_prof_list, expected_code):
    assert WardStayBedType(Literal(hosp_id), Literal(ward_sec), Literal(clin_care_code),
                           Literal(ward_type), Literal(care_prof_list)).resolve_value(None) == expected_code



@pytest.mark.parametrize("legal_status_code, expected", [
    (None, None),
    ("  ", None),
    ("100", "-1"),
    ("07", "07"),
    ("1", "01")
])
def test_nhsd_legal_status(legal_status_code, expected):

    actual = NHSDLegalStatusExpression(
        legal_status_code_expr=Literal(legal_status_code)
    ).resolve_value(None)
    assert expected == actual


def test_record_number(referral: Dict):
    referral['META']['EVENT_ID'] = '69:1'
    referral['Patient']['RowNumber'] = 123
    new_referral = Referral(referral)  # type: Referral

    assert new_referral.Patient.RecordNumber == 69000000123
    assert new_referral.Patient.MHS001UniqID == 69000000123


def test_header_unique_id(referral: Dict):
    referral['META']['EVENT_ID'] = '69:1'
    referral['Patient']['RowNumber'] = 123
    referral['Header']['RowNumber'] = 0
    new_referral = Referral(referral)  # type: Referral

    assert new_referral.Header.MHS000UniqID == 69000000000


def test_referral_unique_id(referral: Dict):
    referral['META']['EVENT_ID'] = '69:1'
    referral['Patient']['RowNumber'] = 123
    referral['RowNumber'] = 4
    new_referral = Referral(referral)  # type: Referral

    assert new_referral.Patient.RecordNumber == 69000000123
    assert new_referral.MHS101UniqID == 69000000004


def test_referral_unique_id_out_of_range(referral: Dict):
    referral['META']['EVENT_ID'] = '69:1'
    referral['Patient']['RowNumber'] = 123
    referral['RowNumber'] = 1000000000
    new_referral = Referral(referral)  # type: Referral

    assert new_referral.Patient.RecordNumber == 69000000123

    with pytest.raises(ValueError):
        assert new_referral.MHS101UniqID == 69000000004


DischargFields = namedtuple("DischargFields", "rp_start_date, rp_end_date, end_date_ws, refer_closure_date, "
                                              "refer_rejection_date, end_date_ass_care_prof, ward_sec_level, "
                                              "ward_type, intend_code_mh, treat_func_code_mh")


@pytest.mark.parametrize("disch_fields, expected", [
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 6, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='3',
        ward_type='06',
        intend_code_mh='53',
        treat_func_code_mh='711'
    ), 4),
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='4',
        ward_type='06',
        intend_code_mh='73',
        treat_func_code_mh='710'
    ), 2),  # treat_func_code_mh 710 returns 2
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='2',
        ward_type='06',
        intend_code_mh='73',
        treat_func_code_mh='710'
    ), 1),  # ward_sec_level 2 returns 1
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='5',
        ward_type='06',
        intend_code_mh='53',
        treat_func_code_mh='710'
    ), 3),  # intend_code_mh 53 returns 3
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='5',
        ward_type='06',
        intend_code_mh='73',
        treat_func_code_mh='724'
    ), 1),  # treat_func_code_mh 724 returns 1
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='5',
        ward_type='06',
        intend_code_mh='73',
        treat_func_code_mh='715'
    ), 3),  # treat_func_code_mh715 and ward_type -6 returns 3
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='5',
        ward_type='06',
        intend_code_mh='73',
        treat_func_code_mh='727'
    ), 3),  # treat_func_code_mh 727 returns 3
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='5',
        ward_type='06',
        intend_code_mh='73',
        treat_func_code_mh='723'
    ), 2),  # treat_func_code_mh 723 returns 2
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='5',
        ward_type='06',
        intend_code_mh='51',
        treat_func_code_mh='800'
    ), 2),  # ward_type 06 and intend_code_mh 51 returns 2
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 10, 31),
        end_date_ws=date(2016, 6, 1),
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='2',
        ward_type='03',
        intend_code_mh='64',
        treat_func_code_mh='728'
    ), 4),  # EndDateWardStay not between period_start
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 10, 31),
        end_date_ws=date(2015, 6, 1),
        end_date_ass_care_prof=None,
        refer_closure_date=date(2015, 5, 1),
        refer_rejection_date=None,
        ward_sec_level='2',
        ward_type='03',
        intend_code_mh='64',
        treat_func_code_mh='728'
    ), 1),  # ReferClosureDate less than EndDateWardStay
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 10, 31),
        end_date_ws=date(2015, 6, 1),
        end_date_ass_care_prof=None,
        refer_closure_date=date(2015, 6, 1),
        refer_rejection_date=date(2015, 5, 1),
        ward_sec_level='2',
        ward_type='03',
        intend_code_mh='64',
        treat_func_code_mh='728'
    ), 1),  # ReferRejectionDate less than EndDateWardStay
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 10, 31),
        end_date_ws=date(2015, 6, 1),
        end_date_ass_care_prof=date(2015, 5, 1),
        refer_closure_date=date(2015, 6, 1),
        refer_rejection_date=None,
        ward_sec_level='2',
        ward_type='03',
        intend_code_mh='64',
        treat_func_code_mh='728'
    ), 1),  # ReferClosureDate less than EndDateWardStay
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='3',
        ward_type='06',
        intend_code_mh='53',
        treat_func_code_mh='711'
    ), 4),  # EndDateWardStay is None
])
def test_bed_type_adult_discharge_expr(referral, disch_fields, expected):
    referral['Header']['ReportingPeriodEndDate'] = disch_fields.rp_end_date
    referral['Header']['ReportingPeriodStartDate'] = disch_fields.rp_start_date
    referral['ServiceTypesReferredTo'][0]['ReferClosureDate'] = disch_fields.refer_closure_date
    referral['ServiceTypesReferredTo'][0]['ReferRejectionDate'] = disch_fields.refer_rejection_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['EndDateWardStay'] = disch_fields.end_date_ws
    referral['HospitalProviderSpells'][0]['WardStays'][0]['WardType'] = disch_fields.ward_type
    referral['HospitalProviderSpells'][0]['WardStays'][0]['WardSecLevel'] = disch_fields.ward_sec_level
    referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][0][
        'TreatFuncCodeMH'] = disch_fields.treat_func_code_mh
    # referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][1]['TreatFuncCodeMH'] = '800'
    referral['HospitalProviderSpells'][0]['WardStays'][0][
        'IntendClinCareIntenCodeMH'] = disch_fields.intend_code_mh
    referral['HospitalProviderSpells'][0]['WardStays'][0]['WardAge'] \
        = None  # set this to , to make AMHServiceWSEndRP true

    new_referral = Referral(referral)

    assert expected == new_referral.HospitalProviderSpells[0].WardStays[0].BedTypeAdultDischarge


@pytest.mark.parametrize("disch_fields, expected", [
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='3',
        ward_type='06',
        intend_code_mh='53',
        treat_func_code_mh='811'
    ), 1),  # ward_sec_level 3 returns 1
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='4',
        ward_type='6',
        intend_code_mh='43',
        treat_func_code_mh='811'
    ), 4),  # default 4
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='4',
        ward_type='06',
        intend_code_mh='53',
        treat_func_code_mh='811'
    ), 3),  # intend_code_mh 53 returns 3
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='4',
        ward_type='06',
        intend_code_mh='83',
        treat_func_code_mh='724'
    ), 1),  # treat_func_code_mh 724 returns 1
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='6',
        ward_type='06',
        intend_code_mh='83',
        treat_func_code_mh='715'
    ), 3),  # ward_type 06 and treat_func_code_mh 715 returns 3
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='6',
        ward_type='00',
        intend_code_mh='83',
        treat_func_code_mh='725'
    ), 3),  # treat_func_code_mh 725 returns 3
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='6',
        ward_type='00',
        intend_code_mh='83',
        treat_func_code_mh='712'
    ), 2),  # treat_func_code_mh 712 returns 2
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=None,
        end_date_ass_care_prof=None,
        refer_closure_date=None,
        refer_rejection_date=None,
        ward_sec_level='6',
        ward_type='06',
        intend_code_mh='51',
        treat_func_code_mh='812'
    ), 2),  # ward_type 06 and intend_code_mh 51 returns 2
    (DischargFields(
        rp_start_date=date(2015, 5, 1),
        rp_end_date=date(2015, 5, 31),
        end_date_ws=date(2015, 5, 10),
        end_date_ass_care_prof=None,
        refer_closure_date=date(2015, 5, 10),
        refer_rejection_date=None,
        ward_sec_level='6',
        ward_type='06',
        intend_code_mh='51',
        treat_func_code_mh='812'
    ), 4)  # end_date_ws < rp_end_date
])
def test_bed_type_adult_end_rp_expr(referral, disch_fields, expected):
    referral['Header']['ReportingPeriodEndDate'] = disch_fields.rp_end_date
    referral['Header']['ReportingPeriodStartDate'] = disch_fields.rp_start_date
    referral['ServiceTypesReferredTo'][0]['ReferClosureDate'] = disch_fields.refer_closure_date
    referral['ServiceTypesReferredTo'][0]['ReferRejectionDate'] = disch_fields.refer_rejection_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['EndDateWardStay'] = disch_fields.end_date_ws
    referral['HospitalProviderSpells'][0]['WardStays'][0]['WardType'] = disch_fields.ward_type
    referral['HospitalProviderSpells'][0]['WardStays'][0]['WardSecLevel'] = disch_fields.ward_sec_level
    referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][0][
        'TreatFuncCodeMH'] = disch_fields.treat_func_code_mh
#    referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][1]['TreatFuncCodeMH'] = '800'
    referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][0]['EndDateAssCareProf'] = \
        disch_fields.end_date_ass_care_prof
#    referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][1]['EndDateAssCareProf'] = \
#        disch_fields.end_date_ass_care_prof
    referral['HospitalProviderSpells'][0]['WardStays'][0][
        'IntendClinCareIntenCodeMH'] = disch_fields.intend_code_mh
    referral['HospitalProviderSpells'][0]['WardStays'][0]['WardAge'] \
        = None  # set this to , to make AMHServiceWSEndRP true

    new_referral = Referral(referral)

    assert expected == new_referral.HospitalProviderSpells[0].WardStays[0].BedTypeAdultEndRP


@pytest.mark.parametrize("submitted_datetime, rp_start_date, expected", [
    (datetime(2018, 7, 10), datetime.strptime('20180501', '%Y%m%d').date(), 2),
    (datetime(2018, 7, 1), datetime.strptime('20180501', '%Y%m%d').date(), 2),
    (datetime(2018, 6, 15), datetime.strptime('20180501', '%Y%m%d').date(), 1),
])
def test_file_type_expression(referral, submitted_datetime, rp_start_date, expected):
    referral['Header']['ReportingPeriodStartDate'] = rp_start_date
    referral['META']['EVENT_RECEIVED_TS'] = submitted_datetime

    new_referral = Referral(referral)
    assert expected == new_referral.Header.FileType


HelperDiscServiceFields = namedtuple("HelperDiscServiceFields", "ward_type intend_cc_code treat_func_code ward_age")


@pytest.mark.parametrize("disch_service, expected", [
    (HelperDiscServiceFields('01', '61', '700', '10'), False),  # WardType IN ('01', '02', '05')
    (HelperDiscServiceFields('03', '61', '700', '10'), False),  # IntendClinCareIntenCodeMH in ('61', '62', '63')
    (HelperDiscServiceFields('03', '64', '700', '10'), False),  # TreatFuncCodeMH IN ('700', '711')
    (HelperDiscServiceFields('03', '64', '701', '10'), False),  # WardAge IN ('10', '11', '12')
    (HelperDiscServiceFields('03', '64', '701', '13'), True),  # WardAge IN ('13', '14', '15')
    (HelperDiscServiceFields('03', '51', '701', '16'), True),  # IntendClinCareIntenCodeMH IN ('51', '52', '53')
    (HelperDiscServiceFields('03', '64', '710', '16'), True),  # TreatFuncCodeMH IN ('710', '712',..,'727')
    (HelperDiscServiceFields('03', '64', '728', '16'), True),  # WardType IN ('03', '04', '06')
    (HelperDiscServiceFields('07', '64', '728', '16'), True),  # else Y
])
def test_is_valid_discharge_amh(referral, disch_service, expected):
    assigned_care_professional_dict = referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][0]
    assigned_care_professional_dict['TreatFuncCodeMH'] = disch_service.treat_func_code

    assigned_care_professional = AssignedCareProfessional(assigned_care_professional_dict)

    assert expected == _is_valid_discharge_amh(ward_type=disch_service.ward_type,
                                               intend_cc_inten_code=disch_service.intend_cc_code,
                                               ward_age=disch_service.ward_age,
                                               assigned_care_prof_list=[assigned_care_professional])


@pytest.mark.parametrize("disch_service, expected", [
    (HelperDiscServiceFields('3', '64', '811', '18'), True),  # Return default True
    (HelperDiscServiceFields('3', '64', '711', '18'), False),  # treat_func_code = 711 returns False
    (HelperDiscServiceFields('01', '64', '811', '18'), False),  # ward_type 01 returns False
    (HelperDiscServiceFields('1', '61', '811', '18'), False)    # intend_clin_code_mh 61 returns False
])
def test_is_valid_discharge_amh(referral, disch_service, expected):
    assigned_care_professional_dict = referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][0]
    assigned_care_professional_dict['TreatFuncCodeMH'] = disch_service.treat_func_code

    assigned_care_professional = AssignedCareProfessional(assigned_care_professional_dict)

    assert expected == _is_valid_ws_end_rp_amh(intend_clin_code_mh=disch_service.intend_cc_code,
                                               assigned_care_prof_list=[assigned_care_professional],
                                               ward_type=disch_service.ward_type,
                                               ward_age=disch_service.ward_age)


class _Commissioner(DSPStructuredModel):
    OrgIDComm = SubmittedAttribute('OrgIDComm', str)  # type: SubmittedAttribute


class _Patient(DSPStructuredModel):
    Name = SubmittedAttribute('Name', str)  # type: SubmittedAttribute
    Dob = SubmittedAttribute('Dob', str)  # type: SubmittedAttribute
    Commissioners = RepeatingSubmittedAttribute('Commissioners', _Commissioner)  # type: RepeatingSubmittedAttribute
    OrgIDComm = SubmittedAttribute('OrgIDComm', str)  # type: SubmittedAttribute


class Commissioner(_Commissioner):
    __concrete__ = True


class Patient(_Patient):
    __concrete__ = True


@pytest.fixture()
def sample_patient():
    comm_1 = Commissioner({'OrgIDComm': 'RA1'})
    comm_2 = Commissioner({'OrgIDComm': 'RA2'})
    return Patient({'Name': 'A Patient',
                       'Dob': '2009-01-01',
                       'Commissioners': [comm_1, comm_2],
                       'OrgIDComm': 'RB1'})


@pytest.fixture()
def patient_with_nones():
    comm_1 = Commissioner({'OrgIDComm': None})
    comm_2 = Commissioner({'OrgIDComm': 'RA2'})
    return Patient({'Name': 'A Patient',
                       'Dob': '2009-01-01',
                       'Commissioners': [comm_1, comm_2],
                       'OrgIDComm': None})


@pytest.fixture()
def patient_all_nones():
    comm_1 = Commissioner({'OrgIDComm': None})
    comm_2 = Commissioner({'OrgIDComm': None})
    return Patient({'Name': 'A Patient',
                       'Dob': '2009-01-01',
                       'Commissioners': [comm_1, comm_2],
                       'OrgIDComm': None})


def test_comm_ids_for_table(sample_patient):
    commissioner_ids = (
        CommIdsForTable(referral_comm_id_expr=Select(_Patient.OrgIDComm),
                        indirect_activities_expr=Select(_Patient.Commissioners)).resolve_value(sample_patient)
    )

    assert set(commissioner_ids) == {sample_patient.OrgIDComm}.union(
        set([c.OrgIDComm for c in sample_patient.Commissioners]))


def test_no_comm_ids_for_table(sample_patient):
    commissioner_ids = CommIdsForTable().resolve_value(sample_patient)

    assert set(commissioner_ids) == set()


def test_simple_comm_ids(sample_patient):
    commissioner_ids = SimpleCommIds(primary_expr=Select(_Patient.OrgIDComm)).resolve_value(sample_patient)

    assert set(commissioner_ids) == {sample_patient.OrgIDComm}


def test_none_for_simple_id(patient_with_nones):
    commissioner_ids = SimpleCommIds(primary_expr=Select(_Patient.OrgIDComm)).resolve_value(patient_with_nones)

    assert set(commissioner_ids) == set()


def test_none_for_multiple_ids(patient_with_nones):
    commissioner_ids = SimpleCommIds(primary_expr=Select(_Patient.Commissioners)).resolve_value(patient_with_nones)

    assert set(commissioner_ids) == set([c.OrgIDComm for c in patient_with_nones.Commissioners]) - {None}


def test_none_comm_ids_for_table(patient_all_nones):
    commissioner_ids = (
        CommIdsForTable(referral_comm_id_expr=Select(_Patient.OrgIDComm),
                        indirect_activities_expr=Select(_Patient.Commissioners)).resolve_value(patient_all_nones)
    )

    assert set(commissioner_ids) == set()
