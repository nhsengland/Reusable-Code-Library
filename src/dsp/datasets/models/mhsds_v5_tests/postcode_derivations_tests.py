import datetime

import pytest

from dsp.datasets.models.mhsds_v5 import Referral, MasterPatientIndex
from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import referral, master_patient_index


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'LS1'),
    (datetime.date(2015, 5, 1), 'LS1 4JL', 'LS1'),
    (datetime.date(2018, 10, 3), 'LS999ZZ', None),  # invalid postcode
])
def test_postcode_district(referral, rep_period_start_date, postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = postcode
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.PostcodeDistrict


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'E01033015'),
    (datetime.date(2015, 5, 1), 'LS1 4JL', 'E01033015'),
    (datetime.date(2018, 10, 3), 'LS999ZZ', None),  # invalid postcode
    (datetime.date(2018, 10, 3), 'EH4 9DX', 'S01008864'),  # postcode valid from 20160201 to 20170201
])
def test_lsoa(referral, rep_period_start_date, postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = postcode
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.LSOA2011


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'E08000035'),
    (datetime.date(2015, 5, 1), 'LS1 4JL', 'E08000035'),
    (datetime.date(2018, 10, 3), 'LS999ZZ', None),  # invalid postcode
])
def test_la_district_auth(referral, rep_period_start_date, postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = postcode
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.LADistrictAuth


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'E99999999'),
    (datetime.date(2015, 5, 1), 'LS1 4JL', 'E99999999'),
    (datetime.date(2018, 10, 3), 'LS999ZZ', None),  # invalid postcode
])
def test_county(referral, rep_period_start_date, postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = postcode
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.County


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'E05001420'),
    (datetime.date(2015, 5, 1), 'LS1 4JL', 'E05001420'),
    (datetime.date(2018, 10, 3), 'LS999ZZ', None),  # invalid postcode
])
def test_electoral_ward(referral, rep_period_start_date, postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = postcode
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.ElectoralWard


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'LS1'),
    (datetime.date(2015, 5, 1), 'LS1 4JL', 'LS1'),
    (datetime.date(2018, 10, 3), 'TS26 0ZX', None),  # invalid postcode at point in time
    (datetime.date(2018, 10, 3), 'LS999ZZ', None),  # invalid postcode
])
def test_postcode_district_main_visitor(referral, rep_period_start_date, postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['HospitalProviderSpells'][0]['PostcodeMainVisitor'] = postcode
    new_referral = Referral(referral)
    assert expected == new_referral.HospitalProviderSpells[0].PostcodeDistrictMainVisitor


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'LS1'),
    (datetime.date(2015, 5, 1), 'LS1 4JL', 'LS1'),
    (datetime.date(2018, 10, 3), 'TS26 0ZX', None),  # invalid postcode at point in time
    (datetime.date(2018, 10, 3), 'LS999ZZ', None),  # invalid postcode
])
def test_postcode_district_disch_dest(referral, rep_period_start_date, postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['HospitalProviderSpells'][0]['PostcodeDischDestHospProvSpell'] = postcode
    new_referral = Referral(referral)
    assert expected == new_referral.HospitalProviderSpells[0].PostcodeDistrictDischDest


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2018, 10, 3), 'LS999ZZ', None),  # invalid postcode
    (datetime.date(2018, 10, 3), 'SO168GX', 1),
    (datetime.date(2018, 10, 3), 'SO168HA', 2),
    (datetime.date(2018, 10, 3), 'SO168DD', 3),
    (datetime.date(2018, 10, 3), 'SO152LN', 4)
])
def test_imd_quartile(referral, rep_period_start_date, postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = postcode
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.IMDQuart


@pytest.mark.parametrize("postcode, expected", [
    (None, None),
    ('LS99 9ZZ', None),
    ('LS10 1LG', None),
    ('ZZ99 3VZ', 'ZZ99 3VZ')
])
def test_default_postcode(master_patient_index, postcode, expected):
    master_patient_index['Postcode'] = postcode
    new_patient = MasterPatientIndex(master_patient_index)
    assert expected == new_patient.DefaultPostcode


@pytest.mark.parametrize("rep_period_start_date, residence_postcode, gp_code, expected", [
    (None, None, None, None),
    (datetime.date(2018, 10, 3), None, None, None),
    (datetime.date(2018, 10, 3), 'LS999ZZ', 'A81001', None),  # invalid postcode
    (datetime.date(2018, 10, 3), 'AL100AU', 'A81001', 320),
    (datetime.date(2018, 10, 3), 'E10 5NP', 'A81002', 346)
])
def test_gp_distance_from_home(referral, rep_period_start_date, residence_postcode, gp_code, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = residence_postcode
    referral['Patient']['GPs'][0]['GMPCodeReg'] = gp_code
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.GPs[0].GPDistanceHome


@pytest.mark.parametrize("rep_period_start_date, residence_postcode, site_id, expected", [
    (None, None, None, None),
    (datetime.date(2018, 10, 3), None, None, None),
    (datetime.date(2018, 10, 3), 'LS999ZZ', 'A81003', None),  # invalid postcode
    (datetime.date(2018, 10, 3), 'AL100AU', 'A81003', 332),
    (datetime.date(2018, 10, 3), 'E10 5NP', 'A81004', 342)
])
def test_cont_loc_distance_from_home(referral, rep_period_start_date, residence_postcode, site_id, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = residence_postcode
    referral['CareContacts'][0]['SiteIDOfTreat'] = site_id
    new_referral = Referral(referral)
    assert expected == new_referral.CareContacts[0].ContLocDistanceHome


@pytest.mark.parametrize("rep_period_start_date, residence_postcode, site_id, expected", [
    (None, None, None, None),
    (datetime.date(2018, 10, 3), None, None, None),
    (datetime.date(2018, 10, 3), 'LS999ZZ', 'A81003', None),  # invalid postcode
    (datetime.date(2018, 10, 3), 'AL100AU', 'A81003', 332),
    (datetime.date(2018, 10, 3), 'B1  1JX', 'A81004', 234)
])
def test_ward_loc_distance_from_home(referral, rep_period_start_date, residence_postcode, site_id, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = residence_postcode
    referral['HospitalProviderSpells'][0]['WardStays'][0]['SiteIDOfTreat'] = site_id
    new_referral = Referral(referral)
    assert expected == new_referral.HospitalProviderSpells[0].WardStays[0].WardLocDistanceHome


@pytest.mark.parametrize("rep_period_start_date, residence_postcode, expected", [
    (datetime.date(2018, 10, 3), 'LS14JL', '15F'),
    (datetime.date(2018, 10, 3), 'LS1 4JL', '15F'),
    (datetime.date(2018, 10, 3), None, None),
    (None, None, None),
    (datetime.date(2023, 3, 31), 'LS14JL', '15F'),
    (datetime.date(2023, 4, 1), 'LS14JL', None),
])
def test_ccg_res_from_patient_postcode(referral, rep_period_start_date, residence_postcode, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['Postcode'] = residence_postcode
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.OrgIDCCGRes


@pytest.mark.parametrize("postcode, ts, expected", [
    ('LS14JL', datetime.date(2018, 10, 3), 'E54000005'),
    ('LS1 4JL', datetime.date(2018, 10, 3), 'E54000005'),
    (None, datetime.date(2018, 10, 3), None),
    (None, None, None),
])
def test_org_id_icb_res(referral, postcode, ts, expected):
    referral['Patient']['Postcode'] = postcode
    referral['Header']['ReportingPeriodStartDate'] = ts
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.OrgIDICBRes
