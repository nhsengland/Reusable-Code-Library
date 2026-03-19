from datetime import date

import pytest

from dsp.datasets.models import csds
from dsp.datasets.models.csds_tests.csds_helper_tests import patient, mpi


@pytest.mark.parametrize("patient_post_code, expected_valid_postcode_flag", [
    ('LS14JL', 'Y'),
    ('LS1 4JL', 'Y'),
    ('LS157DJ', 'Y'),
    ('LS15 7DJ', 'Y'),
    ('bad postcode', 'N'),
    ('BS27 9AA', 'N'),  # postcode not valid after 2010-08-01
    (None, 'N')
])
def test_valid_postcode_flag(patient_post_code, expected_valid_postcode_flag):
    record = patient()
    record['Header']['RP_StartDate'] = date(2020, 1, 1)
    record['Postcode'] = patient_post_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_valid_postcode_flag == new_patient.ValidPostcode_Flag


@pytest.mark.parametrize("patient_post_code, expected_postcode_district", [
    ('LS14JL', 'LS1'),
    ('LS1 4JL', 'LS1'),
    ('LS157DJ', 'LS15'),
    ('LS15 7DJ', 'LS15'),
    ('bad postcode', None),
    ('EH4 9DX', None),  # postcode not valid after 2017-02-01
    (None, None)
])
def test_postcode_district_from_patient_postcode(patient_post_code, expected_postcode_district):
    record = patient()
    record['Header']['RP_StartDate'] = date(2016, 1, 1)
    record['Postcode'] = patient_post_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_postcode_district == new_patient.Postcode_District


@pytest.mark.parametrize("patient_post_code, expected_postcode_default", [
    (None, None),
    ('LS99 9ZZ', None),
    ('LS10 1LG', None),
    ('ZZ99 3VZ', 'ZZ99 3VZ')
])
def test_postcode_default_from_patient_postcode(patient_post_code, expected_postcode_default):
    record = patient()
    record['Header']['RP_StartDate'] = date(2020, 1, 1)
    record['Postcode'] = patient_post_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_postcode_default == new_patient.Postcode_Default


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_lsoa", [
    (date(2015, 5, 1), None, None),
    (date(2015, 5, 1), 'LS14JL', 'E01033015'),  # "lsoa11": [20150501, "E01033015"]
    (date(2014, 5, 1), 'LS14JL', 'E01033008'),  # "lsoa11": [20140201, "E01033008"]
    (date(2013, 5, 1), 'LS14JL', None),  # no lsoa before 20140201
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201. lsoa valid from 20160501
    (date(2016, 10, 3), 'EH4 9DX', 'S01008864'),  # postcode valid from 20160201 to 20170201.
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_lsoa_from_patient_postcode(rep_period_start_date, patient_post_code, expected_lsoa):
    record = patient()
    record['Header']['RP_StartDate'] = rep_period_start_date
    record['Postcode'] = patient_post_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_lsoa == new_patient.LSOA


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_county", [
    (date(2015, 5, 1), None, None),
    (date(2015, 5, 1), 'LS14JL', 'E99999999'),  # "oscty": [20110201, "E99999999"]
    (date(2006, 6, 1), 'LS14JL', '00'),  # "oscty": [20060501, "00"]]
    (date(2005, 6, 1), 'LS14JL', None),  # no county before 20060501
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', 'S99999999'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_county_from_patient_postcode(rep_period_start_date, patient_post_code, expected_county):
    record = patient()
    record['Header']['RP_StartDate'] = rep_period_start_date
    record['Postcode'] = patient_post_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_county == new_patient.County


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_la", [
    (date(2015, 5, 1), None, None),
    (date(2015, 5, 1), 'LS14JL', 'E08000035'),  # "oslaua": [20110201, "E08000035"]
    (date(2006, 6, 1), 'LS14JL', 'DA'),  # "oslaua": [20060501, "DA"]
    (date(2005, 6, 1), 'LS14JL', None),  # no la before 20060501
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', 'S12000036'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_la_from_patient_postcode(rep_period_start_date, patient_post_code, expected_la):
    record = patient()
    record['Header']['RP_StartDate'] = rep_period_start_date
    record['Postcode'] = patient_post_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_la == new_patient.LocalAuthority


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_ward", [
    (date(2015, 5, 1), None, None),
    (date(2015, 5, 1), 'LS14JL', 'E05001420'),  # "osward": [20110201, "E05001420"]
    (date(2006, 6, 1), 'LS14JL', 'GW'),  # "osward": [20060501, "GW"]
    (date(2005, 6, 1), 'LS14JL', None),  # no ward before 20060501
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', 'S13002587'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_ward_from_patient_postcode(rep_period_start_date, patient_post_code, expected_ward):
    record = patient()
    record['Header']['RP_StartDate'] = rep_period_start_date
    record['Postcode'] = patient_post_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_ward == new_patient.ElectoralWard


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_ccg_res", [
    (date(2015, 5, 1), None, None),
    (date(2018, 5, 1), 'LS14JL', '15F'),  # "ccg": [20180501, "15F"]
    (date(2013, 6, 1), 'LS14JL', '03G'),  # "ccg": [20130401, "03G"]
    (date(2005, 6, 1), 'LS14JL', None),  # no ccg before 20060501
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', '042'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_ccg_res_from_patient_postcode(rep_period_start_date, patient_post_code, expected_ccg_res):
    record = patient()
    record['Header']['RP_StartDate'] = rep_period_start_date
    record['Postcode'] = patient_post_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_ccg_res == new_patient.OrgID_CCG_Residence


@pytest.mark.parametrize("rep_period_start_date, residence_postcode, gp_practice_code, expected_distance", [
    (None, None, None, None),
    (date(2018, 10, 3), None, None, None),
    (date(2018, 10, 3), 'LS999ZZ', 'A81003', None),  # invalid postcode
    (date(2018, 10, 3), 'AL100AU', 'A81003', 332),
    (date(2018, 10, 3), 'E10 5NP', 'A81004', 342)
])
def test_gp_distance_from_home(rep_period_start_date, residence_postcode, gp_practice_code, expected_distance):
    record = patient()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Postcode'] = residence_postcode
    record['GPPracticeRegistrations'][0]['OrgID_GP'] = gp_practice_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_distance == new_patient.GPPracticeRegistrations[0].DistanceFromHome_GP


@pytest.mark.parametrize("gp_practice_code, expected_org_id", [
    ('A81004', '00M'),
    ('A81017', '00K'),
    ('D82001', '06V'),
    ('G82092', '99J'),
    ('bad practice code', None),
    (None, None)])
def test_org_id_of_ccg_from_gp_practice(gp_practice_code, expected_org_id):
    record = patient()
    record['Header']['ReportingPeriodStartDate'] = date(2020, 1, 1)
    record['GPPracticeRegistrations'][0]['OrgID_GP'] = gp_practice_code
    new_patient = csds.MPI(record)  # creates a table
    assert expected_org_id == new_patient.GPPracticeRegistrations[0].OrgID_CCG_GP


@pytest.mark.parametrize("rep_period_start_date, residence_postcode, site_id, expected_distance", [
    (None, None, None, None),
    (date(2018, 10, 3), None, None, None),
    (date(2018, 10, 3), 'LS999ZZ', 'A81003', None),  # invalid postcode
    (date(2018, 10, 3), 'AL100AU', 'A81003', 332),
    (date(2018, 10, 3), 'E10 5NP', 'A81004', 342)
])
def test_cont_loc_distance_from_home(rep_period_start_date, residence_postcode, site_id, expected_distance):
    record = patient()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Postcode'] = residence_postcode
    record['Referrals'][0]['CareContacts'][0]['Treatment_OrgSiteID'] = site_id
    new_patient = csds.MPI(record)  # creates a table
    assert expected_distance == new_patient.Referrals[0].CareContacts[0].DistanceFromHome_ContactLocation
