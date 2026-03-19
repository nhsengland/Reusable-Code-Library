import datetime
import pytest

from dsp.datasets.models.msds import PregnancyAndBookingDetails
from dsp.datasets.models.msds_base import _MotherDemog
from dsp.datasets.models.msds_tests.msds_helper_tests import pregnancy_and_booking_details, motherdemog



@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, 'N'),
    (datetime.date(2015, 5, 1), 'LS14JL', 'Y'),
    (datetime.date(2015, 5, 1), 'NQ999XX', 'N'),
    (datetime.date(2016, 10, 3), 'EH49DX', 'Y'),  # valid from 20160201 to 20170201
    (datetime.date(2017, 3, 1), 'EH49DX', 'N'),  # valid from 20160201 to 20170201
])
def test_valid_postcode_flag(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert expected == new_pregnancy_and_booking_details.Mother.ValidPostcodeFlag


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'E08000035'),  # "oslaua": [[20110201, "E08000035"]
    (datetime.date(2006, 5, 1), 'LS14JL', 'DA'),  # "oslaua": [[20060501,, "DA"]
    (datetime.date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (datetime.date(2016, 10, 3), 'EH4 9DX', 'S12000036'),  # valid from 20160201 to 20170201
    (datetime.date(2017, 3, 1), 'EH49DX', None),  # valid from 20160201 to 20170201
])
def test_lad_ua_mother(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.LAD_UAMother == expected


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'E01033015'),  # "lsoa11": [20150501, "E01033015"]
    (datetime.date(2014, 5, 1), 'LS14JL', 'E01033008'),  # "lsoa11": [20140201, "E01033008"]
    (datetime.date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (datetime.date(2016, 10, 3), 'EH4 9DX', 'S01008864'),  # valid from 20160201 to 20170201
    (datetime.date(2018, 10, 3), 'EH4 9DX', None),  # valid from 20160201 to 20170201
    (datetime.date(1990, 3, 1), 'CM40LZ', None),  # valid from 19980601 to 19991201
])
def test_lsoamother2011(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.LSOAMother2011 == expected


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 10, 1), 'LS14JL', '05'),  # "lsoa11": [20150501, "E01033015"]
    (datetime.date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (datetime.date(2016, 10, 3), 'EH4 9DX', None),  # valid from 20160201 to 20170201
    (datetime.date(2018, 10, 3), 'EH4 9DX', None),  # valid from 20160201 to 20170201
    (datetime.date(1990, 3, 1), 'CM40LZ', None),  # valid from 19980601 to 19991201
])
def test_rank_imd_decile_2015(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.Rank_IMD_Decile_2015 == expected

@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), 'LS14JL', 'E99999999'),  # "oscty": [20110201, "E99999999"]
    (datetime.date(2006, 6, 1), 'LS14JL', '00'),  # "oscty": [20060501, "00"]]
    (datetime.date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (datetime.date(2016, 10, 3), 'EH4 9DX', 'S99999999'),  # valid from 20160201 to 20170201
    (datetime.date(2018, 10, 3), 'EH4 9DX', None),  # valid from 20160201 to 20170201
    (datetime.date(1990, 3, 1), 'CM40LZ', None),  # valid from 19980601 to 19991201
])
def test_countymother(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.CountyMother == expected


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2017, 5, 1), 'LS14JL', 'E05001420'),  # "osward": [20110201, "E05001420"]
    (datetime.date(2006, 6, 1), 'LS14JL', 'GW'),  # "osward": [20060501, "GW"]
    (datetime.date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (datetime.date(2016, 10, 3), 'EH4 9DX', 'S13002587'),  # valid from 20160201 to 20170201 [20160501, "S13002587"]
    (datetime.date(2018, 10, 3), 'EH4 9DX', None),  # valid from 20160201 to 20170201
    (datetime.date(1990, 3, 1), 'CM40LZ', None),  # valid from 19980601 to 19991201
])
def test_electoralwardmother(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.ElectoralWardMother == expected


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2018, 6, 1), 'LS14JL', '15F'),  # "ccg": [20180501, "15F"]
    (datetime.date(2016, 6, 1), 'LS14JL', '03G'),  # "ccg": [20130401, "03G"]
    (datetime.date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (datetime.date(2016, 10, 3), 'EH4 9DX', '042'),  # valid from 20160201 to 20170201 [20160501, "042"]
    (datetime.date(2018, 10, 3), 'EH4 9DX', None),  # valid from 20160201 to 20170201
    (datetime.date(1990, 3, 1), 'CM40LZ', None),  # valid from 19980601 to 19991201
    (datetime.date(2023, 3, 31), 'WR11 8YA', '18C'),  # valid before 20230401
    (datetime.date(2023, 4, 1), 'WR11 8YA', None),  # valid after 20230401

])
def test_ccgresidencemother(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.CCGResidenceMother == expected


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2018, 6, 1), 'LS14JL', 'LS1'),
    (datetime.date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (datetime.date(2016, 10, 3), 'EH4 9DX', 'EH4'),  # valid from 20160201 to 20170201
    (datetime.date(2018, 10, 3), 'EH4 9DX', None),  # valid from 20160201 to 20170201
    (datetime.date(1990, 3, 1), 'CM40LZ', None),  # valid from 19980601 to 19991201
])
def test_postcodedistrictmother(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.PostcodeDistrictMother == expected

@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2018, 6, 1), 'LS14JL', None),
    (datetime.date(1990, 3, 1), 'ZZ99 3VZ', 'ZZ99 3VZ'),
    (datetime.date(1990, 3, 1), 'ZZ99 3WZ', 'ZZ99 3WZ'),
    (datetime.date(1990, 3, 1), 'ZZ99 1WZ', 'ZZ99 1WZ')
])
def test_defaultpostcode(pregnancy_and_booking_details, rep_period_start_date, postcode, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['Postcode'] = postcode
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.DefaultPostcode == expected
