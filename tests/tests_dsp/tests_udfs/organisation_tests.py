import datetime
from typing import List

import mock
import pytest

from dsp.model.ods_record import ODSRecordPaths, ODSRecord
from dsp.model.ods_record_codes import ODSRoleScope, ODSRelType
from dsp.udfs import ccg_code_from_gp_practice_code, organisation_name, is_practice_code_valid, gp_practice_name, \
    is_site_code_valid, is_valid_ref_org_active
from dsp.udfs.organisation import DEFAULT_SHA, region_code_from_ccg_code, sha_code_from_site_code, organisation_exists, \
    pct_code_from_gp_practice_code, is_valid_ccg, is_valid_school, postcode_from_org_code, is_valid_org_role, \
    find_related_org_with_role, is_valid_gp_practice_active, is_valid_site_code_active, organisation_exists_anytime, \
    site_code_description, stp_code_from_ccg_code, get_org_role_types, provider_code_from_site_code, \
    icb_code_from_sub_icb_code, sub_icb_code_from_gp_practice_code

@pytest.mark.parametrize("site_code, point_in_time, expected", [
    ("FAKE", 20170101, DEFAULT_SHA),
    ("RR801", 19980101, DEFAULT_SHA),
    ("RR801", 19990101, 'QDH'),
    ("RR801", 20170101, DEFAULT_SHA),
    ("NDR05", 20170101, DEFAULT_SHA)
])
def test_derive_sha_code_from_site_code(site_code, point_in_time, expected):
    sha_code = sha_code_from_site_code(site_code, point_in_time)

    assert expected == sha_code


@pytest.mark.parametrize("gp_code, point_in_time, expected", [
    ("F85678", 20170101, "07X"),
    ("B81616", 20170101, "03F"),
    ("B81617", 20170101, "03K"),
    ("B81619", 20170101, "02Y"),
    ("B81620", 20170101, "03H"),
    ("B81622", 20170101, "02Y"),
    ("B81628", 20170101, "03K"),
    ("B81631", 20170101, "03F"),
    ("F85678", 20120401, None),
    (None, 20170101, None),
    ("A12345", 20120401, None),
    ("A81002", 20150501, "00K")
])
def test_ccg_code_from_gp_practice_code(gp_code, point_in_time, expected):
    ccg_code = ccg_code_from_gp_practice_code(gp_code, point_in_time)
    assert expected == ccg_code


@pytest.mark.parametrize("gp_code, point_in_time, expected", [
    ("F85678", 20170101, "07X"),
    ("B81616", 20170101, "03F"),
    ("B81617", 20170101, "03K"),
    ("B81619", 20170101, "02Y"),
    (None, 20170101, None),
])
def test_sub_icb_code_from_gp_practice_code(gp_code, point_in_time, expected):
    sub_icb_code = sub_icb_code_from_gp_practice_code(gp_code, point_in_time)
    assert expected == sub_icb_code


@pytest.mark.parametrize("sub_icb, point_in_time, expected", [
    ("00L", 20200331, "Q74"),
    ("03K", 20200331, "Q72"),
    ("03N", 20200331, "Q72"),
    ("15E", 20220801, "QHL"),
    (None, 20170101, None)
])
def test_icb_code_from_sub_icb_code(sub_icb, point_in_time, expected):
    icb_code = icb_code_from_sub_icb_code(sub_icb, point_in_time)
    assert expected == icb_code


@pytest.mark.parametrize("gp_code, point_in_time, expected", [
    ("F85678", 20180401, True),
    ("B81616", None, True),
    ("A85683", 20180401, False),
    ('W96015', 20180627, True),
    ("RR801", 20180401, False),  # not valid GP code
    ("RR801", None, False),  # not valid GP code
    ("V81997", 20180401, True),
    ("V81998", 20180401, True),
    ("V81999", None, True),
    (None, 20180401, False),
    ("", 20180401, False),
    (" ", 20180401, False),
    ("A81015", None, True),
    ("A81015", 20210401, False),
    ("A81015", 20180401, True),
    ("P81712", None, True),
    ("P81712", 20150101, True),
    ("P81712", 20180801, False),
])
def test_is_practice_code_valid(gp_code, point_in_time, expected):
    is_valid = is_practice_code_valid(gp_code, point_in_time)
    assert expected == is_valid


@pytest.mark.parametrize("gp_code, point_in_time, expected", [
    ("F85678", 20180401, True),
    ("F85678", 19890101, False),
    ("B81616", 20130401, True),
    ("A85683", 20180401, True),  # invalid org nothing to check if inactive
    ('W96015', 20180627, True),
    ("RR801", 20180401, True),  # invalid org code nothing to check if active
    ("V81997", 20180401, True),  # valid default code
    ("V81998", 20180401, True),  # valid default code
    ("V81999", None, False),  # point in time not given
    (None, 20180401, False),  # null prg code to validate
    ("", 20180401, False),  # empty org code to validate
    (" ", 20180401, False),  # whitespace org code to validate
    ("P81712", 20180704, True),  # GP code valid till 20180704
    ("P81712", 20180705, False), # GP code valid till 20180704
    (None, None, False) # Rule should return False if no date and org provided
])
def test_is_valid_gp_practice_active(gp_code, point_in_time, expected):
    is_valid = is_valid_gp_practice_active(gp_code, point_in_time)
    assert expected == is_valid


@pytest.mark.parametrize("referring_org_code, point_in_time, expected", [
    ("V81999", None, False),  # point in time not given
    (None, 20180401, False),  # null org code to validate
    ("", 20180401, False),  # empty org code to validate
    (" ", 20180401, False),  # whitespace org code to validate
    ("P81712", 20180704, True),  # GP code valid with active role until 20180704
    ("P81712", 20180705, False), # GP code valid with inactive role
    ("V81998", 19000101, True),  # invalid default code, should return True as role not defined
    ("X99998", 19000101, True),  # valid default code, should return True for any point in time
    ("RR8", 19990401, True), # Valid org with ref org role active
    ("RR8", 19970401, False), # Valid org with ref org role inactive
    ("5PW", 20130101, True), # Valid org with ref org role active
    ("5PW", 20140101, False), # Valid org with ref org role inactive
    (" 5PW ", 20140101, False) # Check whitespace in valid code with active ref role
])
def test_is_valid_ref_org_active(referring_org_code, point_in_time, expected):
    is_valid = is_valid_ref_org_active(referring_org_code, point_in_time)
    assert expected == is_valid


@pytest.mark.parametrize("gp_code, expected", [
    ("F85678", "THE TOWN SURGERY LTD"),
    ("B81616", "DR GT HENDOW'S PRACTICE"),
    ("A85683", None),
    ("S99999", "Unknown"),
    ("S30063", "Grampian Out Of Hours Service"),
    ("S30152", "Virtual Practice Only"),
    ("S99942", "PATIENTS REGISTERED WITH"),
    ("S99957", "PATIENTS NOT REGISTERED"),
    ("S99961", "PATIENTS WHERE PRACTICE C"),
    ("S99976", "BRITISH ARMED FORCES PATIENT"),
    ("S99995", "PATIENTS REGISTERED WITH"),
    ("S99999", "Unknown"),
    ("V81997", "Not Registered"),
    ("V81998", "Not Applicable"),
    ("V81999", "Not Known"),
    (None, None),
    ("", None),
    (" ", None)
])
def test_gp_practice_name(gp_code, expected):
    gp_name = gp_practice_name(gp_code)
    assert expected == gp_name


@pytest.mark.parametrize("ccg_code, expected", [
    ("07X", "NHS NORTH CENTRAL LONDON ICB - 93C"),
    ("03F", "NHS HUMBER AND NORTH YORKSHIRE ICB - 03F"),
    ("03K", "NHS HUMBER AND NORTH YORKSHIRE ICB - 03K"),
    ("02Y", "NHS HUMBER AND NORTH YORKSHIRE ICB - 02Y"),
    ("03H", "NHS HUMBER AND NORTH YORKSHIRE ICB - 03H"),
    ("", None),
    (None, None)
])
def test_organisation_name(ccg_code, expected):
    ccg_name = organisation_name(ccg_code)
    assert expected == ccg_name


@pytest.mark.parametrize(
    ["site_code", "point_in_time", "follow_successions", "exists"],
    [
        ("FAKE", 20180101, True, False),  # fake code
        ("RR801", 19900101, True, False),  # exists since 1/4/1998
        ("RR801", None, True, True),  # as above, NHS Trust Site
        ("AXT04", None, True, True),  # as above, Independent Sector H/S Provider Site
        ("NYNWA", None, True, True),  # as above, Independent Sector H/S Provider Site
        ("89999", 20180101, True, False),  # default site code should not be part of organisation ref data
        ("89997", 19900101, True, False),  # default site code should not be part of organisation ref data
        ("XMD00", None, True, False),  # default site code should not be part of organisation ref data
        ("X98", None, True, False),  # default site code should not be part of organisation ref data
        ("Q99", None, True, False),  # default site code should not be part of organisation ref data
        ("R9998", None, True, False),  # default site code should not be part of organisation ref data
        ("RR1", 20180501, False, False),  # existing ogranisation closed before that date, without following of succession
        ("RR1", 20180501, True, True),  # existing ogranisation closed before that date, with following of succession
        ("", None, True, False),
        (None, None, True, False),
    ]
)
def test_organisation_exists(site_code, point_in_time, follow_successions, exists):
    assert organisation_exists(site_code, point_in_time, follow_successions=follow_successions) is exists

@pytest.mark.parametrize("site_code, exists", [
    ("FAKE", False),  # fake code
    ("RR801", True),  # exists since 1/4/1998
    ("RR801", True),  # as above, NHS Trust Site
    ("AXT04", True),  # as above, Independent Sector H/S Provider Site
    ("NYNWA",  True),  # as above, Independent Sector H/S Provider Site
    ("RR864", True),  # valid from 20050401 to 20060930
    ("INVALID", False),  # fake code
    ("89999", True),  # default site code should not be part of organisation ref data
    ("89997", True),  # default site code should not be part of organisation ref data
    ("R9998", True),  # default site code should not be part of organisation ref data
    ("", False),
    (None, False),
])
def test_organisation_exists_anytime(site_code, exists):
    assert organisation_exists_anytime(site_code) is exists


@pytest.mark.parametrize("site_code, point_in_time, follow_succession, exists", [
    ("01N", 20170401, True, True),  # Stopped existing but has succession
    ("01N", 20170401, False, False),  # Stopped existing but has succession
])
def test_organisation_exists_not_following_succession(site_code, point_in_time, follow_succession, exists):
    assert organisation_exists(site_code, point_in_time, follow_successions=follow_succession) is exists


@pytest.mark.parametrize("code, default_org_list, exists", [
    ('ZZ888', ['ZZ888', 'ZZ999'], True),
    ('RA1', ['ZZ', 'RA2'], True),
    ("FAKE", ['ZZ', 'RA2'], False),
])
def test_default_org_present(code, default_org_list: List[str], exists):
    assert organisation_exists(code, default_org_list=default_org_list) is exists


@pytest.mark.parametrize("site_code, expected", [
    ("89999", "Non-NHS UK Provider"),
    ("", None),
    (None, None),
    ("RR801", "LEEDS GENERAL INFIRMARY"),
    ("NTG15", "MARIE STOPES PREGNANCY ADVICE CENTRE (RAVENSCROFT MEDICAL CENTRE)"),
])
def test_site_code_description(site_code, expected):
    site_description = site_code_description(site_code)
    assert expected == site_description





@pytest.mark.parametrize("gp_code, point_in_time, expected", [
    ("F85678", 20170101, "5C1"),
    ("B81616", 20170101, "5NX"),
    ("B81616", 20090101, "5NX"),
    ("B81616", 20050101, "5E5"),
    (None, 20170101, None),
    ("A12345", 20120401, None)
])
def test_pct_code_from_gp_practice_code(gp_code, point_in_time, expected):
    pct_code = pct_code_from_gp_practice_code(gp_code, point_in_time)
    assert expected == pct_code


@pytest.mark.parametrize("ccg_code, point_in_time, expected", [
    (None, 20180101, False),
    ("", 20180101, False),
    ("RR8", 20180101, False),
    ("11T", 20180101, True),
    ("99P", 20180101, True),
])
def test_is_valid_ccg(ccg_code, point_in_time, expected):
    assert is_valid_ccg(ccg_code, point_in_time) == expected


@pytest.mark.parametrize(
    ["site_code", "point_in_time", "follow_successions", "exists"],
    [
        ("89999", 20180101, True, True),
        ("89997", 19900101, True, True),
        ("R9998", None, True, True),
        ("AXT04", None, True, True),  # as above, Independent Sector H/S Provider Site
        ("NTG15", 20050401, True, True),
        ("FALSE", None, True, False),
        ("RR1", 20200501, False, False),
        ("RR1", 20200501, True, True),
        ("", None, True, False),
        (None, None, True, False),
    ]
)
def test_is_site_code_valid(site_code, point_in_time, follow_successions, exists):
    assert is_site_code_valid(site_code, point_in_time, follow_successions=follow_successions) is exists


@pytest.mark.parametrize("site_code, point_in_time, exists", [
    ("FAKE", 20180101, True),  # fake code
    ("RR801", 1997061, False),  # exists since 1/4/1998
    ("RR801", 20000101, True),  # exists since 1/4/1998
    ("NTG15", 19990101, False),  # valid in 20050401 to 20060930
    ("", 20180401, False),  # empty org code to validate
    (" ", 20180401, False),  # whitespace org code to validate
    ("RR801", None, False),  # empty point in time validate
    ("P81712", 20180704, False),  # GP code valid with active role until 20180704
    ("89999", 20210615, True),  # Default
    ("89997", 20210615, True),  # Default
    ("R9998", 20210615, True),  # Default
])
def test_is_valid_site_code_active(site_code, point_in_time, exists):
    assert is_valid_site_code_active(site_code, point_in_time) is exists


def test_ods_record_provider_get_org():
    print(is_practice_code_valid('B81616', 20130401))


@pytest.mark.parametrize("code, point_in_time, expected", [
    ("EE100001", 20151127, True),
    ("EE100001", 18000101, False),
    ("INVALID", 20151127, False),
    (None, 20181010, False),
    ("EE100001", None, True),
    ("EE138529", 20181010, True),
    ("EE138529", 20161001, False)
])
def test_is_valid_school(code, point_in_time, expected):
    assert is_valid_school(code, point_in_time) == expected


@mock.patch("testdata.ref_data.providers.ODSProvider._get")
def test_postcode_from_orgcode(m_get):
    # ODSProvider._get wraps the .get function precisely to enable this kind of mocking.

    test_org_code = "TEST123"
    postcode2011 = "LS1 6AE"
    postcode2012 = "LS11 5BZ"
    postcode2013 = "LS1 4HR"

    # A dummy record will always be returned with good historical postcodes
    m_get.return_value = ODSRecord(
        {
            ODSRecordPaths.POSTCODE: "",
            ODSRecordPaths.EFFECTIVE_FROM: 20110101,
            ODSRecordPaths.HISTORICAL_POSTCODES: [
                {"from": 20110101, "to": 20111231, "postcode": postcode2011},
                {"from": 20120101, "to": 20121231, "postcode": postcode2012},
                {"from": 20130101, "to": None, "postcode": postcode2013}
            ],
            ODSRecordPaths.NAME: "TESTY MC TESTERSON",
            ODSRecordPaths.ORG_CODE: test_org_code,
            ODSRecordPaths.RELS: [],
            ODSRecordPaths.ROLES: [],
            ODSRecordPaths.SUCCESSIONS: [],
        }
    )

    # Before the earliest one, return None
    assert postcode_from_org_code(test_org_code, point_in_time="2010-01-01") == None

    # Check inclusive of from/to dates, use ints
    assert postcode_from_org_code(test_org_code, point_in_time=20110101) == postcode2011
    assert postcode_from_org_code(test_org_code, point_in_time=20111231) == postcode2011

    # Check inclusive of from/to dates, using datetime
    assert postcode_from_org_code(test_org_code, point_in_time=datetime.datetime(2012, 1, 1)) == postcode2012
    assert postcode_from_org_code(test_org_code, point_in_time=datetime.datetime(2012, 12, 31)) == postcode2012

    # Check future date returns postcode with record with "to": None
    assert postcode_from_org_code(test_org_code, point_in_time=datetime.datetime(2020, 1, 1).date()) == postcode2013


@mock.patch("testdata.ref_data.providers.ODSProvider._get")
def test_postcode_from_orgcode_None(m_get):
    # ODSProvider._get wraps the .get function precisely to enable this kind of mocking.
    # A dummy record will always be returned with good historical postcodes
    m_get.return_value = None

    test_org_code = "TEST123"
    assert postcode_from_org_code(test_org_code, point_in_time="2010-01-01") == None
    assert postcode_from_org_code(test_org_code) == None


@pytest.mark.parametrize("point_in_time, expected", [
    (20070101, None),  # earlier than the earliest
    (20180101, "BB11 4LY"),  # 1st postcode
    (20190711, "BB11 4AW"),  # 2nd postcode
    (30000101, "BB11 4AW"),  # Later than the last postcode close
])
def test_postcode_from_orgcode_real_data_closed_historical_bounds(point_in_time, expected):
    # Check the postcode from organisation
    test_org_code = "A809"
    assert postcode_from_org_code(test_org_code, point_in_time=point_in_time) == expected


@mock.patch("testdata.ref_data.providers.ODSProvider._get")
def test_postcode_from_orgcode_no_historical(m_get):
    test_org_code = 'ABCDE'

    m_get.return_value = ODSRecord(
        {
            ODSRecordPaths.POSTCODE: '',
            ODSRecordPaths.EFFECTIVE_FROM: 20191122,
            ODSRecordPaths.HISTORICAL_POSTCODES: [],
            ODSRecordPaths.NAME: "TESTY MC TESTERSON",
            ODSRecordPaths.ORG_CODE: test_org_code,
            ODSRecordPaths.RELS: [],
            ODSRecordPaths.ROLES: [],
            ODSRecordPaths.SUCCESSIONS: [],
        }
    )

    assert postcode_from_org_code(test_org_code, '20191101') == None


# test is_valid_org_role
@pytest.mark.parametrize("org_code, point_in_time, role_types, expected", [
    ("08H", "2020-31-12", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], True),
    ("08H", None, [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], True),
    # point_in_time = None still produces appropriate output
    ("08H", "2020-31-12", [ODSRoleScope.CANCER_NETWORK, ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], True),
    ("08H", "2020-31-12", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP, ODSRoleScope.CANCER_NETWORK], True),
    ("08H", "2020-31-12", [ODSRoleScope.CANCER_NETWORK], False),
    ("08H", "2020-31-12", [ODSRoleScope.CANCER_NETWORK, ODSRoleScope.CLINICAL_COMMISSIONING_GROUP_SITE], False),
    # org 08H has neither of these roles so should return False
    ("13TAE", "2014-12-11", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP_SITE], True),
    ("13TAE", 20141211, [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP_SITE], True),  # test different date formats
    ("13TAE", "2014-12-10", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP_SITE], False),
    # org 13TAE had the above role from 11/12/2014 only so the latter case should return False
    (" 08H", "2020-31-12", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], False),
    # Leading whitespace with valid code should fail validation
    ("08H ", "2020-31-12", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], False),
    # Trailing whitespace with valid code should fail validation
    (" 08H ", "2020-31-12", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], False),
    # Leading and trailing whitespace with valid code should fail validation
    ("", "2020-31-12", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], False),
    # Empty string should fail validation
    (" ", "2020-31-12", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], False),
    # Whitespace should fail validation
    (None, "2020-31-12", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], False),
    # Null/None code should fail validation
    ("03A", "2022-05-01", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], True),
])
def test_is_valid_org_role(org_code, point_in_time, role_types, expected):
    assert is_valid_org_role(org_code, point_in_time, *role_types) == expected


@pytest.mark.parametrize("org_code, point_in_time, role_types, follow_successions, expected", [
    ("03A", "2022-05-01", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], True, True),
    ("03A", "2022-05-01", [ODSRoleScope.CLINICAL_COMMISSIONING_GROUP], False, False),
])
def test_is_valid_org_role_test_successors(org_code, point_in_time, role_types, follow_successions, expected):
    assert is_valid_org_role(org_code, point_in_time, *role_types, follow_successions=follow_successions) == expected


# test find_related_org_with_role
@pytest.mark.parametrize("org_code, rel_type, role_ids, from_org_type, point_in_time, expected", [
    ("8AM58", ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, [ODSRoleScope.REGION_GEOGRAPHY], None, 20130601, "Q45"),
    ("8AM58", "RE5", ["RO210"], "NN", 20130601, "Q45"),
    ("8AM58", "RE5", ["RO210"], "NN", 20150601, "Q74"),
    ("8AM58", "RE5", ["RO210"], "NN", None, None),  # Checks point_in_time = None returned as there are no active rels
    ("8AM58", "RE5", ["RO210"], "NN", 19930101, None),  # Checks an invalid point_in_time returns None
    ("27T", "RE5", ["RO209"], "CC", 20200401, "Y62"),
    ("27T", "RE5", ["RO209"], None, 20200401, "Y62"),  # Checks from_org_type = None returns appropriate org
    ("EQZ97", ODSRelType.IS_OPERATED_BY, ["RO111"], "DS", 19920331, "EQZ"),
    ("AYY01", "RE5", ["RO210"], "PP", None, None),
    # Checks correct relationship issued when there are multiple of the same role
    ("00FAD", "RE6", ["RO98"], "CD", 20150101, "00F"),  # correct org_code returned because point_in_time is valid
    ("00FAD", "RE6", ["RO98"], "CD", "2015-01-01", "00F"),  # test different date format
    ("00FAD", "RE6", ["RO98"], "CD", None, "13T"),  # None returned when point_in_time is None
    ("00FAD", "RE6", ["RO98"], "CD", 20160101, "13T"),  # None returned when point_in_time is invalid
    ("03A", ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, [ODSRoleScope.STRATEGIC_PARTNERSHIP], None, 20220501, "QWO"),
])
def test_find_related_org_with_role(org_code, rel_type, role_ids, from_org_type, point_in_time, expected):
    assert find_related_org_with_role(org_code, rel_type, role_ids, from_org_type, point_in_time) == expected


@pytest.mark.parametrize("org_code, rel_type, role_ids, from_org_type, point_in_time, follow_successions, expected", [
    ("03A", ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, [ODSRoleScope.STRATEGIC_PARTNERSHIP], None, 20220501, True, "QWO"),
    ("03A", ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, [ODSRoleScope.STRATEGIC_PARTNERSHIP], None, 20220501, False, None),
    ("03A", ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, [ODSRoleScope.STRATEGIC_PARTNERSHIP], None, 20200401, True, "QWO"),
    ("03A", ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, [ODSRoleScope.STRATEGIC_PARTNERSHIP], None, 20200401, False, "QWO"),
    ("03A", ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, [ODSRoleScope.REGION_GEOGRAPHY], None, 20130401, True, "Q52"),
    ("03A", ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, [ODSRoleScope.REGION_GEOGRAPHY], None, 20130401, False, "Q52"),
])
def test_find_related_org_with_role_successors(org_code, rel_type, role_ids, from_org_type, point_in_time,
                                               follow_successions, expected):
    assert find_related_org_with_role(org_code, rel_type, role_ids, from_org_type, point_in_time,
                                      follow_successions) == expected


@pytest.mark.parametrize("org_code, point_in_time, expected", [
    ("02P", 20200402, "QF7"),
    ("02P", None, "QF7"),
    ("DOES_NOT_EXIST", 20200402, None),
    (None, 20200402, None),
])
def test_stp_code_from_ccg_code(org_code, point_in_time, expected):
    assert stp_code_from_ccg_code(org_code, point_in_time) == expected


@pytest.mark.parametrize("org_code, point_in_time, expected", [
    ("02P", 20200402, "Y63"), # Should use CCST, STCE
    ("13R", 20200402, "Y56"), # Should use CCCE
    ("13Q", 20200402, "X24"), # Should use CCOA
    ("DOES_NOT_EXIST", 20200402, None),
    (None, 20200402, None),
])
def test_region_code_from_ccg_code(org_code, point_in_time, expected):
    assert region_code_from_ccg_code(org_code, point_in_time) == expected


@pytest.mark.parametrize("org_code, expected_org_roles", [
    ("FAKE", []),
    ("02P", ["RO98"]),
    ("13R", ["RO98"]),
    ("06", [""])
])
def test_get_org_roles(org_code, expected_org_roles):
    org_roles = get_org_role_types(org_code)

    assert org_roles == expected_org_roles


@pytest.mark.parametrize("code, point_in_time, expected", [
    ("RGD01", 20200101, "RGD"),  # NHS Trust Site in NHS Trust
    ("RGD01", 19950101, "RGD01"),  # Where point in time is invalid
    ("RGD02", 20200101, "RGD"),  # Another site in same NHS trust
    ("R1A01", 20200101, "R1A"),  # Another site and trust
    ("abc12", 20200101, None)  # Invalid code
])
def test_provider_code_from_site_code(code, point_in_time, expected):
    provider_code = provider_code_from_site_code(code, point_in_time)

    assert provider_code == expected
