from datetime import date, datetime
from typing import Union, List

from dsp.common.epma_national_expressions import (
    DeriveWhenFiltered,
    ReportingPeriodStartMonth,
    ValidateTreatmentSiteOdsCode,
    TreatmentTrustODScode,
    CheckLegallyRestrictedDoB,
    NHSNumberPdsCheck,
    NotSensitiveAndPDSCheckIsOne,
    GetIMDDecileEPMA,
    GetIMDDecileYearEPMA,
    AgeAtDateEPMA,
    DeriveStandardFiltered,
    DeriveDoseUnitFiltered,
)
from dsp.common.expressions import Literal
from dsp.common.structured_model import SubmittedAttribute

import pytest


@pytest.mark.parametrize(
    "ReportingPeriodStartDate, expected_RP_START_MONTH",
    [
        (date(2020, 1, 17), 202001),
        (date(2019, 7, 15), 201907),
        (date(2015, 12, 21), 201512),
        (None, None),
    ],
)
def test_RP_START_MONTH(ReportingPeriodStartDate, expected_RP_START_MONTH):
    rpsm = ReportingPeriodStartMonth(Literal(ReportingPeriodStartDate)).resolve_value(
        None
    )

    assert rpsm == expected_RP_START_MONTH


@pytest.mark.parametrize(
    "orgSiteIdentifier_of_treatment, point_in_time, expected",
    [
        ("RX101", date(2020, 1, 1), "01"),  # An NHS Trust Site
        (
            "RAE",
            date(2020, 1, 1),
            "02",
        ),  # An ODS code which is not a NHS Trust Site
        (
            "RX101",
            date(1990, 1, 1),
            "03",
        ),  # An ODS Code which is valid but has an invalid point in time
        (
            "ABC12",
            date(2020, 1, 1),
            "04",
        ),  # An ODS code that cannot be found in the ODS Reference data
        (None, date(2020, 1, 1), "05"),  # A Missing ODS code
    ],
)
def test_ValidateTreatmentSiteOdsCode(
    orgSiteIdentifier_of_treatment: str,
    point_in_time: date,
    expected: str,
):

    vtsoc = ValidateTreatmentSiteOdsCode(
        Literal(orgSiteIdentifier_of_treatment),
        Literal(point_in_time),
    ).resolve_value(None)

    assert vtsoc == expected


@pytest.mark.parametrize(
    "treatment_trust_ods_check, org_site_identifier_of_treatment, point_in_time, expected",
    [
        (
            "01",
            "RX101",
            date(2020, 1, 1),
            "RX1",
        ),  # A valid Site Trust with a valid point in time
        (
            "02",
            "RAE",
            date(2020, 1, 1),
            "RAE",
        ),  # A valid NHS Trust with a valid point in time
        (
            "03",
            "RX101",
            date(1900, 1, 1),
            None,
        ),  # A valid NHS Trust but had an invalid point in time
        (
            "01",
            "A0A3Z",
            date(2023, 3, 1),
            "RDY"
        ),  # A valid Site Trust with a valid point in time but with a syntactically different Trust ID
        ("04", None, date(2020, 1, 1), None),  # A missing Trust value
    ],
)
def test_TreatmentTrustODScode(
    treatment_trust_ods_check: str,
    org_site_identifier_of_treatment: str,
    point_in_time: date,
    expected: str,
):

    ttoc = TreatmentTrustODScode(
        Literal(treatment_trust_ods_check),
        Literal(org_site_identifier_of_treatment),
        Literal(point_in_time),
    ).resolve_value(None)

    assert ttoc == expected


@pytest.mark.parametrize(
    "nhs_no_legally_restricted, dob, expected",
    [
        ("5551000737", date(2025, 1, 17), date(2025, 1, 17)),  # Expected
        ("Removed", date(2025, 1, 1), None),  # Legally restricted
    ],
)
def test_CheckLegallyRestrictedDoB(
    nhs_no_legally_restricted: str, dob: date, expected: date
):
    clr_dob = CheckLegallyRestrictedDoB(
        Literal(nhs_no_legally_restricted), Literal(dob)
    ).resolve_value(None)

    assert clr_dob == expected


@pytest.mark.parametrize(
    "matched_confidence_percentage, person_birth_date, nhs_number, nhs_number_legally_restricted, expected",
    [
        (100.00, date(2025, 1, 1), "5551000737", None, "01"),  # Passed Cross-Check
        (None, date(2025, 1, 1), "5551000737", None, "02"),  # Failed Cross-Check
        (100.00, None, "5551000737", None, "03"),  # PersonBirthDate is null
        (100.00, date(2025, 1, 1), None, None, "04"),  # NHSNumber is null
        (
            100.00,
            date(2025, 1, 1),
            "5551000737",
            "Removed",
            "05",
        ),  # Record contains legally restricted data
    ],
)
def test_NHSNumberPdsCheck(
    matched_confidence_percentage: float,
    person_birth_date: date,
    nhs_number: str,
    nhs_number_legally_restricted: str,
    expected: str,
):

    if isinstance(matched_confidence_percentage, SubmittedAttribute):
        record_result = NHSNumberPdsCheck(
            matched_confidence_percentage,
            Literal(person_birth_date),
            Literal(nhs_number),
            Literal(nhs_number_legally_restricted),
        ).resolve_value(None)
    else:
        record_result = NHSNumberPdsCheck(
            Literal(matched_confidence_percentage),
            Literal(person_birth_date),
            Literal(nhs_number),
            Literal(nhs_number_legally_restricted),
        ).resolve_value(None)

    assert record_result == expected


@pytest.mark.parametrize(
    "nhs_number_legally_restricted, nhs_number_pds_check, expected",
    [
        ("5551000958", "01", True),  # Both valid
        ("Removed", "05", False),  # Both invalid
        ("5551000958", "02", False),  # PDS Check not 1
    ],
)
def test_NotSensitiveAndPDSCheckIsOne(
    nhs_number_legally_restricted: str,
    nhs_number_pds_check: str,
    expected: bool,
):
    assert (
        NotSensitiveAndPDSCheckIsOne(
            Literal(nhs_number_legally_restricted), Literal(nhs_number_pds_check)
        ).resolve_value(None)
        == expected
    )


@pytest.mark.parametrize(
    "lsoa_expr, point_in_time_expr, imd_year, alt_point_in_time_expr, lsoa_year, expected",
    [
        # Derive IMDDecile from a valid LSOA, point in time and in a relevant IMD Year
        ("E01033015", datetime(2020, 1, 1, 12, 0, 0, 0), "2019", None, 2011, "08"),
        # Derive IMDDecile from a valid LSOA, alternative point in time and in a relevant IMD Year
        ("E01033015", None, "2019", datetime(2020, 1, 1, 12, 0, 0, 0), 2011, "08"),
        # Unable to derive IMDDecile due to missing LSOA
        (None, datetime(2020, 1, 1, 12, 0, 0, 0), "2019", None, 2011, None),
        # # Unable to derive IMDDecile due to missing point in time(s) #? Needs confirming
        # ("E01033015", None, 2019, None, 2011, None),
        # Unable to derive IMDDecile due to missing IMD Year
        ("E01033015", datetime(2020, 1, 1, 12, 0, 0, 0), None, None, 2011, None),
    ],
)
def test_GetIMDDecileEPMA(
    lsoa_expr: str,
    point_in_time_expr: Union[date, datetime],
    imd_year: Union[int, str],
    alt_point_in_time_expr: Union[date, datetime],
    lsoa_year: int,
    expected: Union[str, int, None],
):

    imd_decile_code = GetIMDDecileEPMA(
        Literal(lsoa_expr),
        Literal(point_in_time_expr),
        Literal(imd_year),
        Literal(alt_point_in_time_expr),
        lsoa_year,
    ).resolve_value(None)

    assert imd_decile_code == expected


@pytest.mark.parametrize(
    "lsoa, date_used_to_derive_imd_decile, alt_date_used_to_derive_imd_decile, expected",
    [
        # Valid decile version which has a 2015
        (
            "E01033010",
            datetime(2020, 1, 1, 12, 0, 0, 0),
            datetime(2020, 1, 1, 12, 0, 0, 0),
            "2015",
        ),
        # Valid decile version which has a 2015 and uses alt date
        ("E01033010", None, datetime(2020, 1, 1, 12, 0, 0, 0), "2015"),
        # Nothing valid
        (None, None, None, None),
    ],
)
def test_GetIMDDecileYear(
    lsoa: str,
    date_used_to_derive_imd_decile: Union[date, datetime],
    alt_date_used_to_derive_imd_decile: Union[date, datetime],
    expected: str,
):
    get_lowest_imd_date = GetIMDDecileYearEPMA(
        Literal(lsoa),
        Literal(date_used_to_derive_imd_decile),
        Literal(alt_date_used_to_derive_imd_decile),
    ).resolve_value(None)

    assert get_lowest_imd_date == expected


@pytest.mark.parametrize(
    "date_of_birth, medication_date, backup_medication_date, expected",
    [
        # Expected Age with medication date
        (
            date(1980, 8, 12),
            datetime(2021, 1, 10, 12, 0, 0, 0),
            datetime(2021, 1, 10, 12, 0, 0, 0),
            40,
        ),
        # Expected Age with backup medication date
        (date(1980, 8, 12), None, datetime(2021, 1, 10, 12, 0, 0, 0), 40),
        # Expected Age with no dates available
        (date(1980, 8, 12), None, None, None),
    ],
)
def test_AgeAtDateEPMA(
    date_of_birth: date,
    medication_date: Union[date, datetime],
    backup_medication_date: Union[date, datetime],
    expected: int,
):

    age = AgeAtDateEPMA(
        Literal(date_of_birth),
        Literal(medication_date),
        Literal(backup_medication_date),
    ).resolve_value(None)

    assert age == expected


@pytest.mark.parametrize(
    "column_to_check, non_filtered_value, is_snomed_ct_filtered, expected",
    [
        ("01", "tablet", True, "tablet"),
        ("02", "tablet", True, "tablet"),
        ("03", "tablet", False, "tablet"),
        ("04", "tablet", False, "tablet"),
        ("01", "tablet", False, None),
        ("03", "tablet", True, None),
    ],
)
def test_DeriveStandardFiltered(
    column_to_check: str,
    non_filtered_value: str,
    is_snomed_ct_filtered: bool,
    expected: Union[str, None],
):
    standard_filtered = DeriveStandardFiltered(
        Literal(column_to_check), Literal(non_filtered_value), is_snomed_ct_filtered
    ).resolve_value(None)

    assert standard_filtered == expected


@pytest.mark.parametrize(
    "dose_unit, dose_unit_check, expected",
    [
        ("percent / 100 WBC", "01", "percent / 100 WBC"),
        (
            "Milligram/meter2/day (qualifier value)",
            "03",
            "Milligram/meter2/day (qualifier value)",
        ),
        ("IamNotaValidMeasurement", "05", None),
        (None, "06", None),
    ],
)
def test_DeriveDoseUnitFiltered(
    dose_unit: str, dose_unit_check: str, expected: Union[str, None]
):
    dose_unit_filtered = DeriveDoseUnitFiltered(
        Literal(dose_unit), Literal(dose_unit_check)
    ).resolve_value(None)

    assert dose_unit_filtered == expected


@pytest.mark.parametrize(
    "when_values, when_check_values, expected",
    [
        (
            ["HS", "WAKE", "MORN.early"],
            ["02", "02", "02"],
            ["HS", "WAKE", "MORN.early"],
        ),  # valid fhir values
        (
            ["when upset stomach, feeling ill or after regurgitate"],
            ["04"],
            ["when upset stomach, feeling ill or after regurgitate"],
        ),  # valid whitelist values
        (
            [
                "Absolute Nonsense",
                "In the morning, but before 11:47am",
                "isdhfasfodhuoda",
            ],
            ["05", "05", "05"],
            [None, None, None],
        ),  # neither fhir or whitelist
        (
            ["HS", "when upset stomach, feeling ill or after regurgitate", "abc.123"],
            ["02", "04", "05"],
            ["HS", "when upset stomach, feeling ill or after regurgitate", None],
        ),  # Mixture of fhir, whitelist and neither
        (
            ["hs", "wAKE", "MORN..early"],
            ["05", "05", "05"],
            [None, None, None],
        ),  # invalid fhir values
        ([None], [None], [None]),  # Empty Arrays
    ],
)
def test_DeriveWhenFiltered(
    when_values: List[str],
    when_check_values: List[str],
    expected: List[Union[str, None]],
):
    when_filtered = DeriveWhenFiltered(
        Literal(when_values), Literal(when_check_values)
    ).resolve_value(None)

    assert when_filtered == expected
