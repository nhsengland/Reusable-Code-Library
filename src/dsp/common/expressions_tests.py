import copy
from collections import namedtuple
from datetime import datetime, date
from decimal import Decimal
from typing import Callable, Dict, Optional, List, Union
import mock
from unittest.mock import Mock

import pytest

from dsp.datasets.common import Fields

from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import (
    referral as referral_mhsds,
)
from dsp.datasets.models.mhsds_v5 import Referral as Referral_MHSDS
from dsp.datasets.models.csds_v1_6 import MPI as CSDS_MPI
from dsp.datasets.models.csds_v1_6_tests.csds_v1_6_helper_tests import mpi as csds_mpi_data

# this import is to make pytest fixture available
# noinspection PyUnresolvedReferences

from dsp.datasets.models.msds import (
    PregnancyAndBookingDetails,
    CareActivity,
    CareContact,
)
from dsp.datasets.models.msds_tests.msds_helper_tests import (
    pregnancy_and_booking_details,
    care_activities,
    care_contacts,
)

# this import is to make pytest fixture available
# noinspection PyUnresolvedReferences

from dsp.model.ods_record import ODSRecord, ODSRecordPaths
from dsp.model.ons_record import ONSRecordPaths

from dsp.common.expressions import (
    AddToDate,
    AgeAtDate,
    AgeAtTime,
    ASQScoreBand,
    DefaultNone,
    BreastFeedingStatusMaster,
    Case,
    FileTypeExpression,
    FilterResolved,
    Find,
    FindFirst,
    First,
    GetAttr,
    ICBFromPostcode,
    If,
    Literal,
    MinFilterResolved,
    MinResolved,
    Select,
    HospitalProviderSpellSpecialCaseCommIds,
    ConditionalCommIds,
    Equals,
    ModelExpression,
    Includes,
    MatchCleanICD10Code,
    OverlapDays,
    Sort,
    SchoolYear,
    AgeGroup,
    AgeBand,
    DaysBetween,
    NotNull,
    DateInBetween,
    DeriveSnomedTerm,
    StringToDateTime,
    Concat,
    MinutesBetween,
    CleanEthnicCategory,
    UnitaryAuthorityFromGPPracticeCode,
    RecordNumberOrUniqueID,
    DerivePostcode,
    CcgFromPostcode,
    CCGFromGPPracticeCode,
    NaiveDatetimeFromTimestampString,
    OrgDistance,
    FileTypeExpressionYTD,
    ProviderFromSiteCode,
    LowerLayerSuperOutputAreaFromPostcode,
    MultipleColumnsNotNull,
    SubICBFromPostcode,
    SubICBFromGPPracticeCode,
    ICBFromSubICB,
    Coalesce,
    as_type,
    DateBeforeOrEqualTo,
)

from dsp.common.model_accessor import ModelAttribute
from dsp.common.structured_model import (
    DSPStructuredModel,
    DerivedAttribute,
    RepeatingSubmittedAttribute,
    SubmittedAttribute,
)
from dsp.shared.constants import DS, FileTypeTypes


@pytest.mark.parametrize(
    "input_date, expected",
    [
        (None, True),
        (datetime(2018, 11, 8), False),
    ],
)
def test_if(referral_mhsds: Dict, input_date: datetime, expected: str):
    referral_mhsds["Patient"]["ClusteringToolAssessments"][0]["CareClusters"][0][
        "EndDateCareClust"
    ] = input_date
    new_referral_mhsds = Referral_MHSDS(referral_mhsds)  # type: Referral_MHSDS

    assert (
        new_referral_mhsds.Patient.ClusteringToolAssessments[0]
        .CareClusters[0]
        .ClusterOpenEndRPFlag
        == expected
    )


@pytest.mark.parametrize(
    "value, otherwise, expected",
    [
        (10, 0, 5),
        (1, 0, 0),
        (1, None, None),
    ],
)
def test_if_then_else(value: int, otherwise: Optional[int], expected: int):
    class _MyIf(DSPStructuredModel):
        Value = SubmittedAttribute("Value", int)  # type: SubmittedAttribute

    class MyIf(_MyIf):
        __concrete__ = True
        Derived = DerivedAttribute(
            "Derived",
            int,
            If(
                condition=Select(_MyIf.Value) > Literal(5),
                then=Literal(5),
                otherwise=Literal(otherwise),
            ),
        )

    myif = MyIf({"Value": value})
    assert myif.Derived == expected


def test_lt_le_eq_ne_ge_gt_not():
    class _MyEquality(DSPStructuredModel):
        Value = SubmittedAttribute("Value", int)  # type: SubmittedAttribute

    class MyEquality(_MyEquality):
        __concrete__ = True
        Lt = DerivedAttribute("Lt", bool, Select(_MyEquality.Value) < Literal(1))
        Le = DerivedAttribute("Le", bool, Select(_MyEquality.Value) <= Literal(1))
        Eq = DerivedAttribute("Eq", bool, Select(_MyEquality.Value) == Literal(1))
        Ne = DerivedAttribute("Ne", bool, Select(_MyEquality.Value) != Literal(1))
        Ge = DerivedAttribute("Ge", bool, Select(_MyEquality.Value) >= Literal(1))
        Gt = DerivedAttribute("Gt", bool, Select(_MyEquality.Value) > Literal(1))
        Not = DerivedAttribute("Not", bool, ~Select(_MyEquality.Value))

    my_equality = MyEquality({"Value": 1})

    print(my_equality.as_dict())

    assert not my_equality.Lt
    assert my_equality.Le
    assert my_equality.Eq
    assert not my_equality.Ne
    assert my_equality.Ge
    assert not my_equality.Gt
    assert not my_equality.Not


def test_greater_than():
    class _Info(DSPStructuredModel):
        StartDate = SubmittedAttribute(
            "StartDate", datetime
        )  # type: SubmittedAttribute
        EndDate = SubmittedAttribute("EndDate", datetime)  # type: SubmittedAttribute
        FirstNum = SubmittedAttribute("FirstNum", int)  # type: SubmittedAttribute
        SecondNum = SubmittedAttribute("SecondNum", int)  # type: SubmittedAttribute

    class Info(_Info):
        __concrete__ = True
        Date_Greater_Than = DerivedAttribute(
            "Date_Greater_Than", bool, Select(_Info.EndDate) > Select(_Info.StartDate)
        )

        Int_Greater_Than = DerivedAttribute(
            "Int_Greater_Than", bool, Select(_Info.SecondNum) > Select(_Info.FirstNum)
        )

        NewDate = DerivedAttribute(
            "NewDate",
            datetime,
            AddToDate(
                days_to_add_expr=Literal(2),
                months_to_add_expr=Literal(1),
                years_to_add_expr=Literal(1),
                input_date_expr=Select(_Info.StartDate),
            ),
        )

    info1 = Info(
        {
            "StartDate": datetime(2028, 10, 10),
            "EndDate": datetime(2028, 12, 10),
            "FirstNum": 10,
            "SecondNum": 20,
        }
    )

    assert info1.Date_Greater_Than
    assert info1.Int_Greater_Than
    assert info1.NewDate == datetime(2029, 11, 12)


CCGFields = namedtuple(
    "CCGFields",
    "end_date_gmp_registration, org_id_gp_prac, gmp_code_reg, org_id_ccg_practice",
)


@pytest.mark.parametrize(
    "uniq_submission_id, row_num, return_decimal, expected",
    [
        ("1234", 11, None, 1234000000011),
        ("1234567899", 111111, None, 1234567899000111111),
        ("1234567899", 111111, True, Decimal(1234567899000111111)),
    ],
)
def test_record_num_or_uniq_id(uniq_submission_id, row_num, return_decimal, expected):
    if return_decimal:
        uniq_table_id = RecordNumberOrUniqueID(
            submission_id_expr=Literal(uniq_submission_id),
            row_number_expr=Literal(row_num),
            return_decimal=return_decimal,
        ).resolve_value(None)

    else:
        uniq_table_id = RecordNumberOrUniqueID(
            submission_id_expr=Literal(uniq_submission_id),
            row_number_expr=Literal(row_num),
        ).resolve_value(None)

    assert type(uniq_table_id) == type(expected)
    assert uniq_table_id == expected


@pytest.mark.parametrize(
    "birthdate, activitydate, activityage, expected_schoolyr",
    [
        (None, datetime(2016, 9, 2), 1, None),
        (datetime(2015, 9, 1), None, 2, None),
        (None, None, 3, None),
        (datetime(2015, 9, 1), datetime(2016, 9, 2), 1, "E1"),
        (datetime(2000, 9, 1), datetime(2000, 9, 28), 0, None),
        (datetime(1999, 10, 2), datetime(2000, 10, 27), 1, "E1"),
        (datetime(1998, 11, 3), datetime(2000, 11, 26), 2, "E2"),
        (datetime(1997, 12, 4), datetime(2000, 12, 25), 3, "N1"),
        (datetime(1996, 1, 5), datetime(2000, 1, 24), 4, "N2"),
        (datetime(1995, 2, 6), datetime(2000, 2, 23), 5, "R"),
        (datetime(1994, 3, 7), datetime(2000, 3, 22), 6, "Y1"),
        (datetime(1993, 4, 8), datetime(2000, 4, 21), 7, "Y2"),
        (datetime(1992, 5, 9), datetime(2000, 5, 20), 8, "Y3"),
        (datetime(1991, 6, 10), datetime(2000, 6, 19), 9, "Y4"),
        (datetime(1990, 7, 11), datetime(2000, 7, 18), 10, "Y5"),
        (datetime(1989, 8, 12), datetime(2000, 8, 17), 11, "Y6"),
        (datetime(1988, 7, 13), datetime(2000, 7, 16), 12, "Y7"),
        (datetime(1987, 6, 14), datetime(2000, 6, 15), 13, "Y8"),
        (datetime(1985, 5, 15), datetime(2000, 5, 14), 14, "Y9"),
        (datetime(1984, 4, 16), datetime(2000, 4, 13), 15, "Y10"),
        (datetime(1983, 3, 17), datetime(2000, 3, 12), 16, "Y11"),
        (datetime(1982, 2, 18), datetime(2000, 2, 11), 17, "Y12"),
        (datetime(1981, 1, 19), datetime(2000, 1, 10), 18, "Y13"),
        (datetime(1980, 12, 20), datetime(2000, 12, 9), 19, None),
        (datetime(1992, 1, 21), datetime(2011, 12, 8), 19, None),
        (datetime(1992, 12, 22), datetime(2011, 11, 7), 18, None),
        (datetime(1994, 1, 23), datetime(2011, 12, 6), 17, "Y13"),
        (datetime(1994, 2, 24), datetime(2011, 1, 5), 16, "Y12"),
        (datetime(1995, 3, 25), datetime(2011, 2, 4), 15, "Y11"),
        (datetime(1996, 4, 26), datetime(2011, 3, 3), 14, "Y10"),
        (datetime(1997, 5, 27), datetime(2011, 4, 2), 13, "Y9"),
        (datetime(1998, 6, 28), datetime(2011, 5, 1), 12, "Y8"),
        (datetime(1999, 7, 27), datetime(2011, 6, 2), 11, "Y7"),
        (datetime(2000, 8, 26), datetime(2011, 7, 3), 10, "Y6"),
        (datetime(2001, 7, 25), datetime(2011, 6, 4), 9, "Y5"),
        (datetime(2002, 6, 24), datetime(2011, 5, 5), 8, "Y4"),
        (datetime(2003, 5, 23), datetime(2011, 4, 6), 7, "Y3"),
        (datetime(2004, 4, 22), datetime(2011, 3, 7), 6, "Y2"),
        (datetime(2005, 3, 21), datetime(2011, 2, 8), 5, "Y1"),
        (datetime(2006, 2, 20), datetime(2011, 1, 9), 4, "R"),
        (datetime(2008, 1, 19), datetime(2011, 12, 10), 3, "N2"),
        (datetime(2008, 12, 18), datetime(2011, 11, 11), 2, "N1"),
        (datetime(2009, 11, 17), datetime(2011, 10, 12), 1, "E2"),
        (datetime(2010, 10, 16), datetime(2011, 9, 13), 0, "E1"),
        (datetime(2019, 10, 15), datetime(2019, 11, 14), 0, None),
        (datetime(2018, 11, 14), datetime(2019, 12, 15), 1, "E1"),
        (datetime(2016, 12, 13), datetime(2019, 1, 16), 2, "E2"),
        (datetime(2016, 1, 12), datetime(2019, 2, 17), 3, "N1"),
        (datetime(2015, 2, 11), datetime(2019, 3, 18), 4, "N2"),
        (datetime(2014, 3, 10), datetime(2019, 4, 19), 5, "R"),
        (datetime(2013, 4, 9), datetime(2019, 5, 20), 6, "Y1"),
        (datetime(2012, 5, 8), datetime(2019, 6, 21), 7, "Y2"),
        (datetime(2011, 6, 7), datetime(2019, 7, 22), 8, "Y3"),
        (datetime(2010, 7, 6), datetime(2019, 8, 23), 9, "Y4"),
        (datetime(2009, 8, 5), datetime(2019, 9, 24), 10, "Y6"),
        (datetime(2008, 9, 4), datetime(2019, 10, 25), 11, "Y6"),
        (datetime(2007, 10, 3), datetime(2019, 11, 26), 12, "Y7"),
        (datetime(2006, 11, 2), datetime(2019, 12, 27), 13, "Y8"),
        (datetime(2004, 12, 1), datetime(2019, 1, 28), 14, "Y9"),
        (datetime(2004, 1, 2), datetime(2019, 2, 27), 15, "Y10"),
        (datetime(2003, 2, 3), datetime(2019, 3, 26), 16, "Y11"),
        (datetime(2002, 3, 4), datetime(2019, 4, 25), 17, "Y12"),
        (datetime(2001, 4, 5), datetime(2019, 5, 24), 18, "Y13"),
        (datetime(2000, 5, 6), datetime(2019, 6, 23), 19, None),
        (datetime(1995, 4, 7), datetime(2015, 2, 22), 19, None),
        (datetime(1997, 3, 8), datetime(2015, 5, 21), 18, "Y13"),
        (datetime(1998, 2, 9), datetime(2015, 11, 20), 17, "Y13"),
        (datetime(1999, 1, 10), datetime(2015, 4, 19), 16, "Y11"),
        (datetime(1999, 12, 11), datetime(2015, 8, 18), 15, "Y10"),
        (datetime(2000, 11, 12), datetime(2015, 3, 17), 14, "Y9"),
        (datetime(2001, 10, 13), datetime(2015, 5, 16), 13, "Y8"),
        (datetime(2002, 9, 14), datetime(2015, 2, 15), 12, "Y7"),
        (datetime(2003, 8, 15), datetime(2015, 2, 14), 11, "Y7"),
        (datetime(2004, 7, 16), datetime(2015, 1, 13), 10, "Y6"),
        (datetime(2006, 6, 17), datetime(2015, 11, 12), 9, "Y5"),
        (datetime(2007, 5, 18), datetime(2015, 12, 11), 8, "Y4"),
        (datetime(2008, 4, 19), datetime(2015, 8, 10), 7, "Y2"),
        (datetime(2009, 3, 20), datetime(2015, 11, 9), 6, "Y2"),
        (datetime(2010, 2, 21), datetime(2015, 5, 8), 5, "R"),
        (datetime(2011, 1, 22), datetime(2015, 10, 7), 4, "R"),
        (datetime(2011, 12, 23), datetime(2015, 2, 6), 3, "N1"),
        (datetime(2012, 11, 24), datetime(2015, 9, 5), 2, "N1"),
        (datetime(2014, 10, 25), datetime(2015, 11, 4), 1, "E1"),
        (datetime(2013, 9, 26), datetime(2015, 8, 3), 1, "E1"),
    ],
)
def test_school_year(
    birthdate: datetime,
    activitydate: datetime,
    activityage: int,
    expected_schoolyr: str,
):
    class _Demographic(DSPStructuredModel):
        BirthDate = SubmittedAttribute(
            "BirthDate", datetime
        )  # type: SubmittedAttribute
        ActivityDate = SubmittedAttribute(
            "ActivityDate", datetime
        )  # type: SubmittedAttribute
        ActivityAge = SubmittedAttribute("ActivityAge", int)  # type: SubmittedAttribute

    class Demographic(_Demographic):
        __concrete__ = True
        SchoolYrAtActivity = DerivedAttribute(
            "SchoolYrAtActivity",
            str,
            SchoolYear(
                date_of_birth_expr=Select(_Demographic.BirthDate),
                date_of_activity_expr=Select(_Demographic.ActivityDate),
                age_at_activity_expr=Select(_Demographic.ActivityAge),
            ),
        )

    demographic = Demographic(
        {
            "BirthDate": birthdate,
            "ActivityDate": activitydate,
            "ActivityAge": activityage,
        }
    )
    assert demographic.SchoolYrAtActivity == expected_schoolyr


@pytest.mark.parametrize(
    "age_yrs, expected_group",
    [
        (None, None),
        (0, "0-18"),
        (18, "0-18"),
        (9, "0-18"),
        (19, "19-64"),
        (64, "19-64"),
        (42, "19-64"),
        (65, "65_Plus"),
        (129, "65_Plus"),
        (97, "65_Plus"),
        (130, None),
    ],
)
def test_age_groups(age_yrs: int, expected_group: str):
    class _Demographic(DSPStructuredModel):
        Age_Yrs = SubmittedAttribute("Age_Yrs", int)  # type: SubmittedAttribute

    class Demographic(_Demographic):
        __concrete__ = True
        AgeGrouping = DerivedAttribute(
            "AgeGrouping", str, AgeGroup(age_yrs_expr=Select(_Demographic.Age_Yrs))
        )

    demographic = Demographic({"Age_Yrs": age_yrs})
    assert demographic.AgeGrouping == expected_group


@pytest.mark.parametrize(
    "score, snomed_id, expected",
    [
        # blank fields tests
        (None, None, None),
        (None, "953211000000100", None),
        ("17", None, None),
        ("BUTT", "953311000000105", None),
        ("17", "BUTT", None),
        # score below threshold tests
        (
            "25",
            "953211000000100",
            "C1",
        ),  # Snomed_id = 953211000000100 and score < 25.17
        ("0", "953211000000100", "C1"),  # snomed_id = 953211000000100 and score 0
        (
            "20",
            "953261000000103",
            "C1",
        ),  # snomed_id = 953261000000103 and score < 24.02
        ("0", "953261000000103", "C1"),  # snomed_id = 953261000000103 and score 0
        ("30", "953311000000105", "C1"),  # snomed_id = 953311000000105 and score < 33.3
        ("0", "953311000000105", "C1"),  # snomed_id = 953311000000105 and score 0
        ("35", "953231000000108", "G1"),  # snomed_id = 953231000000108 and score < 38.7
        ("0", "953231000000108", "G1"),  # snomed_id = 953231000000108 and score 0
        ("25", "953281000000107", "G1"),  # snomed_id = 953281000000107 and score < 28.1
        ("0", "953281000000107", "G1"),  # snomed_id = 953281000000107 and score 0
        (
            "35",
            "953331000000102",
            "G1",
        ),  # snomed_id = 953331000000102 and score < 36.14
        ("0", "953331000000102", "G1"),  # snomed_id = 953331000000102 and score 0
        (
            "35",
            "953221000000106",
            "F1",
        ),  # snomed_id = 953221000000106 and score < 35.16
        ("0", "953221000000106", "F1"),  # snomed_id = 953221000000106 and score 0
        (
            "15",
            "953271000000105",
            "F1",
        ),  # snomed_id = 953271000000105 and score < 18.42
        ("0", "953271000000105", "F1"),  # snomed_id = 953271000000105 and score 0
        (
            "15",
            "953321000000104",
            "F1",
        ),  # snomed_id = 953321000000104 and score < 19.25
        ("0", "953321000000104", "F1"),  # snomed_id = 953321000000104 and score 0
        (
            "30",
            "953251000000101",
            "S1",
        ),  # snomed_id = 953251000000101 and score < 31.54
        ("0", "953251000000101", "S1"),  # snomed_id = 953251000000101 and score 0
        (
            "25",
            "953301000000108",
            "S1",
        ),  # snomed_id = 953301000000108 and score < 25.31
        ("0", "953301000000108", "S1"),  # snomed_id = 953301000000108 and score 0
        (
            "30",
            "953351000000109",
            "S1",
        ),  # snomed_id = 953291000000109 and score < 32.01
        ("0", "953351000000109", "S1"),  # snomed_id = 953291000000109 and score 0
        (
            "25",
            "953241000000104",
            "P1",
        ),  # snomed_id = 953241000000104 and score < 29.78
        ("0", "953241000000104", "P1"),  # snomed_id = 953241000000104 and score 0
        (
            "25",
            "953291000000109",
            "P1",
        ),  # snomed_id = 953291000000109 and score < 27.62
        ("0", "953291000000109", "P1"),  # snomed_id = 953291000000109 and score 0
        (
            "25",
            "953341000000106",
            "P1",
        ),  # snomed_id = 953341000000106 and score < 27.08
        ("0", "953341000000106", "P1"),  # snomed_id = 953341000000106 and score 0
        # # score above threshold tests
        (
            "25.17",
            "953211000000100",
            "C2",
        ),  # snomed_id = 953211000000100 and score > 25.17 but <= 60
        (
            "30",
            "953211000000100",
            "C2",
        ),  # snomed_id = 953211000000100 and score > 25.17 but <= 60
        ("60", "953211000000100", "C2"),  # snomed_id = 953341000000106 and score 60
        (
            "24.02",
            "953261000000103",
            "C2",
        ),  # snomed_id = 953261000000103 and score > 24.02 but <= 60
        (
            "25",
            "953261000000103",
            "C2",
        ),  # snomed_id = 953261000000103 and score > 24.02 but <= 60
        ("60", "953261000000103", "C2"),  # snomed_id = 953261000000103 and score 60
        (
            "33.3",
            "953311000000105",
            "C2",
        ),  # snomed_id = 953311000000105 and score > 33.3 but <= 60
        (
            "35",
            "953311000000105",
            "C2",
        ),  # snomed_id = 953311000000105 and score > 33.3 but <= 60
        ("60", "953311000000105", "C2"),  # snomed_id = 953311000000105 and score 60
        (
            "38.07",
            "953231000000108",
            "G2",
        ),  # snomed_id = 953231000000108 and score > 38.07 but <= 60
        (
            "38.08",
            "953231000000108",
            "G2",
        ),  # snomed_id = 953231000000108 and score > 38.07 but <= 60
        (
            "40",
            "953231000000108",
            "G2",
        ),  # snomed_id = 953231000000108 and score > 38.07 but <= 60
        ("60", "953231000000108", "G2"),  # snomed_id = 953231000000108 and score 60
        (
            "28.01",
            "953281000000107",
            "G2",
        ),  # snomed_id = 953281000000107 and score > 28.01 but <= 60
        (
            "28.02",
            "953281000000107",
            "G2",
        ),  # snomed_id = 953281000000107 and score > 28.01 but <= 60
        (
            "30",
            "953281000000107",
            "G2",
        ),  # snomed_id = 953281000000107 and score > 28.01 but <= 60
        ("60", "953281000000107", "G2"),  # snomed_id = 953281000000107 and score 60
        (
            "36.14",
            "953331000000102",
            "G2",
        ),  # snomed_id = 953281000000107 and score > 36.14 but <= 60
        (
            "40",
            "953331000000102",
            "G2",
        ),  # snomed_id = 953281000000107 and score > 36.14 but <= 60
        ("60", "953331000000102", "G2"),  # snomed_id = 953281000000107 and score 60
        (
            "35.16",
            "953221000000106",
            "F2",
        ),  # snomed_id = 953221000000106 and score > 35.16 but <= 60
        (
            "40",
            "953221000000106",
            "F2",
        ),  # snomed_id = 953221000000106 and score > 35.16 but <= 60
        ("60", "953221000000106", "F2"),  # snomed_id = 953221000000106 and score 60
        (
            "18.42",
            "953271000000105",
            "F2",
        ),  # snomed_id = 953271000000105 and score > 18.42 but <= 60
        (
            "20",
            "953271000000105",
            "F2",
        ),  # snomed_id = 953271000000105 and score > 18.42 but <= 60
        ("60", "953271000000105", "F2"),  # snomed_id = 953271000000105 and score 60
        (
            "19.25",
            "953321000000104",
            "F2",
        ),  # snomed_id = 953321000000104 and score > 19.25 but <= 60
        (
            "19.26",
            "953321000000104",
            "F2",
        ),  # snomed_id = 953321000000104 and score > 19.25 but <= 60
        (
            "50",
            "953321000000104",
            "F2",
        ),  # snomed_id = 953321000000104 and score > 19.25 but <= 60
        (
            "60",
            "953321000000104",
            "F2",
        ),  # snomed_id = 953321000000104 and score > 19.25 but <= 60
        (
            "31.54",
            "953251000000101",
            "S2",
        ),  # snomed_id = 953251000000101 and score > 31.54 but <= 60
        (
            "35",
            "953251000000101",
            "S2",
        ),  # snomed_id = 953251000000101 and score > 31.54 but <= 60
        ("60", "953251000000101", "S2"),  # snomed_id = 953251000000101 and score 60
        (
            "25.31",
            "953301000000108",
            "S2",
        ),  # snomed_id = 953301000000108 and score > 25.31 but <= 60
        (
            "30",
            "953301000000108",
            "S2",
        ),  # snomed_id = 953301000000108 and score > 25.31 but <= 60
        ("60", "953301000000108", "S2"),  # snomed_id = 953301000000108 and score 60
        (
            "32.01",
            "953351000000109",
            "S2",
        ),  # snomed_id = 953351000000109 and score > 32.01 but <= 60
        (
            "35",
            "953351000000109",
            "S2",
        ),  # snomed_id = 953351000000109 and score > 32.01 but <= 60
        ("60", "953351000000109", "S2"),  # snomed_id = 953351000000109 and score 60
        (
            "29.78",
            "953241000000104",
            "P2",
        ),  # snomed_id = 953241000000104 and score > 29.78 but <= 60
        (
            "30",
            "953241000000104",
            "P2",
        ),  # snomed_id = 953241000000104 and score > 29.78 but <= 60
        ("60", "953241000000104", "P2"),  # snomed_id = 953241000000104 and score 60
        (
            "27.62",
            "953291000000109",
            "P2",
        ),  # snomed_id = 953291000000109 and score > 27.62 but <= 60
        (
            "30",
            "953291000000109",
            "P2",
        ),  # snomed_id = 953291000000109 and score > 27.62 but <= 60
        ("60", "953291000000109", "P2"),  # snomed_id = 953291000000109 and score 60
        (
            "27.08",
            "953341000000106",
            "P2",
        ),  # snomed_id = 953341000000106 and score > 27.08 but <= 60
        (
            "30",
            "953341000000106",
            "P2",
        ),  # snomed_id = 953341000000106 and score > 27.08 but <= 60
        ("60", "953341000000106", "P2"),  # snomed_id = 953341000000106 and score 60
        #
        # # score invalid tests
        ("-1", "953211000000100", "C3"),  # snomed_id = 953211000000100 and score < 0
        (
            "60.01",
            "953211000000100",
            "C3",
        ),  # snomed_id = 953211000000100 and score > 60
        ("61", "953211000000100", "C3"),  # snomed_id = 953211000000100 and score > 60
        ("-1", "953261000000103", "C3"),  # snomed_id = 953261000000103 and score < 0
        (
            "60.01",
            "953261000000103",
            "C3",
        ),  # snomed_id = 953261000000103 and score > 60
        ("61", "953261000000103", "C3"),  # snomed_id = 953261000000103 and score > 60
        ("-1", "953311000000105", "C3"),  # snomed_id = 953311000000105 and score < 0
        (
            "60.01",
            "953311000000105",
            "C3",
        ),  # snomed_id = 953311000000105 and score > 60
        ("61", "953311000000105", "C3"),  # snomed_id = 953311000000105 and score > 60
        ("-1", "953231000000108", "G3"),  # snomed_id = 953231000000108 and score < 0
        (
            "60.01",
            "953231000000108",
            "G3",
        ),  # snomed_id = 953231000000108 and score > 60
        ("61", "953231000000108", "G3"),  # snomed_id = 953231000000108 and score > 60
        ("-1", "953281000000107", "G3"),  # snomed_id = 953281000000107 and score < 0
        (
            "60.01",
            "953281000000107",
            "G3",
        ),  # snomed_id = 953281000000107 and score > 60
        ("61", "953281000000107", "G3"),  # snomed_id = 953281000000107 and score > 60
        ("-1", "953331000000102", "G3"),  # snomed_id = 953331000000102 and score < 0
        (
            "60.01",
            "953331000000102",
            "G3",
        ),  # snomed_id = 953331000000102 and score > 60
        ("61", "953331000000102", "G3"),  # snomed_id = 953331000000102 and score > 60
        ("-1", "953221000000106", "F3"),  # snomed_id = 953221000000106 and score < 0
        (
            "60.01",
            "953221000000106",
            "F3",
        ),  # snomed_id = 953221000000106 and score > 60
        ("61", "953221000000106", "F3"),  # snomed_id = 953221000000106 and score > 60
        ("-1", "953271000000105", "F3"),  # snomed_id = 953271000000105 and score < 0
        (
            "60.01",
            "953271000000105",
            "F3",
        ),  # snomed_id = 953271000000105 and score > 60
        ("61", "953271000000105", "F3"),  # snomed_id = 953271000000105 and score > 60
        ("-1", "953321000000104", "F3"),  # snomed_id = 953321000000104 and score < 0
        (
            "60.01",
            "953321000000104",
            "F3",
        ),  # snomed_id = 953321000000104 and score > 60
        ("61", "953321000000104", "F3"),  # snomed_id = 953321000000104 and score > 60
        ("-1", "953251000000101", "S3"),  # snomed_id = 953251000000101 and score < 0
        (
            "60.01",
            "953251000000101",
            "S3",
        ),  # snomed_id = 953251000000101 and score > 60
        ("61", "953251000000101", "S3"),  # snomed_id = 953251000000101 and score > 60
        ("-1", "953301000000108", "S3"),  # snomed_id = 953301000000108 and score < 0
        (
            "60.01",
            "953301000000108",
            "S3",
        ),  # snomed_id = 953301000000108 and score > 60
        ("61", "953301000000108", "S3"),  # snomed_id = 953301000000108 and score > 60
        ("-1", "953351000000109", "S3"),  # snomed_id = 953351000000109 and score < 0
        (
            "60.01",
            "953351000000109",
            "S3",
        ),  # snomed_id = 953351000000109 and score > 60
        ("61", "953351000000109", "S3"),  # snomed_id = 953351000000109 and score > 60
        ("-1", "953241000000104", "P3"),  # snomed_id = 953241000000104 and score < 0
        (
            "60.01",
            "953241000000104",
            "P3",
        ),  # snomed_id = 953241000000104 and score > 60
        ("61", "953241000000104", "P3"),  # snomed_id = 953241000000104 and score > 60
        ("-1", "953291000000109", "P3"),  # snomed_id = 953291000000109 and score < 0
        (
            "60.01",
            "953291000000109",
            "P3",
        ),  # snomed_id = 953291000000109 and score > 60
        ("61", "953291000000109", "P3"),  # snomed_id = 953291000000109 and score > 60
        ("-1", "953341000000106", "P3"),  # snomed_id = 953341000000106 and score < 0
        (
            "60.01",
            "953341000000106",
            "P3",
        ),  # snomed_id = 953341000000106 and score > 60
        ("61", "953341000000106", "P3"),  # snomed_id = 953341000000106 and score > 60
    ],
)
def test_asq_scoreband_expression(score: int, snomed_id: str, expected: str):
    class _CodedScoredAssessmentContact(DSPStructuredModel):
        SNOMED_ID = SubmittedAttribute("SNOMED_ID", str)
        Score = SubmittedAttribute("Score", str)

    class CodedScoredAssessmentContact(_CodedScoredAssessmentContact):
        __concrete__ = True
        ASQ_ScoreBand = DerivedAttribute(
            "ASQ_ScoreBand",
            str,
            ASQScoreBand(
                snomed_id=Select(_CodedScoredAssessmentContact.SNOMED_ID),
                score=Select(_CodedScoredAssessmentContact.Score),
            ),
        )

    coded_scored_assessment_contact = CodedScoredAssessmentContact(
        {"Score": score, "SNOMED_ID": snomed_id}
    )
    assert coded_scored_assessment_contact.ASQ_ScoreBand == expected


@pytest.mark.parametrize(
    "coded_finding, breast_feeding_status, expected",
    [
        (None, None, None),
        (None, "BISH", None),
        ("BISH", None, None),
        (None, "01", "B1"),
        (None, "02", "B2"),
        (None, "03", "B3"),
        ("1697410041", "01", "B1"),
        ("XaPO8", "01", "B2"),
        ("62P7.", "01", "B3"),
        ("169741004", "NO", "B1"),
        ("169969002", "01", "B2"),
        ("169741004", "02", "B1"),
    ],
)
def test_breast_feeding_status_master_expression(
    coded_finding: str, breast_feeding_status: str, expected: str
):
    class _BreastfeedingStatus(DSPStructuredModel):
        BreastFeedingStatus = SubmittedAttribute("BreastFeedingStatus", str)

    class BreastfeedingStatus(_BreastfeedingStatus):
        __concrete__ = True

    class _CareActivity(DSPStructuredModel):
        CodedFinding = SubmittedAttribute("CodedFinding", str)
        BreastfeedingStatuses = RepeatingSubmittedAttribute(
            "BreastfeedingStatuses", _BreastfeedingStatus
        )  # type: List[_BreastfeedingStatus]

    class CareActivity(_CareActivity):
        __concrete__ = True

        BreastFeedingStatus_Master = DerivedAttribute(
            "BreastFeedingStatus_Master",
            str,
            BreastFeedingStatusMaster(
                coded_finding=Select(_CareActivity.CodedFinding),
                breast_feeding_status=Select(_CareActivity.BreastfeedingStatuses),
            ),
        )

    bfstatus = [BreastfeedingStatus({"BreastFeedingStatus": breast_feeding_status})]
    care_act = CareActivity(
        {"CodedFinding": coded_finding, "BreastfeedingStatuses": bfstatus}
    )

    assert care_act.BreastFeedingStatus_Master == expected


@pytest.mark.parametrize(
    "coded_finding, expected",
    [
        (None, None),
        ("BISH", None),
        ("XaPO8", "B2"),
        ("62P7.", "B3"),
        ("169741004", "B1"),
        ("169969002", "B2"),
        ("62P7.", "B3"),
    ],
)
def test_breast_feeding_status_master_expression_empty_bfstatus_table(
    coded_finding: str, expected: str
):
    class _BreastfeedingStatus(DSPStructuredModel):
        BreastFeedingStatus = SubmittedAttribute("BreastFeedingStatus", str)

    class BreastfeedingStatus(_BreastfeedingStatus):
        __concrete__ = True

    class _CareActivity(DSPStructuredModel):
        CodedFinding = SubmittedAttribute("CodedFinding", str)
        BreastfeedingStatuses = RepeatingSubmittedAttribute(
            "BreastfeedingStatuses", _BreastfeedingStatus
        )  # type: List[_BreastfeedingStatus]

    class CareActivity(_CareActivity):
        __concrete__ = True

        BreastFeedingStatus_Master = DerivedAttribute(
            "BreastFeedingStatus_Master",
            str,
            BreastFeedingStatusMaster(
                coded_finding=Select(_CareActivity.CodedFinding),
                breast_feeding_status=Select(_CareActivity.BreastfeedingStatuses),
            ),
        )

    bfstatus = []
    care_act = CareActivity(
        {"CodedFinding": coded_finding, "BreastfeedingStatuses": bfstatus}
    )

    assert care_act.BreastFeedingStatus_Master == expected


@pytest.mark.parametrize(
    "age_yr, expected_band",
    [
        (None, None),
        (0, "0-4"),
        (4, "0-4"),
        (2, "0-4"),
        (5, "5-9"),
        (9, "5-9"),
        (7, "5-9"),
        (10, "10-14"),
        (14, "10-14"),
        (12, "10-14"),
        (15, "15-19"),
        (19, "15-19"),
        (17, "15-19"),
        (20, "20-24"),
        (24, "20-24"),
        (22, "20-24"),
        (25, "25-29"),
        (29, "25-29"),
        (27, "25-29"),
        (30, "30-34"),
        (34, "30-34"),
        (32, "30-34"),
        (35, "35-39"),
        (39, "35-39"),
        (37, "35-39"),
        (40, "40-44"),
        (44, "40-44"),
        (42, "40-44"),
        (45, "45-49"),
        (49, "45-49"),
        (47, "45-49"),
        (50, "50-54"),
        (54, "50-54"),
        (52, "50-54"),
        (55, "55-59"),
        (59, "55-59"),
        (57, "55-59"),
        (60, "60-64"),
        (64, "60-64"),
        (62, "60-64"),
        (65, "65-69"),
        (69, "65-69"),
        (67, "65-69"),
        (70, "70-74"),
        (74, "70-74"),
        (72, "70-74"),
        (75, "75-79"),
        (79, "75-79"),
        (77, "75-79"),
        (80, "80-84"),
        (84, "80-84"),
        (82, "80-84"),
        (85, "85-89"),
        (89, "85-89"),
        (87, "85-89"),
        (90, "90-94"),
        (94, "90-94"),
        (92, "90-94"),
        (95, "95-99"),
        (99, "95-99"),
        (97, "95-99"),
        (100, "100-104"),
        (104, "100-104"),
        (102, "100-104"),
        (105, "105-109"),
        (109, "105-109"),
        (107, "105-109"),
        (110, "110-114"),
        (114, "110-114"),
        (112, "110-114"),
        (115, "115-119"),
        (119, "115-119"),
        (117, "115-119"),
        (120, "120-124"),
        (124, "120-124"),
        (122, "120-124"),
        (125, "125-129"),
        (129, "125-129"),
        (127, "125-129"),
        (130, None),
    ],
)
def test_age_bands(age_yr: int, expected_band: str):
    class _Demographic(DSPStructuredModel):
        Age_Yr = SubmittedAttribute("Age_Yr", int)  # type: SubmittedAttribute

    class Demographic(_Demographic):
        __concrete__ = True
        AgeBanding = DerivedAttribute(
            "AgeBanding", str, AgeBand(age_yr_expr=Select(_Demographic.Age_Yr))
        )

    demographic = Demographic(
        {
            "Age_Yr": age_yr,
        }
    )
    assert demographic.AgeBanding == expected_band


@pytest.mark.parametrize(
    "date_of_birth_expr, date_for_age_expr, ignore_time, time_unit, expected",
    [
        (None, datetime(2021, 3, 1), True, 1, None),
        (datetime(2016, 2, 29), None, True, 1, None),
        (datetime(2015, 1, 1), datetime(2019, 12, 31), True, 1, 4),
        (datetime(2015, 1, 1), datetime(2020, 1, 1), True, 1, 5),
        (datetime(2015, 1, 1, 1, 2, 0), datetime(2019, 12, 31, 2, 3, 0), False, 1, 4),
        (datetime(2015, 1, 1, 1, 2, 0), datetime(2019, 12, 31, 2, 3, 0), True, 1, 4),
        # reverse date handling test
        (datetime(2019, 12, 31, 2, 3, 0), datetime(2015, 1, 1, 1, 2, 0), True, 1, -4),
        # time_unit = days test
        (datetime(2015, 1, 1, 1, 2, 0), datetime(2019, 12, 31, 2, 3, 0), True, 2, 1825),
        # leap year test
        (datetime(2016, 2, 29), datetime(2021, 2, 28), True, 1, 4),
    ],
)
def test_age_at_date(
    date_of_birth_expr, date_for_age_expr, ignore_time, time_unit, expected
):
    actual = AgeAtDate(
        Literal(date_of_birth_expr), Literal(date_for_age_expr), ignore_time, time_unit
    ).resolve_value(None)

    assert actual == expected


@pytest.mark.parametrize(
    "timestamp_str, expected_datetime",
    [
        ("2019-12-31T00:00:01Z", datetime(2019, 12, 31, 0, 0, 1)),
        ("2019-12-31T00:00:01-00:00", datetime(2019, 12, 31, 0, 0, 1)),
        ("2019-12-31T00:00:01+00:00", datetime(2019, 12, 31, 0, 0, 1)),
        ("2019-12-31T00:00:01+01:00", datetime(2019, 12, 31, 0, 0, 1)),
        ("2019-12-31T00:00:01+12:00", datetime(2019, 12, 31, 0, 0, 1)),
        (None, None),
    ],
)
def test_naive_datetime_from_timestamp_string(
    timestamp_str: str, expected_datetime: datetime
):
    class _Example(DSPStructuredModel):
        AssToolCompTimestamp = SubmittedAttribute(
            "AssToolCompTimestamp", str
        )  # type: SubmittedAttribute

    class Example(_Example):
        __concrete__ = True
        AssToolCompDatetime = DerivedAttribute(
            "AssToolCompDatetime",
            int,
            NaiveDatetimeFromTimestampString(
                timestamp_str_exp=Select(_Example.AssToolCompTimestamp)
            ),
        )

    example = Example(
        {
            "AssToolCompTimestamp": timestamp_str,
        }
    )
    assert example.AssToolCompDatetime == expected_datetime


@pytest.mark.parametrize(
    "birthdate, refdatestr, expected_age",
    [
        (datetime(2015, 1, 1), "2019-01-01T00:00:01+01:00", 4),  # Offset to be ignored.
        (
            datetime(2016, 2, 29),
            "2021-02-28T13:14:15+00:00",
            4,
        ),  # UK leap day birthday rule
    ],
)
def test_age_at_date_via_timestamp_string(
    birthdate: datetime, refdatestr: str, expected_age: int
):
    class _Demographic(DSPStructuredModel):
        BirthDate = SubmittedAttribute(
            "BirthDate", datetime
        )  # type: SubmittedAttribute
        RefTimestampString = SubmittedAttribute(
            "RefTimestampString", str
        )  # type: SubmittedAttribute

    class Demographic(_Demographic):
        __concrete__ = True
        AgeAtRefDate = DerivedAttribute(
            "AgeAtRefDate",
            int,
            AgeAtDate(
                date_of_birth_expr=Select(_Demographic.BirthDate),
                date_for_age_expr=NaiveDatetimeFromTimestampString(
                    timestamp_str_exp=Select(_Demographic.RefTimestampString)
                ),
            ),
        )

    demog = Demographic(
        {
            "BirthDate": birthdate,
            "RefTimestampString": refdatestr,
        }
    )
    assert demog.AgeAtRefDate == expected_age


@pytest.mark.parametrize(
    "birthdate, refdate, expected_age",
    [
        (datetime(2015, 1, 1), datetime(2019, 12, 31), 4),
        (datetime(2015, 1, 1), datetime(2020, 1, 1), 5),
        (datetime(2015, 2, 28), datetime(2020, 2, 27), 4),
        (datetime(2015, 2, 28), datetime(2020, 2, 28), 5),
        (datetime(2015, 2, 28), datetime(2020, 2, 29), 5),
        (datetime(2016, 2, 28), datetime(2020, 2, 27), 3),
        (datetime(2016, 2, 28), datetime(2020, 2, 28), 4),
        (datetime(2016, 2, 28), datetime(2020, 2, 29), 4),
        (datetime(2016, 2, 29), datetime(2020, 2, 28), 3),
        (datetime(2016, 2, 29), datetime(2020, 2, 29), 4),
        (datetime(2016, 2, 29), datetime(2020, 3, 1), 4),
        (datetime(2016, 2, 29), datetime(2021, 2, 28), 4),  # UK leap day birthday rule
        (datetime(2016, 2, 29), datetime(2021, 3, 1), 5),
    ],
)
def test_age_at_date_in_years(
    birthdate: datetime, refdate: datetime, expected_age: int
):
    class _Demographic(DSPStructuredModel):
        BirthDate = SubmittedAttribute(
            "BirthDate", datetime
        )  # type: SubmittedAttribute
        RefDate = SubmittedAttribute("RefDate", datetime)  # type: SubmittedAttribute

    class Demographic(_Demographic):
        __concrete__ = True
        AgeAtRefDate = DerivedAttribute(
            "AgeAtRefDate",
            int,
            AgeAtDate(
                date_of_birth_expr=Select(_Demographic.BirthDate),
                date_for_age_expr=Select(_Demographic.RefDate),
            ),
        )

    demog = Demographic(
        {
            "BirthDate": birthdate,
            "RefDate": refdate,
        }
    )
    assert demog.AgeAtRefDate == expected_age


@pytest.mark.parametrize(
    "birthdate, refdate, expected_age",
    [
        (datetime(2015, 1, 1), datetime(2015, 1, 1), 0),
        (datetime(2015, 1, 1), datetime(2015, 12, 31), 364),
        (datetime(2015, 1, 1), datetime(2016, 1, 1), 365),
        (datetime(2016, 1, 1), datetime(2017, 1, 1), 366),
        (
            datetime(2016, 3, 10),
            datetime(2019, 7, 21),
            3 * 365 + (2 * 30 + 2 * 31) + 11,
        ),
    ],
)
def test_age_at_date_in_days(birthdate: datetime, refdate: datetime, expected_age: int):
    class _Demographic(DSPStructuredModel):
        BirthDate = SubmittedAttribute(
            "BirthDate", datetime
        )  # type: SubmittedAttribute
        RefDate = SubmittedAttribute("RefDate", datetime)  # type: SubmittedAttribute

    class Demographic(_Demographic):
        __concrete__ = True
        AgeAtRefDate = DerivedAttribute(
            "AgeAtRefDate",
            int,
            AgeAtDate(
                date_of_birth_expr=Select(_Demographic.BirthDate),
                date_for_age_expr=Select(_Demographic.RefDate),
                time_unit=AgeAtDate.DAYS,
            ),
        )

    demog = Demographic(
        {
            "BirthDate": birthdate,
            "RefDate": refdate,
        }
    )
    assert demog.AgeAtRefDate == expected_age


def timestamp(hour: int, minute: int, second: int) -> datetime:
    return datetime(1970, 1, 1, hour, minute, second)


@pytest.mark.parametrize(
    "birthdate, birthtime, refdate, reftime, expected_age",
    [
        (
            datetime(2000, 1, 1),
            timestamp(20, 30, 40),
            datetime(2000, 1, 1),
            timestamp(20, 35, 39),
            4,
        ),
        (
            datetime(2000, 1, 1),
            timestamp(20, 30, 40),
            datetime(2000, 1, 1),
            timestamp(20, 35, 40),
            5,
        ),
        (
            datetime(2000, 1, 1),
            timestamp(20, 30, 40),
            datetime(2000, 1, 1),
            timestamp(21, 30, 39),
            59,
        ),
        (
            datetime(2000, 1, 1),
            timestamp(20, 30, 40),
            datetime(2000, 1, 1),
            timestamp(21, 30, 40),
            60,
        ),
        (
            datetime(2000, 1, 1),
            timestamp(20, 30, 40),
            datetime(2000, 1, 1),
            timestamp(21, 35, 39),
            64,
        ),
        (
            datetime(2000, 1, 1),
            timestamp(20, 30, 40),
            datetime(2000, 1, 1),
            timestamp(21, 35, 40),
            65,
        ),
        (
            datetime(2000, 1, 1),
            timestamp(20, 30, 40),
            datetime(2000, 1, 1),
            timestamp(21, 35, 41),
            65,
        ),
        (
            datetime(2000, 1, 1),
            timestamp(10, 30, 40),
            datetime(2000, 1, 11),
            timestamp(14, 30, 40),
            (10 * 24 + 4) * 60,
        ),
    ],
)
def test_age_at_time(
    birthdate: datetime,
    birthtime: datetime,
    refdate: datetime,
    reftime: datetime,
    expected_age,
):
    class _Demographic(DSPStructuredModel):
        BirthDate = SubmittedAttribute(
            "BirthDate", datetime
        )  # type: SubmittedAttribute
        BirthTime = SubmittedAttribute(
            "BirthTime", datetime
        )  # type: SubmittedAttribute
        RefDate = SubmittedAttribute("RefDate", datetime)  # type: SubmittedAttribute
        RefTime = SubmittedAttribute("RefTime", datetime)  # type: SubmittedAttribute

    class Demographic(_Demographic):
        __concrete__ = True
        AgeAtRefInMinutes = DerivedAttribute(
            "AgeAtRefInMinutes",
            int,
            AgeAtTime(
                date_of_birth_expr=Select(_Demographic.BirthDate),
                date_for_age_expr=Select(_Demographic.RefDate),
                time_of_birth_expr=Select(_Demographic.BirthTime),
                time_for_age_expr=Select(_Demographic.RefTime),
                time_unit=AgeAtTime.MINUTES,
            ),
        )
        AgeAtRefInHours = DerivedAttribute(
            "AgeAtRefInHours",
            int,
            AgeAtTime(
                date_of_birth_expr=Select(_Demographic.BirthDate),
                date_for_age_expr=Select(_Demographic.RefDate),
                time_of_birth_expr=Select(_Demographic.BirthTime),
                time_for_age_expr=Select(_Demographic.RefTime),
                time_unit=AgeAtTime.HOURS,
            ),
        )

    demog = Demographic(
        {
            "BirthDate": birthdate,
            "BirthTime": birthtime,
            "RefDate": refdate,
            "RefTime": reftime,
        }
    )
    assert demog.AgeAtRefInMinutes == expected_age
    assert demog.AgeAtRefInHours == expected_age // 60


@pytest.mark.parametrize(
    "value, expected",
    [
        (1, "A"),
        (2, "B"),
        (3, "C"),
    ],
)
def test_case(value: int, expected: str):
    class _MyModel(DSPStructuredModel):
        Foo = SubmittedAttribute("Foo", int)

    class MyModel(_MyModel):
        __concrete__ = True
        Bar = DerivedAttribute(
            "Bar",
            str,
            Case(
                Select(_MyModel.Foo),
                [
                    (lambda x: x == 1, Literal("A")),
                    (lambda x: x < 3, Literal("B")),
                ],
                default=Literal("C"),
            ),
        )

    m = MyModel({"Foo": value})
    assert m.Bar == expected


@pytest.mark.parametrize("value, expected", [(3, None)])
def test_case_without_default(value: int, expected: str):
    class _MyModel(DSPStructuredModel):
        Foo = SubmittedAttribute("Foo", int)

    class MyModel(_MyModel):
        __concrete__ = True
        Bar = DerivedAttribute(
            "Bar",
            str,
            Case(
                Select(_MyModel.Foo),
                [
                    (lambda x: x == 1, Literal("A")),
                    (lambda x: x < 3, Literal("B")),
                ],
            ),
        )

    m = MyModel({"Foo": value})
    assert m.Bar == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (42, 42),
        (None, None),
    ],
)
def test_getattr(value: Optional[int], expected: Optional[int]):
    class _Foo(DSPStructuredModel):
        Bar = SubmittedAttribute("Bar", int)

    class Foo(_Foo):
        __concrete__ = True
        Baz = DerivedAttribute("Baz", int, Select(_Foo.Bar))

    foo = Foo({"Bar": value})

    assert foo.Bar == expected


def test_first():
    class _Bar(DSPStructuredModel):
        Baz = SubmittedAttribute("Baz", int)

    class _Foo(DSPStructuredModel):
        Bars = RepeatingSubmittedAttribute("Bars", _Bar)

    class Bar(_Bar):
        __concrete__ = True

    class Foo(_Foo):
        __concrete__ = True
        FirstBar = DerivedAttribute("FirstBar", int, First(Select(_Foo.Bars)))

    foo = Foo(
        {
            "Bars": [
                Bar({"Baz": 10}),
                Bar({"Baz": 20}),
                Bar({"Baz": 30}),
            ]
        }
    )

    assert foo.FirstBar.Baz == 10


def test_find():
    class _Foo(DSPStructuredModel):
        Bar = SubmittedAttribute("Bar", int)

    class Foo(_Foo):
        __concrete__ = True

    class _MyModel(DSPStructuredModel):
        Foos = RepeatingSubmittedAttribute("Foos", _Foo)

    class MyModel(_MyModel):
        __concrete__ = True
        BestBar = DerivedAttribute(
            "BestBar", int, First(Find(Select(_MyModel.Foos), lambda x: x.Bar > 100))
        )

    m = MyModel({"Foos": [Foo({"Bar": 100}), Foo({"Bar": 200})]})
    assert m.BestBar.Bar == 200


def test_findfirst_nested():
    class _Bar(DSPStructuredModel):
        Id = SubmittedAttribute("Id", int)
        Baz = SubmittedAttribute("Baz", int)

    class _Foo(DSPStructuredModel):
        Id = SubmittedAttribute("Id", int)
        Bars = RepeatingSubmittedAttribute("Bars", _Bar)

    class Bar(_Bar):
        __concrete__ = True

    class Foo(_Foo):
        __concrete__ = True

    class _MyModel(DSPStructuredModel):
        Foos = RepeatingSubmittedAttribute("Foos", _Foo)

    class MyModel(_MyModel):
        __concrete__ = True
        BestBar = DerivedAttribute(
            "BestBar",
            int,
            GetAttr(
                FindFirst(
                    iterable=GetAttr(
                        FindFirst(
                            iterable=Select(_MyModel.Foos),
                            predicate=lambda x: x.Id == 2,
                        ),
                        Literal("Bars"),
                    ),
                    predicate=lambda x: x.Id == 22,
                ),
                Literal("Baz"),
            ),
        )

    m = MyModel(
        {
            "Foos": [
                Foo(
                    {
                        "Id": 1,
                        "Bars": [
                            Bar(
                                {
                                    "Id": 11,
                                    "Baz": 101,
                                }
                            ),
                            Bar(
                                {
                                    "Id": 12,
                                    "Baz": 102,
                                }
                            ),
                        ],
                    }
                ),
                Foo(
                    {
                        "Id": 2,
                        "Bars": [
                            Bar(
                                {
                                    "Id": 21,
                                    "Baz": 201,
                                }
                            ),
                            Bar(
                                {
                                    "Id": 22,
                                    "Baz": 202,
                                }
                            ),
                        ],
                    }
                ),
            ]
        }
    )
    assert m.BestBar == 202


def test_latest_by_date():
    class _Foo(DSPStructuredModel):
        Date = SubmittedAttribute("Date", date)
        Bar = SubmittedAttribute("Bar", int)

    class Foo(_Foo):
        __concrete__ = True

    class _MyModel(DSPStructuredModel):
        Foos = RepeatingSubmittedAttribute("Foos", _Foo)

    class MyModel(_MyModel):
        __concrete__ = True

        LatestByDate = DerivedAttribute(
            "LatestByDate",
            int,
            First(Sort(Select(_MyModel.Foos), lambda x: x.Date, descending=True)),
        )

    m = MyModel(
        {
            "Foos": [
                Foo({"Date": date(2020, 1, 1), "Bar": 1}),
                Foo({"Date": date(2020, 1, 3), "Bar": 3}),
                Foo({"Date": date(2020, 1, 2), "Bar": 2}),
            ]
        }
    )

    assert m.LatestByDate.Date == date(2020, 1, 3)
    assert m.LatestByDate.Bar == 3


def test_findfirst_nested_with_extra_args():  # value: int, expected: str):
    class _Bar(DSPStructuredModel):
        Id = SubmittedAttribute("Id", int)
        Baz = SubmittedAttribute("Baz", int)

    class _Foo(DSPStructuredModel):
        Id = SubmittedAttribute("Id", int)
        Bars = RepeatingSubmittedAttribute("Bars", _Bar)

    class Bar(_Bar):
        __concrete__ = True

    class Foo(_Foo):
        __concrete__ = True

    class _MyModel(DSPStructuredModel):
        Foos = RepeatingSubmittedAttribute("Foos", _Foo)
        PreferredFoo = SubmittedAttribute("PreferredFoo", int)
        PreferredBar = SubmittedAttribute("PreferredBar", int)

    _Bar.Parent = ModelAttribute("parent", _Foo)
    _Foo.Parent = ModelAttribute("parent", _MyModel)

    class MyModel(_MyModel):
        __concrete__ = True
        BestBar = DerivedAttribute(
            "BestBar",
            int,
            GetAttr(
                FindFirst(
                    iterable=GetAttr(
                        FindFirst(
                            iterable=Select(_MyModel.Foos),
                            predicate=lambda x, y: x.Id == y,
                            extra_args=[Select(_MyModel.PreferredFoo)],
                        ),
                        Literal("Bars"),
                    ),
                    predicate=lambda x, y: x.Id == y,
                    extra_args=[Select(_MyModel.PreferredBar)],
                ),
                Literal("Baz"),
            ),
        )

    m = MyModel(
        {
            "PreferredFoo": 2,
            "PreferredBar": 22,
            "Foos": [
                Foo(
                    {
                        "Id": 1,
                        "Bars": [
                            Bar(
                                {
                                    "Id": 11,
                                    "Baz": 101,
                                }
                            ),
                            Bar(
                                {
                                    "Id": 12,
                                    "Baz": 102,
                                }
                            ),
                        ],
                    }
                ),
                Foo(
                    {
                        "Id": 2,
                        "Bars": [
                            Bar(
                                {
                                    "Id": 21,
                                    "Baz": 201,
                                }
                            ),
                            Bar(
                                {
                                    "Id": 22,
                                    "Baz": 202,
                                }
                            ),
                        ],
                    }
                ),
            ],
        }
    )
    assert m.BestBar == 202


class _CommissionerWithDate(DSPStructuredModel):
    OrgIDComm = SubmittedAttribute("OrgIDComm", str)  # type: SubmittedAttribute
    StartDateOrgCodeComm = SubmittedAttribute(
        "StartDateOrgCodeComm", date
    )  # type: SubmittedAttribute
    EndDateOrgCodeComm = SubmittedAttribute(
        "EndDateOrgCodeComm", date
    )  # type: SubmittedAttribute


class _DummyHeader(DSPStructuredModel):
    ReportingPeriodStartDate = SubmittedAttribute(
        "ReportingPeriodStartDate", date
    )  # type: SubmittedAttribute
    ReportingPeriodEndDate = SubmittedAttribute(
        "ReportingPeriodEndDate", date
    )  # type: SubmittedAttribute


class _META(DSPStructuredModel):
    EVENT_RECEIVED_TS = SubmittedAttribute(
        Fields.EVENT_RECEIVED_TS, datetime
    )  # type: SubmittedAttribute


class _DummyRecord(DSPStructuredModel):
    OrgIDComm = SubmittedAttribute("OrgIDComm", str)  # type: SubmittedAttribute
    Commissioners = RepeatingSubmittedAttribute(
        "Commissioners", _CommissionerWithDate
    )  # type: RepeatingSubmittedAttribute
    StartDate = SubmittedAttribute("StartDate", date)  # type: SubmittedAttribute
    EndDate = SubmittedAttribute("EndDate", date)  # type: SubmittedAttribute
    Header = SubmittedAttribute("Header", _DummyHeader)  # type: SubmittedAttribute
    META = SubmittedAttribute("META", _META)  # type: SubmittedAttribute


class META(_META):
    __concrete__ = True


class CommissionerWithDate(_CommissionerWithDate):
    __concrete__ = True


class DummyHeader(_DummyHeader):
    __concrete__ = True


class DummyRecord(_DummyRecord):
    __concrete__ = True


class _Commissioner(DSPStructuredModel):
    OrgIDComm = SubmittedAttribute("OrgIDComm", str)  # type: SubmittedAttribute


class _Patient(DSPStructuredModel):
    Name = SubmittedAttribute("Name", str)  # type: SubmittedAttribute
    Dob = SubmittedAttribute("Dob", str)  # type: SubmittedAttribute
    Commissioners = RepeatingSubmittedAttribute(
        "Commissioners", _Commissioner
    )  # type: RepeatingSubmittedAttribute
    OrgIDComm = SubmittedAttribute("OrgIDComm", str)  # type: SubmittedAttribute


class Commissioner(_Commissioner):
    __concrete__ = True


class Patient(_Patient):
    __concrete__ = True


@pytest.fixture()
def sample_patient():
    comm_1 = Commissioner({"OrgIDComm": "RA1"})
    comm_2 = Commissioner({"OrgIDComm": "RA2"})
    return Patient(
        {
            "Name": "A Patient",
            "Dob": "2009-01-01",
            "Commissioners": [comm_1, comm_2],
            "OrgIDComm": "RB1",
        }
    )


@pytest.fixture()
def patient_with_nones():
    comm_1 = Commissioner({"OrgIDComm": None})
    comm_2 = Commissioner({"OrgIDComm": "RA2"})
    return Patient(
        {
            "Name": "A Patient",
            "Dob": "2009-01-01",
            "Commissioners": [comm_1, comm_2],
            "OrgIDComm": None,
        }
    )


@pytest.fixture()
def patient_all_nones():
    comm_1 = Commissioner({"OrgIDComm": None})
    comm_2 = Commissioner({"OrgIDComm": None})
    return Patient(
        {
            "Name": "A Patient",
            "Dob": "2009-01-01",
            "Commissioners": [comm_1, comm_2],
            "OrgIDComm": None,
        }
    )


# This creates two hospital spell providers for a particular month with a 10 day gap in between
def hospital_spell_commissioners_factory(start_date, end_date):
    comm_1 = CommissionerWithDate(
        {
            "OrgIDComm": "a",
            "StartDateOrgCodeComm": date(2019, 1, 1),
            "EndDateOrgCodeComm": date(2019, 1, 10),
        }
    )
    comm_2 = CommissionerWithDate(
        {
            "OrgIDComm": "b",
            "StartDateOrgCodeComm": date(2019, 1, 20),
            "EndDateOrgCodeComm": date(2019, 1, 30),
        }
    )
    comm_3 = CommissionerWithDate(
        {
            "OrgIDComm": "d",
            "StartDateOrgCodeComm": date(2019, 1, 4),
            "EndDateOrgCodeComm": date(2019, 1, 7),
        }
    )
    comm_4 = CommissionerWithDate(
        {
            "OrgIDComm": "e",
            "StartDateOrgCodeComm": date(2019, 1, 29),
            "EndDateOrgCodeComm": None,
        }
    )
    header = DummyHeader(
        {
            "ReportingPeriodStartDate": date(2019, 1, 1),
            "ReportingPeriodEndDate": date(2019, 1, 30),
        }
    )
    meta = META({"EVENT_RECEIVED_TS": datetime(2019, 3, 1)})
    return DummyRecord(
        {
            "OrgIDComm": "c",
            "Commissioners": [comm_1, comm_2, comm_3, comm_4],
            "StartDate": start_date,
            "EndDate": end_date,
            "Header": header,
            "META": meta,
        }
    )


commissioner_parameters = [
    # simple case inside a HospProvSpellComm
    [date(2019, 1, 2), date(2019, 1, 3), ["a"]],
    # overlapping HospProvSpells
    [date(2019, 1, 3), date(2019, 1, 8), ["a", "d"]],
    # gap period (no active HospProvSpellComm so fall back)
    [date(2019, 1, 11), date(2019, 1, 18), ["c"]],
    # active record and gap period
    [date(2019, 1, 8), date(2019, 1, 13), ["a", "c"]],
    # cross multiple HospProvSpellComms including overlapping and gap
    [date(2019, 1, 5), date(2019, 1, 25), ["a", "b", "c", "d"]],
    # just inside boundaries
    [date(2019, 1, 10), date(2019, 1, 20), ["a", "b", "c"]],
    # just outside boundaries
    [date(2019, 1, 11), date(2019, 1, 19), ["c"]],
    # gap period and active record
    [date(2019, 1, 15), date(2019, 1, 25), ["b", "c"]],
    # single day (gap period)
    [date(2019, 1, 15), date(2019, 1, 15), ["c"]],
    # single day (active HospProvSpellComm)
    [date(2019, 1, 10), date(2019, 1, 10), ["a"]],
    # negative days
    [date(2019, 1, 30), date(2019, 1, 1), []],
    # partially before rp
    [date(2018, 12, 15), date(2019, 1, 2), ["a"]],
    # partially after rp
    [date(2019, 1, 25), date(2019, 2, 10), ["b", "e"]],
    # entirely before rp
    [date(2018, 12, 10), date(2018, 12, 31), []],
    # entirely after rp
    [date(2019, 2, 1), date(2019, 2, 10), []],
    # has overlap and gap and no end
    [date(2019, 1, 1), None, ["a", "b", "c", "d", "e"]],
    # has gap and no end
    [date(2019, 1, 15), None, ["b", "c", "e"]],
    # start after rp
    [date(2019, 2, 1), None, []],
]


@pytest.mark.parametrize(
    "record_start, record_end, expected_comm_ids", commissioner_parameters
)
def test_commissioner_special_case_logic(record_start, record_end, expected_comm_ids):
    referral_mhsds = hospital_spell_commissioners_factory(record_start, record_end)

    selected_comm_ids = HospitalProviderSpellSpecialCaseCommIds(
        start_date_expr=Select(_DummyRecord.StartDate),
        end_date_expr=Select(_DummyRecord.EndDate),
        comm_provider_expr=Select(_DummyRecord.Commissioners),
        fallback_expr=Select(_DummyRecord.OrgIDComm),
        reporting_period_start_expr=Select(
            _DummyRecord.Header.ReportingPeriodStartDate
        ),
        reporting_period_end_expr=Select(_DummyRecord.Header.ReportingPeriodEndDate),
    ).resolve_value(referral_mhsds)

    assert set(selected_comm_ids) == set(expected_comm_ids)


@pytest.mark.parametrize(
    "record_start, record_end, expected_comm_ids", commissioner_parameters
)
def test_commissioner_special_case_logic(record_start, record_end, expected_comm_ids):
    referral_mhsds = hospital_spell_commissioners_factory(record_start, record_end)

    selected_comm_ids = HospitalProviderSpellSpecialCaseCommIds(
        start_date_expr=Select(_DummyRecord.StartDate),
        end_date_expr=Select(_DummyRecord.EndDate),
        comm_provider_expr=Select(_DummyRecord.Commissioners),
        fallback_expr=Select(_DummyRecord.OrgIDComm),
        reporting_period_start_expr=Select(
            _DummyRecord.Header.ReportingPeriodStartDate
        ),
        reporting_period_end_expr=Select(_DummyRecord.Header.ReportingPeriodEndDate),
    ).resolve_value(referral_mhsds)

    assert set(selected_comm_ids) == set(expected_comm_ids)


def test_file_type_expression():
    referral_mhsds = hospital_spell_commissioners_factory(
        date(2019, 1, 1), date(2019, 2, 1)
    )
    file_type = FileTypeExpression(
        Select(_DummyRecord.META.EVENT_RECEIVED_TS),
        Select(_DummyRecord.Header.ReportingPeriodStartDate),
        DS.MHSDS_V5,
    ).resolve_value(referral_mhsds)

    assert file_type == FileTypeTypes.REFRESH


@pytest.mark.parametrize(
    "submitted_date_time, rp_start_date, dataset, expected",
    [
        (datetime(2021, 11, 19, 0, 0, 0), date(2021, 10, 1), "mhsds_v5", 2),
        (datetime(2021, 12, 20, 23, 59, 59), date(2021, 10, 1), "mhsds_v5", 2),
        (datetime(2022, 1, 21, 23, 59, 59), date(2021, 10, 1), "mhsds_v5", 3),
        (datetime(2022, 2, 18, 23, 59, 59), date(2021, 10, 1), "mhsds_v5", 4),
        (datetime(2022, 3, 18, 23, 59, 59), date(2021, 10, 1), "mhsds_v5", 5),
        (datetime(2022, 4, 22, 23, 59, 59), date(2021, 10, 1), "mhsds_v5", 6),
        (datetime(2022, 5, 20, 23, 59, 59), date(2021, 10, 1), "mhsds_v5", 7),
        (datetime(2022, 5, 1, 0, 0, 0), date(2022, 4, 1), "mhsds_v5", 1),
        (datetime(2022, 5, 20, 23, 59, 59), date(2022, 4, 1), "mhsds_v5", 1),
        (datetime(2022, 5, 21, 0, 0, 0), date(2022, 4, 1), "mhsds_v5", 2),
        (datetime(2022, 6, 23, 23, 59, 59), date(2022, 4, 1), "mhsds_v5", 3),
        (datetime(2022, 7, 21, 23, 59, 59), date(2022, 4, 1), "mhsds_v5", 4),
        (datetime(2022, 8, 19, 23, 59, 59), date(2022, 4, 1), "mhsds_v5", 5),
        (datetime(2022, 8, 19, 23, 59, 59), date(2022, 5, 1), "mhsds_v5", 4),
    ],
)
def test_file_type_ytd_expression(
    submitted_date_time, rp_start_date, dataset, expected
):
    actual = FileTypeExpressionYTD(
        Literal(submitted_date_time), Literal(rp_start_date), dataset
    ).resolve_value(None)

    assert actual == expected


class _Commissioner(DSPStructuredModel):
    OrgIDComm = SubmittedAttribute("OrgIDComm", str)  # type: SubmittedAttribute


class _Patient(DSPStructuredModel):
    Name = SubmittedAttribute("Name", str)  # type: SubmittedAttribute
    Dob = SubmittedAttribute("Dob", str)  # type: SubmittedAttribute
    Commissioners = RepeatingSubmittedAttribute(
        "Commissioners", _Commissioner
    )  # type: RepeatingSubmittedAttribute
    OrgIDComm = SubmittedAttribute("OrgIDComm", str)  # type: SubmittedAttribute


def test_conditional_comm_ids(sample_patient):
    commissioner_ids = ConditionalCommIds(
        primary_expr=Select(_Patient.Commissioners),
        secondary_expr=Select(_Patient.OrgIDComm),
    ).resolve_value(sample_patient)

    assert set(commissioner_ids) == set(
        [c.OrgIDComm for c in sample_patient.Commissioners]
    )

    commissioner_ids = ConditionalCommIds(
        primary_expr=Select(_Patient.OrgIDComm),
        secondary_expr=Select(_Patient.Commissioners),
    ).resolve_value(sample_patient)

    assert set(commissioner_ids) == {sample_patient.OrgIDComm}


def test_conditional_with_none(patient_with_nones):
    commissioner_ids = ConditionalCommIds(
        primary_expr=Select(_Patient.OrgIDComm),
        secondary_expr=Select(_Patient.Commissioners),
    ).resolve_value(patient_with_nones)

    assert set(commissioner_ids) == set(
        [c.OrgIDComm for c in patient_with_nones.Commissioners]
    ) - {None}


def test_conditional_with_none_secondary(patient_all_nones):
    commissioner_ids = ConditionalCommIds(
        primary_expr=Select(_Patient.Commissioners),
        secondary_expr=Select(_Patient.OrgIDComm),
    ).resolve_value(patient_all_nones)

    assert set(commissioner_ids) == set()


@pytest.mark.parametrize(
    "lhs_value, rhs_value, expected",
    [
        (None, None, True),
        (1, 1, True),
        (None, 0, False),
        (0, None, False),
        ("1", 1, False),
    ],
)
def test_equals(lhs_value, rhs_value, expected):
    lhs = Mock(ModelExpression)
    lhs.resolve_value.return_value = lhs_value

    rhs = Mock(ModelExpression)
    rhs.resolve_value.return_value = rhs_value

    predicate = Equals(lhs, rhs)
    actual = predicate.resolve_value(model={})  # type: bool

    assert actual == expected


@pytest.mark.parametrize(
    "input_value, lookup_list, expected",
    [
        (None, None, False),
        (None, [1, 2], False),
        (1, [1, 2], True),
        (1, [4, 2, 3, 5], False),
        ("1", ["1", "2"], True),
        ("1", ["4", "2", "3", "5"], False),
    ],
)
def test_contain_any(input_value, lookup_list, expected):
    input = Mock(ModelExpression)
    input.resolve_value.return_value = input_value

    lookup = Mock(ModelExpression)
    lookup.resolve_value.return_value = lookup_list

    predicate = Includes(input, lookup)
    actual = predicate.resolve_value(model={})  # type: bool
    assert actual == expected


@pytest.mark.parametrize(
    "findschemeinuse, icd10code, expected_icd10code",
    [
        (
            "04",
            "83482000",
            "83482000",
        ),  # Set output to '83482000' where findschemeinuse is '04' and icd10 is '83482000'.
        ("01", "A0.1", "-3"),  # Removing single non-alphanumeric and matching.
        ("01", "a01", "-3"),  # Converting to upper and matching.
        ("01", "a0.1", "-3"),  # All cleaning together and matching.
        ("01", "A33", "A33X"),  # Add X and lookup success.
        (
            "01",
            "A0.1A",
            "-3",
        ),  # Removing single non-alphanumeric and matching - With asterisk.
        ("01", "a01A", "-3"),  # Converting to upper and matching - With asterisk.
        ("01", "a0.1a", "-3"),  # All cleaning together and matching - With asterisk.
        ("01", "A33A", "A33XA"),  # Add X and lookup success - With asterisk.
        (
            "01",
            "A0.1D",
            "-3",
        ),  # Removing single non-alphanumeric and matching - With dagger.
        ("01", "a01D", "-3"),  # Converting to upper and matching - With dagger.
        ("01", "a0.1d", "-3"),  # All cleaning together and matching - With dagger.
        ("01", "A33D", "A33XD"),  # Add X and lookup success - With dagger.
        ("01", "ZZ9", "-3"),  # Invalid code no match, where findschemeinuse is '1'
        ("03", "ZZ9", "-3"),  # Invalid code no match, where findschemeinuse is not '4'
        (
            "03",
            "G041",
            "-3",
        ),  # Invalid code - more than 3 chars - no match, where findschemeinuse is not '4'
        # Further tests to check behaviour following bug fix
        ("01", "F32", "F329"),
        ("01", "F10D", "F109D"),
        ("01", "F10", "F109"),
        ("01", "F10", "F109"),
        ("01", "A33D", "A33XD"),
        ("01", "F400A", "F400A"),
        ("01", "F400D", "F400D"),
        ("01", "F400", "F400"),
        ("01", "G041", "G041"),  # Original test was incorrect
        ("01", "83482000D", "-3"),
        ("01", None, None),
        (None, "A01", None),  # Where findschemeinuse is not populated
        ("01", None, None),  # Where submitted icd10code is not populated
        ("01", "F05.0", "F050"),
        ("01", "adb,sd" "", "-3"),  # testing special characters
        ("01", "adb!sd" "D", "-3"),  # testing special characters
        ("01", "A00", "A009"),
        ("01", "A01*A", "-3"),
        ("01", "A01*D", "-3"),
        ("01", "A01*", "-3"),
        ("01", "a01*a", "-3"),
        ("01", "a01*d", "-3"),
        ("01", "a01*", "-3"),
        ("01", "a01*A", "-3"),
        ("01", "a01*D", "-3"),
        ("01", "A01†A", "-3"),
        ("01", "A00.0A", "A000A"),
        ("01", "A00.0A", "A000A"),
        ("01", "A00.0D", "A000D"),
        ("01", "A00.0A", "A000A"),
        ("01", "m45.X0", "M45X0"),
        ("01", "A33*A", "A33XA"),
        ("01", "A33*D", "A33XD"),
        ("01", "A33*", "A33X"),
        ("01", "A00*A", "A009A"),
        ("01", "A000†A", "A000A"),
        ("01", "A000†D", "A000D"),
        ("01", "A000†", "A000"),
        ("01", "a33†a", "A33XA"),
        ("01", "a33†d", "A33XD"),
        ("01", "a33†", "A33X"),
        ("01", "a00†a", "A009A"),
        ("01", "a00†d", "A009D"),
        ("01", "a00†", "A009"),
        ("01", "a000†a", "A000A"),
    ],
)
def test_match_clean_icd10_code(findschemeinuse, icd10code, expected_icd10code):
    class _Info(DSPStructuredModel):
        FindSchemeInUse = SubmittedAttribute("FindSchemeInUse", str)
        ICD10Code = SubmittedAttribute("ICD10Code", str)

    class Info(_Info):
        __concrete__ = True

        Validated_ICD10Code = DerivedAttribute(
            "Validated_ICD10Code",
            str,
            MatchCleanICD10Code(
                icd10code=Select(_Info.ICD10Code),
                findschemeinuse=Select(_Info.FindSchemeInUse),
            ),
        )  # type: DerivedAttribute

    info1 = Info({"FindSchemeInUse": findschemeinuse, "ICD10Code": icd10code})

    assert info1.Validated_ICD10Code == expected_icd10code


@pytest.mark.parametrize(
    "care_contact_date, referral_received_date, expected_time_in_days",
    [
        (date(2020, 1, 1), date(2020, 1, 1), 0),
        (None, None, None),
        (None, date(2020, 1, 1), None),
        (date(2020, 1, 1), None, None),
        (date(2019, 12, 31), date(2020, 1, 1), -1),
        (date(2019, 12, 31), date(2019, 12, 30), 1),
        (date(2019, 1, 1), date(2018, 1, 1), 365),
        (date(2016, 2, 29), date(2016, 2, 1), 28),
        (date(2017, 3, 1), date(2016, 2, 29), 366),
        (datetime(2020, 5, 1, 12, 34, 56, 0), datetime(2020, 5, 1, 23, 59, 59, 0), 0),
        (date(2020, 5, 1), datetime(2020, 5, 1, 23, 59, 59, 0), 0),
        (datetime(2020, 5, 1, 12, 34, 56, 0), date(2020, 5, 1), 0),
        (date(2019, 4, 15), date(2005, 2, 17), 5170),
    ],
)
def test_days_between(care_contact_date, referral_received_date, expected_time_in_days):
    class _Info(DSPStructuredModel):
        CareContactDate = SubmittedAttribute("CareContactDate", date)
        ReferralReceivedDate = SubmittedAttribute("ReferralReceivedDate", date)

    class Info(_Info):
        __concrete__ = True

        DerivDaysBetween = DerivedAttribute(
            "DerivDaysBetween",
            str,
            DaysBetween(
                Select(_Info.CareContactDate), Select(_Info.ReferralReceivedDate)
            ),
        )  # type: DerivedAttribute

    info1 = Info(
        {
            "CareContactDate": care_contact_date,
            "ReferralReceivedDate": referral_received_date,
        }
    )

    assert info1.DerivDaysBetween == expected_time_in_days


@pytest.mark.parametrize(
    "care_contact_date, referral_received_date, expected_time_in_days",
    [
        (date(2020, 1, 1), date(2020, 1, 1), 1),
        (None, None, None),
        (None, date(2020, 1, 1), None),
        (date(2020, 1, 1), None, None),
        (date(2019, 12, 31), date(2020, 1, 1), 0),
        (date(2019, 12, 31), date(2019, 12, 30), 2),
        (date(2019, 1, 1), date(2018, 1, 1), 366),
        (date(2016, 2, 29), date(2016, 2, 1), 29),
        (date(2017, 3, 1), date(2016, 2, 29), 367),
        (datetime(2020, 5, 1, 12, 34, 56, 0), datetime(2020, 5, 1, 23, 59, 59, 0), 1),
        (date(2020, 5, 1), datetime(2020, 5, 1, 23, 59, 59, 0), 1),
        (datetime(2020, 5, 1, 12, 34, 56, 0), date(2020, 5, 1), 1),
        (date(2019, 4, 15), date(2005, 2, 17), 5171),
    ],
)
def test_days_between_plus_one(
    care_contact_date, referral_received_date, expected_time_in_days
):
    class _Info(DSPStructuredModel):
        CareContactDate = SubmittedAttribute("CareContactDate", date)
        ReferralReceivedDate = SubmittedAttribute("ReferralReceivedDate", date)

    class Info(_Info):
        __concrete__ = True

        DerivDaysBetween = DerivedAttribute(
            "DerivDaysBetween",
            str,
            DaysBetween(
                Select(_Info.CareContactDate),
                Select(_Info.ReferralReceivedDate),
                days_to_add=1,
            ),
        )  # type: DerivedAttribute

    info1 = Info(
        {
            "CareContactDate": care_contact_date,
            "ReferralReceivedDate": referral_received_date,
        }
    )

    assert info1.DerivDaysBetween == expected_time_in_days


@pytest.mark.parametrize(
    "start_date, start_time, end_date, end_time, expected_minutes",
    [
        (
            date(2020, 1, 1),
            datetime(2020, 1, 1, 10, 0, 0),
            date(2020, 1, 2),
            datetime(2020, 1, 2, 10, 0, 0),
            1440,
        ),
        (
            date(2020, 1, 1),
            datetime(2020, 1, 1, 10, 0, 0),
            date(2020, 1, 2),
            datetime(2020, 1, 2, 10, 10, 0),
            1450,
        ),
        (
            date(2020, 1, 1),
            datetime(2020, 1, 1, 10, 0, 0),
            date(2020, 1, 2),
            datetime(2020, 1, 2, 10, 8, 10),
            1448,
        ),
        (
            date(2020, 1, 1),
            datetime(2020, 1, 1, 10, 0, 0),
            date(2020, 1, 2),
            datetime(2020, 1, 2, 10, 0, 55),
            1440,
        ),
    ],
)
def test_minutes_between(start_date, start_time, end_date, end_time, expected_minutes):
    class _Info(DSPStructuredModel):
        StartDate = SubmittedAttribute("StartDate", date)
        EndDate = SubmittedAttribute("EndDate", date)
        StartTime = SubmittedAttribute("StartTime", datetime)
        EndTime = SubmittedAttribute("EndTime", datetime)

    class Info(_Info):
        __concrete__ = True

        DerivWaitingMinutes = DerivedAttribute(
            "DerivWaitingMinutes",
            int,
            MinutesBetween(
                start_date_expr=Select(_Info.StartDate),
                end_date_expr=Select(_Info.EndDate),
                start_time_expr=Select(_Info.StartTime),
                end_time_expr=Select(_Info.EndTime),
            ),
        )  # type: int

    info1 = Info(
        {
            "StartDate": start_date,
            "StartTime": start_time,
            "EndDate": end_date,
            "EndTime": end_time,
        }
    )

    assert info1.DerivWaitingMinutes == expected_minutes


@pytest.mark.parametrize(
    "referral_received_date, reporting_period_start_date, reporting_period_end_date, expected_date_in_between",
    [
        (date(2020, 1, 15), date(2020, 1, 1), date(2020, 1, 31), True),
        (date(2020, 2, 15), date(2020, 1, 1), date(2020, 1, 31), False),
        (None, date(2020, 1, 1), date(2020, 1, 31), False),
        (date(2020, 2, 15), None, date(2020, 1, 31), False),
        (date(2020, 2, 15), date(2020, 1, 1), None, False),
    ],
)
def test_date_in_between(
    referral_received_date,
    reporting_period_start_date,
    reporting_period_end_date,
    expected_date_in_between,
):
    class _Info(DSPStructuredModel):
        ReferralReceivedDate = SubmittedAttribute("ReferralReceivedDate", date)
        ReportingPeriodStartDate = SubmittedAttribute("ReportingPeriodStartDate", date)
        ReportingPeriodEndDate = SubmittedAttribute("ReportingPeriodEndDate", date)

    class Info(_Info):
        __concrete__ = True

        DerivDateInBetween = DerivedAttribute(
            "DerivDateInBetween",
            str,
            DateInBetween(
                Select(_Info.ReferralReceivedDate),
                Select(_Info.ReportingPeriodStartDate),
                Select(_Info.ReportingPeriodEndDate),
            ),
        )  # type: DerivedAttribute

    info1 = Info(
        {
            "ReferralReceivedDate": referral_received_date,
            "ReportingPeriodStartDate": reporting_period_start_date,
            "ReportingPeriodEndDate": reporting_period_end_date,
        }
    )

    assert info1.DerivDateInBetween == expected_date_in_between


@pytest.mark.parametrize(
    "referral_received_date, expected_not_null",
    [
        (date(2020, 1, 15), True),
        (None, False),
    ],
)
def test_not_null(referral_received_date, expected_not_null):
    class _Info(DSPStructuredModel):
        ReferralReceivedDate = SubmittedAttribute("ReferralReceivedDate", date)

    class Info(_Info):
        __concrete__ = True

        DerivNotNull = DerivedAttribute(
            "DerivNotNull", str, NotNull(Select(_Info.ReferralReceivedDate))
        )  # type: DerivedAttribute

    info1 = Info({"ReferralReceivedDate": referral_received_date})

    assert info1.DerivNotNull == expected_not_null


@pytest.mark.parametrize(
    "snomed_id, care_contact_date, expected",
    [
        # Blank field tests
        (None, date(2010, 1, 1), None),
        ("962851000000103", None, None),
        (None, None, None),
        # Valid code and date
        (
            "962851000000103",
            date(2018, 1, 1),
            "Patient Activation Measure Level (observable entity)",
        ),
        (
            "1105421000000101",
            date(2020, 1, 1),
            "Needs and Provision Complexity Scale version 6 medical care needs score (observable entity)",
        ),
        (
            "953621000000109",
            date(2018, 1, 1),
            "Ages and Stages Questionnaires Third Edition 60 month questionnaire - fine motor score (observable entity)",
        ),
        (
            "720433000",
            date(2018, 1, 1),
            "Patient Health Questionnaire Nine Item score (observable entity)",
        ),
        (
            "803351000000106",
            date(2020, 1, 1),
            "Whooley depression screen score (observable entity)",
        ),
        # Valid code but contact date before effective_time in DSS Corp
        ("962851000000103", date(2015, 1, 1), None),
        ("1105421000000101", date(2019, 1, 1), None),
        ("803351000000106", date(2009, 1, 1), None),
    ],
)
def test_derived_snomed_term(snomed_id, care_contact_date, expected):
    class _DeriveSnomedTerm(DSPStructuredModel):
        SNOMED_ID = SubmittedAttribute("SNOMED_ID", str)
        Contact_Date = SubmittedAttribute("Contact_Date", date)

    class DeriveSnomedCTTerm(_DeriveSnomedTerm):
        __concrete__ = True
        SNOMEDCTAssTerm = DerivedAttribute(
            "SNOMEDCTAssTerm",
            str,
            (
                DeriveSnomedTerm(
                    snomed_code_expr=Select(_DeriveSnomedTerm.SNOMED_ID),
                    point_in_time_expr=Select(_DeriveSnomedTerm.Contact_Date),
                )
            ),
        )

    deriveterm = DeriveSnomedCTTerm(
        {
            "SNOMED_ID": snomed_id,
            "Contact_Date": care_contact_date,
        }
    )
    assert deriveterm.SNOMEDCTAssTerm == expected


def test_string_to_datetime():
    class _Info(DSPStructuredModel):
        MyDate = SubmittedAttribute("MyDate", str)
        MyTime = SubmittedAttribute("MyTime", str)

    class Info(_Info):
        __concrete__ = True

        DateTime = DerivedAttribute(
            "DateTime",
            datetime,
            StringToDateTime(
                Concat(
                    [
                        Select(_Info.MyDate),
                        Select(_Info.MyTime),
                    ]
                ),
                "%d/%m/%Y%H:%M",
            ),
        )  # type: DerivedAttribute

    info1 = Info(
        {
            "MyDate": "23/05/2020",
            "MyTime": "14:32",
        }
    )

    assert info1.DateTime == datetime(2020, 5, 23, 14, 32)


@pytest.mark.parametrize(
    "ethnic_category, expected_validated_ethnic_category",
    [
        ("", "-1"),
        (None, "-1"),
        ("0", "-1"),
        ("A", "A"),
        ("Q", "-3"),
        # ("a", -3),
        ("9", "99"),
        ("99", "99"),
        ("Hello World", "-3"),
    ],
)
def test_clean_ethnic_category(ethnic_category, expected_validated_ethnic_category):
    class _DeriveValidatedEthnicCategory(DSPStructuredModel):
        EthnicCategory = SubmittedAttribute("EthnicCategory", str)

    class DeriveValidatedEthnicCategory(_DeriveValidatedEthnicCategory):
        __concrete__ = True
        Validated_EthnicCategory = DerivedAttribute(
            "Validated_EthnicCategory",
            str,
            CleanEthnicCategory(
                ethnic_category=Select(_DeriveValidatedEthnicCategory.EthnicCategory)
            ),
        )

    derived_val = DeriveValidatedEthnicCategory(
        {
            "EthnicCategory": ethnic_category,
        }
    )

    assert derived_val.Validated_EthnicCategory == expected_validated_ethnic_category


def test_default_none():
    result = DefaultNone().resolve_value(None)
    assert result is None


@pytest.mark.parametrize(
    "input,expected",
    [
        (["a", "b", "d"], "a"),
        (
            [datetime(2000, 1, 1), datetime(1900, 3, 3), datetime(2020, 2, 2)],
            datetime(1900, 3, 3),
        ),
        ([-10, 20, -100], -100),
    ],
)
def test_min_resolved(input: List, expected):
    result = MinResolved(Literal(input)).resolve_value(None)
    assert result == expected


@pytest.mark.parametrize(
    "input,filter_func,expected",
    [
        (["a", "b", "d"], lambda x: x in ["a", "d"], ["a", "d"]),
        (
            [datetime(2000, 1, 1), datetime(1900, 3, 3), datetime(2020, 2, 2)],
            lambda x: x > datetime(1999, 1, 1),
            [datetime(2000, 1, 1), datetime(2020, 2, 2)],
        ),
        ([-10, 20, -100], lambda x: x == 20, [20]),
        ([1, 2, 3], lambda x: x > 4, []),
    ],
)
def test_filter_resolved(input: List, filter_func: Callable, expected):
    result = FilterResolved(Literal(input), filter_func).resolve_value(None)
    assert result == expected


@pytest.mark.parametrize(
    "input,filter_func,expected",
    [
        (["a", "b", "d"], lambda x: x in ["a", "d"], "a"),
        (
            [datetime(2000, 1, 1), datetime(1900, 3, 3), datetime(2020, 2, 2)],
            lambda x: x > datetime(1999, 1, 1),
            datetime(2000, 1, 1),
        ),
        ([-10, 20, -100], lambda x: x == 20, 20),
        ([1, 2, 3], lambda x: x > 4, None),
    ],
)
def test_min_filter_resolved(input: List, filter_func: Callable, expected):
    result = MinFilterResolved(Literal(input), filter_func).resolve_value(None)
    assert result == expected


@pytest.mark.parametrize(
    "reporting_period_start_date, reporting_period_end_date, LOA_start_date, LOA_end_date, expected_overlap",
    [
        # LOA start and end date
        (
            datetime(2018, 10, 9),
            datetime(2018, 10, 18),
            datetime(2018, 10, 10),
            datetime(2018, 10, 15),
            6,
        ),
        # reporting period start date and reporting period end date
        (
            datetime(2018, 10, 10),
            datetime(2018, 10, 15),
            datetime(2018, 10, 9),
            None,
            7,
        ),
        # reporting period start date and LOA end date
        (
            datetime(2018, 10, 10),
            datetime(2018, 10, 18),
            datetime(2018, 10, 9),
            datetime(2018, 10, 15),
            6,
        ),
        # LOA start date and reporting period end date
        (
            datetime(2018, 10, 9),
            datetime(2018, 10, 15),
            datetime(2018, 10, 10),
            None,
            7,
        ),
    ],
)
def test_overlap(
    reporting_period_start_date,
    reporting_period_end_date,
    LOA_start_date,
    LOA_end_date,
    expected_overlap,
):

    olap = OverlapDays(
        Literal(reporting_period_start_date),
        Literal(reporting_period_end_date),
        Literal(LOA_start_date),
        Literal(LOA_end_date),
        days_to_add_expr=Literal(1),
    ).resolve_value(None)

    assert olap == expected_overlap


@pytest.mark.parametrize(
    "gp_practice_code, event_date, expected_unitary_authority",
    [
        ("A81001", date(2020, 12, 12), "E06000004"),
        ("A81004", date(2020, 12, 12), "E06000002"),
        ("Y02551", date(2020, 12, 12), "E08000025"),
        ("B86018", date(2020, 12, 12), "E08000035"),
        ("P88009", date(2020, 12, 12), "E08000007"),
        ("P88009", date(1901, 1, 1), None),
        ("XMD00", date(2019, 1, 1), None),
        ("X99998", date(2019, 1, 1), None),
        ("1NV4L1D", date(2020, 12, 12), None),
        (None, date(2020, 12, 12), None),
    ],
)
def test_derive_unitary_authority_from_gp_code(
    gp_practice_code, event_date, expected_unitary_authority
):
    unitary_authority = UnitaryAuthorityFromGPPracticeCode(
        Literal(gp_practice_code), Literal(event_date)
    ).resolve_value(None)

    assert unitary_authority == expected_unitary_authority


@pytest.mark.parametrize(
    "postcode, point_in_time, field_name, expected",
    [
        (None, None, ONSRecordPaths.UNITARY_AUTHORITY, None),
        (None, date(2015, 5, 1), ONSRecordPaths.UNITARY_AUTHORITY, None),
        ("LS14JL", date(2015, 5, 1), ONSRecordPaths.UNITARY_AUTHORITY, "E08000035"),
        ("LS1 4JL", date(2015, 5, 1), ONSRecordPaths.UNITARY_AUTHORITY, "E08000035"),
        (
            "LS999ZZ",
            date(2018, 10, 3),
            ONSRecordPaths.UNITARY_AUTHORITY,
            None,
        ),  # invalid postcode
        ("LS14JL", date(2015, 5, 1), ONSRecordPaths.LOWER_LAYER_SOA, "E01033015"),
        ("LS1 4JL", date(2015, 5, 1), ONSRecordPaths.LOWER_LAYER_SOA, "E01033015"),
        (
            "LS999ZZ",
            date(2018, 10, 3),
            ONSRecordPaths.LOWER_LAYER_SOA,
            None,
        ),  # invalid postcode
        ("EH4 9DX", date(2018, 10, 3), ONSRecordPaths.LOWER_LAYER_SOA, "S01008864"),
    ],
)
def test_derive_postcode(postcode, point_in_time, field_name, expected):
    actual = DerivePostcode(
        Literal(postcode), Literal(point_in_time), field_name
    ).resolve_value(None)

    assert actual == expected


@pytest.mark.parametrize(
    "postcode, point_in_time, icb_switchover, expected",
    [
        # Sanity test
        (None, None, False, None),
        # No postcode returns null
        (None, date(2015, 5, 1), False, None),
        # No point in time means current CCG is returned
        ("LS16 6EH", None, False, "15F"),
        ("LS16 6EH", None, True, "15F"),
        # Particular point in time returns correct value
        ("LS16 6EH", date(2015, 5, 1), False, "03C"),
        ("LS16 6EH", date(2015, 5, 1), True, "03C"),
    ],
)
def test_ccg_from_postcode(postcode, point_in_time, icb_switchover, expected):
    actual = CcgFromPostcode(
        Literal(postcode), Literal(point_in_time), Literal(icb_switchover)
    ).resolve_value(None)

    assert actual == expected


@pytest.mark.parametrize(
    "actual_date, check_date, expected",
    [
        # Sanity Check
        (None, None, False),
        # Actual date is null
        (None, date(2023, 3, 31), False),
        # Check date is null
        (date(2015, 5, 1), None, False),
        # Actual date is before or equal to the check date
        (date(2023, 3, 31), date(2023, 3, 31), True),
        # Actual date is after the check date
        (date(2023, 4, 1), date(2023, 3, 31), False),
    ]
)
def test_date_before(actual_date, check_date, expected):

    actual = DateBeforeOrEqualTo(Literal(actual_date), Literal(check_date)).resolve_value(None)
    assert actual == expected


@pytest.mark.parametrize(
    "gp_code, point_in_time, icb_switchover, expected",
    [
        # Sanity Check
        (None, None, False, None),
        # No GP code returns null
        (None, date(2015, 5, 1), False, None),
        # No point in time returns current CCG
        ("F85678", None, False, "93C"),
        # Valid code returns correct value
        ("F85678", date(2017, 1, 1), False, "07X"),
        ("F85678", date(2017, 1, 1), True, "07X"),
        # Affected by ICB Switchover
        ("C83653", date(2022, 6, 30), False, "71E"),
        ("C83653", date(2022, 7, 1), False, "03W"),
        ("C83653", date(2022, 7, 1), True, "71E"),
    ],
)
def test_ccg_from_gp_practice_code(gp_code, point_in_time, icb_switchover, expected):
    actual = CCGFromGPPracticeCode(
        Literal(gp_code), Literal(point_in_time), Literal(icb_switchover)
    ).resolve_value(None)

    assert actual == expected


@pytest.mark.parametrize(
    "postcode, point_in_time, expected",
    [
        # Sanity test
        (None, None, None),
        # No postcode returns null
        (None, date(2022, 7, 1), None),
        # No point in time means current sub-ICB is returned
        ("LS16 6EH", None, "15F"),
        # Particular point in time returns correct value
        ("LS16 6EH", date(2018, 4, 30), "03C"),
        ("B21 0AR", date(2022, 7, 1), "15E"),
        ("DN22 6HZ", date(2022, 7, 31), "02Q"),
        ("DN22 6HZ", date(2022, 8, 1), "02Q"),
    ],
)
def test_sub_icb_from_postcode(postcode, point_in_time, expected):
    actual = SubICBFromPostcode(
        Literal(postcode), Literal(point_in_time)
    ).resolve_value(None)

    assert actual == expected


@pytest.mark.parametrize(
    "postcode, point_in_time, expected",
    [
        # Sanity test
        (None, None, None),
        # No postcode returns null
        (None, date(2022, 7, 1), None),
        # No point in time means current sub-ICB is returned
        ("LS16 6EH", None, "QWO"),
        # Particular point in time returns correct value
        ("LS16 6EH", date(2020, 5, 31), "E54000005"),
        ("SK13 8DN", date(2022, 7, 1), "QJ2"),
        ("PE8  5HZ", date(2022, 7, 31), "QPM"),
        ("PE8  5HZ", date(2022, 8, 1), "QPM"),
    ],
)
def test_icb_from_postcode(postcode, point_in_time, expected):
    actual = ICBFromPostcode(Literal(postcode), Literal(point_in_time)).resolve_value(
        None
    )

    assert actual == expected


@pytest.mark.parametrize(
    "gp_code, point_in_time, expected",
    [
        # Sanity Check
        (None, None, None),
        # No GP code returns null
        (None, date(2020, 3, 28), None),
        # No point in time returns current sub-ICB
        ("F85678", None, "93C"),
        # Valid code returns correct value
        ("F85678", date(2020, 3, 28), "07X"),
    ],
)
def test_sub_icb_from_gp_practice_code(gp_code, point_in_time, expected):
    actual = SubICBFromGPPracticeCode(
        Literal(gp_code), Literal(point_in_time)
    ).resolve_value(None)

    assert actual == expected


@pytest.mark.parametrize(
    "sub_icb, point_in_time, expected",
    [
        # Sanity test
        (None, None, None),
        # No sub-icb returns null
        (None, date(2022, 7, 1), None),
        # No point in time means current ICB is returned
        ("06Q", None, "QH8"),
        # Particular point in time returns correct value
        ("06Q", date(2022, 8, 1), "QH8"),
    ],
)
def test_icb_from_sub_icb(sub_icb, point_in_time, expected):
    actual = ICBFromSubICB(Literal(sub_icb), Literal(point_in_time)).resolve_value(None)

    assert actual == expected


@pytest.mark.parametrize(
    "starting_date, days_to_add, months_to_add, years_to_add, expected_date",
    [
        (None, 1, 0, 0, None),
        (date(2017, 1, 1), 0, 0, 0, date(2017, 1, 1)),
        (date(2017, 1, 1), 1, 0, 0, date(2017, 1, 2)),
        (date(2017, 1, 1), 0, 2, 0, date(2017, 3, 1)),
        (date(2017, 1, 1), 0, 0, 3, date(2020, 1, 1)),
        (date(2017, 1, 1), 2, 6, 1, date(2018, 7, 3)),
    ],
)
def test_add_to_date(
    starting_date, days_to_add, months_to_add, years_to_add, expected_date
):
    actual = AddToDate(
        input_date_expr=Literal(starting_date),
        days_to_add_expr=Literal(days_to_add),
        months_to_add_expr=Literal(months_to_add),
        years_to_add_expr=Literal(years_to_add),
    ).resolve_value(None)

    assert actual == expected_date


@mock.patch("dsp.ref_data.providers.ODSProvider._get")
@pytest.mark.parametrize(
    "org_postcode, expected_distance",
    [
        ("LS11 5BZ", 11),
        ("CB9 8HN", 243),
    ],
)
def test_org_distance(m_get, org_postcode, expected_distance):
    from_postcode = "LS19 6EF"

    # A dummy record will always be returned with good historical postcodes
    m_get.return_value = ODSRecord(
        {
            ODSRecordPaths.POSTCODE: "",
            ODSRecordPaths.EFFECTIVE_FROM: 20110101,
            ODSRecordPaths.HISTORICAL_POSTCODES: [
                {"from": 20120101, "to": None, "postcode": org_postcode}
            ],
            ODSRecordPaths.NAME: "TESTY MC TESTERSON",
            ODSRecordPaths.ORG_CODE: "TESTY",
            ODSRecordPaths.RELS: [],
            ODSRecordPaths.ROLES: [],
            ODSRecordPaths.SUCCESSIONS: [],
        }
    )

    actual_postcode_1 = OrgDistance(
        Literal("TESTY"), Literal(from_postcode), Literal(date(2022, 1, 1))
    ).resolve_value(None)

    assert actual_postcode_1 == expected_distance


@pytest.mark.parametrize(
    "source_code, point_in_time, expected",
    [
        ("RGD01", 20200101, "RGD"),  # NHS Trust Site in NHS Trust
        ("RGD01", None, "RGD"),  # NHS Trust Site in NHS Trust with no point(s) in time given
        ("RGD01", 19950101, "RGD01"),  # Where point in time is invalid
        ("RGD02", 20200101, "RGD"),  # Another site in same NHS trust
        ("R1A01", 20200101, "R1A"),  # Another site and trust
        ("abc12", 20200101, None),  # Invalid code
    ],
)
def test_ProviderFromSiteCode(source_code, point_in_time, expected):
    provider_code = ProviderFromSiteCode(
        Literal(source_code), Literal(point_in_time)
    ).resolve_value(None)

    assert provider_code == expected


@pytest.mark.parametrize(
    "postcode, point_in_time, alternative_point_in_time, expected",
    [
        # Able to find an LSOA from a given postcode with a valid Point in time
        ("LS1 4AP", datetime(2022, 1, 1, 12, 00, 00), None, "E01033015"),
        # Able to find an LSOA from a given postcode with a valid backup Point in time
        ("LS1 4AP", None, datetime(2022, 1, 1, 12, 00, 00), "E01033015"),
        # Unable to find an LSOA from a incorrect Postcode
        ("LS1", datetime(2022, 1, 1, 12, 00, 00), None, None),
        # Unable to find an LSOA from a invalid date
        ("LS1 4AP", datetime(1965, 1, 1, 12, 00, 00), None, None),
        # Unable to find an LSOA due to a missing postcode
        (None, datetime(1965, 1, 1, 12, 00, 00), None, None),
    ],
)
def test_LowerLayerSuperOutputFromPostcode(
    postcode: str,
    point_in_time: Union[date, datetime],
    alternative_point_in_time: Union[date, datetime],
    expected: str,
):
    find_lsoa = LowerLayerSuperOutputAreaFromPostcode(
        Literal(postcode), Literal(point_in_time), Literal(alternative_point_in_time)
    ).resolve_value(None)

    assert find_lsoa == expected


@pytest.mark.parametrize(
    "columns, expected",
    [
        (
            [Literal("TestVal"), Literal(datetime(2020, 1, 1, 12, 0, 0)), Literal(1)],
            True,
        ),
        (
            [
                Literal("TestVal"),
                Literal(datetime(2020, 1, 1, 12, 0, 0)),
                Literal(None),
            ],
            False,
        ),
    ],
)
def test_MultipleColumnsNotNull(columns: List[ModelExpression], expected: bool) -> bool:
    assert MultipleColumnsNotNull(columns) == expected


@pytest.mark.parametrize(
    "field_list, expected",
    [
        ([Literal(None), Literal("2021-01-03")], "2021-01-03"),
        ([Literal(1), Literal(2)], 2),
        (
            [
                Literal("a string"),
                Literal("another string"),
                Literal(None),
                Literal(None),
            ],
            "a string",
        ),
        ([Literal(None), Literal(None), Literal(None)], None),
        (
            [Literal(None), Literal(None), Literal(datetime(2020, 5, 7, 12, 0, 0))],
            datetime(2020, 5, 7, 12, 0, 0),
        ),
    ],
)
def test_Coalesce(field_list, expected):
    assert Coalesce(field_list) == expected


def test_as_type():
    """Unit tests for as_type() utility"""
    assert as_type(float, "1") == 1.0
    assert as_type(int, "1") == 1
    assert as_type(float, "1.23") == 1.23
    assert as_type(float, "-1.23") == -1.23
    assert as_type(float, None) is None
    assert as_type(float, "something else") is None
    assert as_type(float, None, default=3.14) == 3.14

    with pytest.raises(Exception):

        def blow_up(_):
            # not TypeError or ValueError
            raise Exception()

        as_type(blow_up, "some value")


@pytest.mark.parametrize(
    "msd202_id_0, msd202_id_1, snomed_code_0, snomed_code_1, ucum_0, ucum_1,"
    "obs_value_0, obs_value_1, weight_0, weight_1, height_0, height_1, expected",
    [
        (
            1,
            3,
            60621009,
            60621009,
            "kg/m²",
            "kg/m²",
            "25.25",
            "30.30",
            None,
            None,
            None,
            None,
            "30.30",
        ),  # test MasterSnomedCTObsCode get latest valid record
        (
            1,
            3,
            60621009,
            00000000,
            "kg/m²",
            "kg/m²",
            "25.25",
            "30.30",
            None,
            None,
            None,
            None,
            "25.25",
        ),  # test snomed match
        (
            1,
            3,
            60621009,
            60621009,
            "kg/m²",
            "beans",
            "25.25",
            "30.30",
            None,
            None,
            None,
            None,
            "25.25",
        ),  # test ucum type 1 match
        (
            1,
            3,
            60621009,
            60621009,
            "kg/m2",
            "beans",
            "25.25",
            "30.30",
            None,
            None,
            None,
            None,
            "25.25",
        ),  # test ucum type 2 match
        (
            1,
            3,
            60621009,
            60621009,
            "beans",
            "beans",
            "25.25",
            "30.30",
            None,
            None,
            None,
            None,
            None,
        ),  # test ucum type no match
        (
            1,
            3,
            60621009,
            60621009,
            "KG/M2",
            "beans",
            "25.25",
            "30.30",
            None,
            None,
            None,
            None,
            "25.25",
        ),  # test ucum case insensitivity
        (
            1,
            3,
            60621009,
            60621009,
            "kg/m²",
            "kg/m²",
            "25.25",
            "nope.",
            None,
            None,
            None,
            None,
            "25.25",
        ),  # test non numeric value check
        (
            1,
            3,
            00000000,
            11111111,
            None,
            None,
            None,
            None,
            "67",
            "58",
            "140",
            "153",
            "24.78",
        ),  # test calculate bmi from latest record
        (
            1,
            3,
            00000000,
            11111111,
            None,
            None,
            None,
            None,
            "67.5",
            None,
            None,
            "145",
            "32.10",
        ),  # test weight and height in different records
        (
            1,
            3,
            88888888,
            66666666,
            "kg/m²",
            "kg/m²",
            "25.25",
            "30.30",
            "-2",
            None,
            "rubbash",
            None,
            None,
        ),  # test no matching snomed, no valid weight or height
        (
            1,
            3,
            88888888,
            66666666,
            "kg/m²",
            "kg/m²",
            "25.25",
            "30.30",
            "-2",
            None,
            "125",
            None,
            None,
        ),  # test only height achieved
        (
            1,
            3,
            88888888,
            66666666,
            "kg/m²",
            "kg/m²",
            "25.25",
            "30.30",
            "42",
            None,
            "rubbash",
            None,
            None,
        ),  # test only weight achieved
    ],
)
def test_derived_bmi_multiple_care_activities(
    pregnancy_and_booking_details: dict,
    care_contacts,
    care_activities: dict,
    msd202_id_0: int,
    msd202_id_1: int,
    snomed_code_0: int,
    snomed_code_1: int,
    ucum_0: str,
    ucum_1: str,
    obs_value_0: str,
    obs_value_1: str,
    weight_0: str,
    weight_1: str,
    height_0: str,
    height_1: str,
    expected: str,
):
    care_act_0 = care_activities.copy()
    care_act_0["MSD202_ID"] = msd202_id_0
    care_act_0["UCUMUnit"] = ucum_0
    care_act_0["MasterSnomedCTObsCode"] = snomed_code_0
    care_act_0["ObsValue"] = obs_value_0
    care_act_0["PersonHeight"] = height_0
    care_act_0["PersonWeight"] = weight_0

    care_act_1 = care_activities.copy()
    care_act_1["MSD202_ID"] = msd202_id_1
    care_act_1["UCUMUnit"] = ucum_1
    care_act_1["MasterSnomedCTObsCode"] = snomed_code_1
    care_act_1["ObsValue"] = obs_value_1
    care_act_1["PersonHeight"] = height_1
    care_act_1["PersonWeight"] = weight_1

    care_act_na = care_activities.copy()
    care_act_na["MSD202_ID"] = 2
    care_act_na["ObsValue"] = None
    care_act_na["PersonHeight"] = None
    care_act_na["PersonWeight"] = None

    care_contacts["CareActivities"] = [
        CareActivity(care_act_0),
        CareActivity(care_act_1),
    ]
    pregnancy_and_booking_details["CareContacts"] = [CareContact(care_contacts)]

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)

    assert preg_and_booking.CareContacts[0].DerivedBMI == expected


def test_derived_bmi_no_care_activities(
    pregnancy_and_booking_details: dict,
    care_contacts,
    care_activities: dict,
):

    care_contacts["CareActivities"] = []
    pregnancy_and_booking_details["CareContacts"] = [CareContact(care_contacts)]

    preg_and_booking = PregnancyAndBookingDetails(pregnancy_and_booking_details)

    assert preg_and_booking.CareContacts[0].DerivedBMI is None
