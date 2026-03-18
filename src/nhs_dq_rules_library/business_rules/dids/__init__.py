from functools import partial
import json
from operator import le, ge
from pyspark.sql.functions import to_date, col, length, lit, when, year, month, datediff, coalesce, months_between, \
    last_day, regexp_replace
from pyspark.sql.types import StringType, TimestampType, ByteType, LongType, StructType
from dsp import udfs
from dsp.common.regex_patterns import YYYY_MM_DD_PATTERN, POSTCODE_PATTERN
from dsp.common.common import length_check, allow_empty, format_check
from dsp.common.conversions import dt_to_yyyyMMdd_dashed, to_tinyint, to_str
from dsp.common.date_formats import date_comparision_expr_with_pattern, valid_date_yyyyMMdd_dashed
from dsp.common.dq_errors import DQErrs
from dsp.datasets.common import Fields as CommonFields, DQRule, UniquenessRule
from dsp.enrichments import enrich_with_meta
from dsp.outputs.dids.output import Fields as Output
from dsp.outputs.dids.submitted import Fields as Submitted
from dsp.loaders.dids.dids_csv import csv_loader
from dsp.loaders.dids.dids_xml import loader
from dsp.validations.common import DateDelta
from dsp.validations.dids.validation_functions import (
    column_not_blank_rule_expr,
    is_active_with_condition,
    length_check_an
)
from dsp.validations.common_validation_functions import date_comparison
from dsp.udfs.misc import PatientSourceSetting, NHSNumberStatus, RefererCodeValidityRegex
from dsp.udfs.ethnic_categories import EthnicCategory
from dsp.shared.constants import FT, METADATA
from dsp.shared.pds.pds_discovery import set_pds_source
from dsp.shared import local_path


class Derived:
    AGE_AT_EVENT = 'AGE_AT_EVENT'


# Loader for local pds test data
def before_local_pds_cohort(spark):
    schema_path = local_path(f'testdata/pds_test_data/pds.pds.json')
    json_schema = json.loads(open(schema_path, 'r').read())
    schema = StructType.fromJson(json_schema)
    df = spark.read.json(local_path("testdata/pds_test_data/file/pds.pds.json"), schema=schema)

    set_pds_source(df)


ENRICHMENTS = [
    partial(
        enrich_with_meta, enrichments=[
            (
                Output.SUBMITTER_ORGANISATION,
                lambda metadata: lit(metadata[METADATA.SENDER_ID])
            )
        ]
    )
]

STRIP_FIELDS = [
    Submitted.NHS_NUMBER,
    Submitted.NHS_NUMBER_STATUS_IND,
    Submitted.PERSON_BIRTH_DATE,
    Submitted.ETHNIC_CATEGORY,
    Submitted.PERSON_GENDER_CODE,
    Submitted.POSTCODE,
    Submitted.PRACTICE_CODE,
    Submitted.PATIENT_SOURCE_SETTING,
    Submitted.REFERRER,
    Submitted.REFERRING_ORG_CODE,
    Submitted.DIAGNOSTIC_TEST_REQUEST_DATE,
    Submitted.DIAG_TEST_REQUEST_REC_DATE,
    Submitted.DIAGNOSTIC_TEST_DATE,
    Submitted.IMAGING_CODE_NICIP,
    Submitted.IMAGING_CODE_SNOMED,
    Submitted.SERVICE_REPORT_DATE,
    Submitted.SITE_CODE_IMAGING,
    Submitted.RADIOLOGICAL_ACCESSION_NUM,
]

EXTRA_FIELD_CLEANSING = [
    (Submitted.NHS_NUMBER, lambda x: regexp_replace(x, r"[\s-]*", "")),
]

DQ_FORMAT_RULES = []

TYPE_COERCIONS = []

# TODO: Temp fix to allow files to be merged will be revisited once schema is finalised
POST_VALIDATION_TYPE_CORRECTIONS = [
    (Submitted.PERSON_GENDER_CODE, to_tinyint, to_str),
    (Submitted.PERSON_BIRTH_DATE, to_date, dt_to_yyyyMMdd_dashed),
    (Submitted.SERVICE_REPORT_DATE, to_date, dt_to_yyyyMMdd_dashed),
    (Submitted.DIAGNOSTIC_TEST_DATE, to_date, dt_to_yyyyMMdd_dashed),
    (Submitted.DIAGNOSTIC_TEST_REQUEST_DATE, to_date, dt_to_yyyyMMdd_dashed),
    (Submitted.DIAG_TEST_REQUEST_REC_DATE, to_date, dt_to_yyyyMMdd_dashed),
]

DQ_META_FIELDS = [
    Submitted.SITE_CODE_IMAGING,
    Submitted.RADIOLOGICAL_ACCESSION_NUM
]

DQ_RULES = [
    # formatting rules
    DQRule(Submitted.DIAGNOSTIC_TEST_DATE, [Submitted.DIAGNOSTIC_TEST_DATE], True,
           *allow_empty(valid_date_yyyyMMdd_dashed(dq_message=DQErrs.DIDS045))),
    DQRule(Submitted.DIAGNOSTIC_TEST_REQUEST_DATE, [Submitted.DIAGNOSTIC_TEST_REQUEST_DATE], True,
           *allow_empty(valid_date_yyyyMMdd_dashed(dq_message=DQErrs.DIDS026))),
    DQRule(Submitted.DIAG_TEST_REQUEST_REC_DATE, [Submitted.DIAG_TEST_REQUEST_REC_DATE], True,
           *allow_empty(valid_date_yyyyMMdd_dashed(dq_message=DQErrs.DIDS036))),
    DQRule(Submitted.SERVICE_REPORT_DATE, [Submitted.SERVICE_REPORT_DATE], True,
           *allow_empty(valid_date_yyyyMMdd_dashed(dq_message=DQErrs.DIDS061))),
    DQRule(Submitted.POSTCODE, [Submitted.POSTCODE], True,
           *allow_empty(format_check(POSTCODE_PATTERN, dq_message=DQErrs.DIDS017))),
    DQRule(Submitted.SITE_CODE_IMAGING, [Submitted.SITE_CODE_IMAGING], True,
            lambda c1: ~column_not_blank_rule_expr(col(c1)) |
                       (length_check_an(c1, min_size=5, max_size=12)), DQErrs.DIDS069),
    DQRule(Submitted.SITE_CODE_IMAGING, [Submitted.SITE_CODE_IMAGING], True,
           lambda c1: column_not_blank_rule_expr(col(c1)), DQErrs.DIDS067),
    DQRule(Submitted.RADIOLOGICAL_ACCESSION_NUM, [Submitted.RADIOLOGICAL_ACCESSION_NUM], True,
           *allow_empty(length_check(max_size=20, dq_message=DQErrs.DIDS072))),
    DQRule(Submitted.NHS_NUMBER, [Submitted.NHS_NUMBER], True,
           *allow_empty(length_check(min_size=10, max_size=10, dq_message=DQErrs.DIDS002))),
    DQRule(Submitted.PERSON_BIRTH_DATE, [Submitted.PERSON_BIRTH_DATE], True,
           *allow_empty(valid_date_yyyyMMdd_dashed(dq_message=DQErrs.DIDS005))),
    DQRule(Submitted.PERSON_BIRTH_DATE, [Submitted.PERSON_BIRTH_DATE, Submitted.DIAGNOSTIC_TEST_DATE], True,
           lambda c1, c2: date_comparison(
               comp_operator=le, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2),
           DQErrs.DIDS006),
    DQRule(Submitted.PERSON_BIRTH_DATE, [Submitted.PERSON_BIRTH_DATE, Submitted.SERVICE_REPORT_DATE], True,
           lambda c1, c2: date_comparison(
               comp_operator=le, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2),
           DQErrs.DIDS008),
    DQRule(Submitted.PERSON_BIRTH_DATE, [Submitted.PERSON_BIRTH_DATE], True,
            lambda c1: date_comparison(
               comp_operator=ge, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, lit('1900-01-02')),
           DQErrs.DIDS009),
    DQRule(Submitted.PERSON_BIRTH_DATE, [Submitted.PERSON_BIRTH_DATE, Submitted.DIAGNOSTIC_TEST_REQUEST_DATE], True,
           lambda c1, c2: col(c1).isNull() | (months_between(last_day(col(c1)), last_day(col(c2))) <= 6),
           DQErrs.DIDS010),
    DQRule(Submitted.DIAGNOSTIC_TEST_REQUEST_DATE,
           [Submitted.DIAGNOSTIC_TEST_REQUEST_DATE, Submitted.DIAG_TEST_REQUEST_REC_DATE], True,
           lambda c1, c2: date_comparison(
               comp_operator=le, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2), DQErrs.DIDS027),
    DQRule(Submitted.DIAGNOSTIC_TEST_REQUEST_DATE, [Submitted.DIAGNOSTIC_TEST_REQUEST_DATE], True,
           lambda c1: date_comparison(
               comp_operator=ge, additional_date_check_expr=date_comparision_expr_with_pattern)
           (c1, to_date(lit('1970-01-01'))),
           DQErrs.DIDS031),
    DQRule(Submitted.DIAGNOSTIC_TEST_REQUEST_DATE, [Submitted.DIAGNOSTIC_TEST_REQUEST_DATE, Submitted.DIAGNOSTIC_TEST_DATE],
           True, lambda c1, c2: date_comparison(
            comp_operator=le, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2), DQErrs.DIDS034),
    DQRule(Submitted.DIAG_TEST_REQUEST_REC_DATE, [Submitted.DIAG_TEST_REQUEST_REC_DATE, Submitted.DIAGNOSTIC_TEST_DATE],
           True, lambda c1, c2: date_comparison(
            comp_operator=le, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2), DQErrs.DIDS043),
    DQRule(Submitted.DIAG_TEST_REQUEST_REC_DATE, [Submitted.DIAG_TEST_REQUEST_REC_DATE, Submitted.SERVICE_REPORT_DATE],
           True, lambda c1, c2: date_comparison(
            comp_operator=le, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2), DQErrs.DIDS044),
    DQRule(Submitted.DIAG_TEST_REQUEST_REC_DATE, [Submitted.DIAG_TEST_REQUEST_REC_DATE, Submitted.PERSON_BIRTH_DATE],
           True, lambda c1, c2: col(c1).isNull() | (months_between(last_day(col(c2)), last_day(col(c1))) <= 6),
           DQErrs.DIDS040),
    DQRule(Submitted.DIAG_TEST_REQUEST_REC_DATE, [Submitted.DIAG_TEST_REQUEST_REC_DATE],
           True, lambda c1: date_comparison(
            comp_operator=ge, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, lit('1970-01-01')),
           DQErrs.DIDS041),
    DQRule(Submitted.DIAGNOSTIC_TEST_DATE, [Submitted.DIAGNOSTIC_TEST_DATE], True, lambda c1: column_not_blank_rule_expr(col(c1)),
           DQErrs.DIDS050),
    DQRule(Submitted.RADIOLOGICAL_ACCESSION_NUM, [Submitted.RADIOLOGICAL_ACCESSION_NUM], True,
           lambda c1: column_not_blank_rule_expr(col(c1)), DQErrs.DIDS070),
    DQRule(Submitted.DIAGNOSTIC_TEST_DATE, [Submitted.DIAGNOSTIC_TEST_DATE], True,
           lambda c1: (col(c1).isNull()) | (~col(c1).rlike(YYYY_MM_DD_PATTERN)) |
                      (months_between(last_day(col("META.EVENT_RECEIVED_TS")), c1) < 7),
           DQErrs.DIDS055),
    DQRule(Submitted.DIAGNOSTIC_TEST_DATE, [Submitted.DIAGNOSTIC_TEST_DATE],
           True, lambda c1: (col(c1).isNull()) | (~col(c1).rlike(YYYY_MM_DD_PATTERN))
                      | (col(c1) <= col("META.EVENT_RECEIVED_TS")),
           DQErrs.DIDS051),
    DQRule(Submitted.SERVICE_REPORT_DATE, [Submitted.SERVICE_REPORT_DATE],
           True, lambda c1: (col(c1).isNull()) | (~col(c1).rlike(YYYY_MM_DD_PATTERN))
                      | (col(c1) <= col("META.EVENT_RECEIVED_TS")),
           DQErrs.DIDS066),
    DQRule(Submitted.SERVICE_REPORT_DATE, [Submitted.DIAGNOSTIC_TEST_REQUEST_DATE, Submitted.SERVICE_REPORT_DATE],
           True, lambda c1, c2: date_comparison(
            comp_operator=le, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2),
           DQErrs.DIDS035),
    DQRule(Submitted.SERVICE_REPORT_DATE, [Submitted.DIAGNOSTIC_TEST_DATE, Submitted.SERVICE_REPORT_DATE],
           True, lambda c1, c2: date_comparison(
            comp_operator=le, additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2),
           DQErrs.DIDS048),
    DQRule(Submitted.IMAGING_CODE_SNOMED,
           [Submitted.IMAGING_CODE_NICIP, Submitted.IMAGING_CODE_SNOMED], True,
           lambda c1, c2: column_not_blank_rule_expr(col(c1)) | column_not_blank_rule_expr(col(c2)), DQErrs.DIDS056),
    DQRule(Submitted.IMAGING_CODE_NICIP, [Submitted.IMAGING_CODE_NICIP, Submitted.DIAGNOSTIC_TEST_DATE], True,
            lambda c1, c2: is_active_with_condition(c1, c2, udfs.ClinicalNicipValid, offset=6), DQErrs.DIDS057),
    DQRule(Submitted.IMAGING_CODE_SNOMED, [Submitted.IMAGING_CODE_SNOMED, Submitted.DIAGNOSTIC_TEST_DATE], True,
           lambda c1, c2: is_active_with_condition(c1, c2, udfs.ClinicalSnomedValid, offset=6), DQErrs.DIDS059),
    DQRule(Submitted.PERSON_GENDER_CODE, [Submitted.PERSON_GENDER_CODE], True,
           lambda c1: (col(c1).isNull()) | (col(c1).isin(['0', '1', '2', '9'])),
           DQErrs.DIDS014),
    DQRule(Submitted.ETHNIC_CATEGORY, [Submitted.ETHNIC_CATEGORY], True,
           lambda c1: (~column_not_blank_rule_expr(col(c1))) |
                      (col(c1).isin([item.value for item in EthnicCategory])), DQErrs.DIDS012),
    DQRule(Submitted.PATIENT_SOURCE_SETTING,
           [Submitted.PATIENT_SOURCE_SETTING], True,
           lambda c1: (~column_not_blank_rule_expr(col(c1))) |
                      (col(c1).isin([item.value for item in PatientSourceSetting])),
           DQErrs.DIDS021),
    DQRule(Submitted.PATIENT_SOURCE_SETTING, [Submitted.PATIENT_SOURCE_SETTING], True,
           lambda c1: column_not_blank_rule_expr(col(c1)),
           DQErrs.DIDS020
           ),
    DQRule(Submitted.PRACTICE_CODE, [Submitted.PRACTICE_CODE], True,
           lambda c1: (~column_not_blank_rule_expr(col(c1))) | (udfs.GPPracticeCodeValid(col(c1))), DQErrs.DIDS018),
    DQRule(Submitted.SITE_CODE_IMAGING, [Submitted.SITE_CODE_IMAGING], True,
           lambda c1: (~column_not_blank_rule_expr(col(c1))) | (~(length_check_an(c1, min_size=5, max_size=12))) |
                      (udfs.SiteCodeValidAnytime(col(c1))), DQErrs.DIDS068),
    DQRule(Submitted.NHS_NUMBER_STATUS_IND, [Submitted.NHS_NUMBER_STATUS_IND], True,
           lambda c1: (~column_not_blank_rule_expr(col(c1))) | (col(c1).isin([item.value for item in NHSNumberStatus])),
           DQErrs.DIDS003),
    DQRule(Submitted.REFERRING_ORG_CODE, [Submitted.REFERRING_ORG_CODE], True,
           lambda c1: (~column_not_blank_rule_expr(col(c1))) | (udfs.ReferringOrgCodeValid(col(c1))), DQErrs.DIDS024),
    DQRule(Submitted.NHS_NUMBER, [Submitted.NHS_NUMBER], True,
           *allow_empty((udfs.NHSNumber, DQErrs.DIDS001))),
    DQRule(
        Submitted.NHS_NUMBER, [
            Submitted.NHS_NUMBER, Submitted.PERSON_BIRTH_DATE, Submitted.ETHNIC_CATEGORY, Submitted.PERSON_GENDER_CODE,
            Submitted.POSTCODE, Submitted.PRACTICE_CODE
        ], True,
        lambda c1, c2, c3, c4, c5, c6: column_not_blank_rule_expr(col(c1)) | column_not_blank_rule_expr(col(c2)) |
            column_not_blank_rule_expr(col(c3)) | column_not_blank_rule_expr(col(c4)) |
            column_not_blank_rule_expr(col(c5)) | column_not_blank_rule_expr(col(c6)), DQErrs.DIDS073
    ),
    DQRule(
        Submitted.NHS_NUMBER, [
            Submitted.NHS_NUMBER, Submitted.PERSON_BIRTH_DATE, Submitted.ETHNIC_CATEGORY, Submitted.PERSON_GENDER_CODE,
            Submitted.POSTCODE, Submitted.PRACTICE_CODE
        ], True,
        lambda c1, c2, c3, c4, c5, c6: column_not_blank_rule_expr(col(c1)) | column_not_blank_rule_expr(col(c2)) |
            column_not_blank_rule_expr(col(c3)) | column_not_blank_rule_expr(col(c4)) | ~col(c5).rlike(r'^[zZ]{2}99') |
            column_not_blank_rule_expr(col(c6)), DQErrs.DIDS074
    ),
    DQRule(Submitted.POSTCODE, [Submitted.POSTCODE], True,
           lambda c1: (col(c1).isNull()) | col(c1).rlike(r'^[zZ]{2}99') |
                      (~column_not_blank_rule_expr(col(c1))) | (udfs.PostcodeInReferenceData(col(c1))), DQErrs.DIDS016),
    DQRule(Submitted.IMAGING_CODE_NICIP, [Submitted.IMAGING_CODE_NICIP, Submitted.IMAGING_CODE_SNOMED, Submitted.DIAGNOSTIC_TEST_DATE], True,
            lambda c1, c2, c3: ~(column_not_blank_rule_expr(col(c1)) & (column_not_blank_rule_expr(col(c2))))
                                | (udfs.ClinicalNicipMapsToSnomed(col(c1), col(c2), col(c3))), DQErrs.DIDS082),

    # DQ warnings
    DQRule(Submitted.REFERRER, [Submitted.REFERRER], False,
           *allow_empty(format_check(RefererCodeValidityRegex.VALIDITY_REGEX, dq_message=DQErrs.DIDS023))),
    DQRule(Submitted.DIAGNOSTIC_TEST_DATE, [Submitted.DIAGNOSTIC_TEST_DATE], False,
           lambda c1: (col(c1).isNull()) | (~col(c1).rlike(YYYY_MM_DD_PATTERN)) |
                      (months_between(last_day(col("META.EVENT_RECEIVED_TS")), c1) < 4) |
                       (months_between(last_day(col("META.EVENT_RECEIVED_TS")), c1) >= 7),
           DQErrs.DIDS049),
    DQRule(Submitted.DIAGNOSTIC_TEST_REQUEST_DATE,
           [Submitted.DIAGNOSTIC_TEST_REQUEST_DATE, Submitted.DIAGNOSTIC_TEST_DATE], False,
           lambda c1, c2: (col(c1).isNull()) | (col(c2).isNull()) | (datediff(col(c2), col(c1)) <= 365),
           DQErrs.DIDS028),
    DQRule(Submitted.DIAG_TEST_REQUEST_REC_DATE,
           [Submitted.DIAGNOSTIC_TEST_DATE, Submitted.DIAG_TEST_REQUEST_REC_DATE], False,
           lambda c1, c2: date_comparison(
               comp_operator=le,
               offset=DateDelta(days=365),
               additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2), DQErrs.DIDS038),
    DQRule(Submitted.SERVICE_REPORT_DATE,
           [Submitted.SERVICE_REPORT_DATE, Submitted.DIAGNOSTIC_TEST_DATE], False,
           lambda c1, c2: date_comparison(
               comp_operator=le,
               offset=DateDelta(months=1),
               additional_date_check_expr=date_comparision_expr_with_pattern)(c1, c2), DQErrs.DIDS062),
    DQRule(Submitted.IMAGING_CODE_NICIP, [Submitted.IMAGING_CODE_NICIP, Submitted.DIAGNOSTIC_TEST_DATE], False,
           lambda c1, c2: is_active_with_condition(c1, c2, udfs.ClinicalInvalidNicipValidWithOffset, offset=6),
           DQErrs.DIDS077),
    DQRule(Submitted.IMAGING_CODE_SNOMED, [Submitted.IMAGING_CODE_SNOMED, Submitted.DIAGNOSTIC_TEST_DATE], False,
           lambda c1, c2: is_active_with_condition(c1, c2, udfs.ClinicalInvalidSnomedValidWithOffset, offset=6),
           DQErrs.DIDS078),
    DQRule(Submitted.REFERRING_ORG_CODE, [Submitted.REFERRING_ORG_CODE, Submitted.DIAGNOSTIC_TEST_REQUEST_DATE,
                                          Submitted.DIAGNOSTIC_TEST_DATE], False,
           lambda c1, c2, c3: ~col(c2).isNull() & is_active_with_condition(c1, c2, udfs.ValidRefOrgActive) |
                col(c2).isNull() & is_active_with_condition(c1, c3, udfs.ValidRefOrgActive), DQErrs.DIDS079),
    DQRule(Submitted.PRACTICE_CODE, [Submitted.PRACTICE_CODE, Submitted.DIAGNOSTIC_TEST_DATE], False,
           lambda c1, c2: is_active_with_condition(c1, c2, udfs.ValidGPPracticeActive), DQErrs.DIDS080),
    DQRule(Submitted.SITE_CODE_IMAGING, [Submitted.SITE_CODE_IMAGING, Submitted.DIAGNOSTIC_TEST_DATE], False,
           lambda c1, c2: is_active_with_condition(c1, c2, udfs.SiteCodeActive), DQErrs.DIDS081),
]

DQ_UNIQUENESS = UniquenessRule([Submitted.RADIOLOGICAL_ACCESSION_NUM, Submitted.SITE_CODE_IMAGING], DQErrs.DIDS071)

DERIVATIONS = [
    (Derived.AGE_AT_EVENT, lambda: udfs.AgeAtEvent(Submitted.PERSON_BIRTH_DATE, Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.CCG_CODE, lambda: udfs.CCGCodeFromGeneralPractice(Submitted.PRACTICE_CODE, Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.COMMISSIONING_ORGANISATION_CODE,
     lambda: udfs.PCTCodeFromGeneralPractice(Submitted.PRACTICE_CODE, Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.PROVIDER_CODE, lambda: udfs.ProviderCodeFromSite(Submitted.SITE_CODE_IMAGING)),
    ('Clinical',
     lambda: udfs.ClinicalStructFromNicipOrSnomedCT(Submitted.IMAGING_CODE_NICIP, Submitted.IMAGING_CODE_SNOMED,
                                                    Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.LOCATION_LSOA, lambda: udfs.PostcodeToLSOA(Submitted.POSTCODE, Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.LOCATION_MSOA, lambda: udfs.PostcodeToMSOA(Submitted.POSTCODE, Submitted.DIAGNOSTIC_TEST_DATE)),

]

RESULT = [
    (CommonFields.META, lambda: col(CommonFields.META)),
    (Output.RADIOLOGICAL_ACCESSION_NUMBER, lambda: col(Submitted.RADIOLOGICAL_ACCESSION_NUM)),
    (Output.NHS_NUMBER, lambda: col(Submitted.NHS_NUMBER)),
    (Output.NHS_NUMBER_STATUS, lambda: col(Submitted.NHS_NUMBER_STATUS_IND)),
    (Output.NHS_NUMBER_STATUS_DESCRIPTION, lambda: udfs.NHSNumberStatusDescription(Submitted.NHS_NUMBER_STATUS_IND)),
    (Output.NHS_NUMBER_SUPPLIED, lambda: when(length(Submitted.NHS_NUMBER) > 0, lit(1)).otherwise(lit(0))),
    (Output.DATE_OF_BIRTH, lambda: col(Submitted.PERSON_BIRTH_DATE)),
    (Output.AGE_BAND_5_YEARS, lambda: udfs.AgeBand5Years(Derived.AGE_AT_EVENT)),
    (Output.AGE_BAND_10_YEARS, lambda: udfs.AgeBand10Years(Derived.AGE_AT_EVENT)),
    (Output.AGE_BAND_CHILDREN_INC_0_YEARS, lambda: udfs.AgeBandChildrenInc0(Derived.AGE_AT_EVENT)),
    (Output.AGE_BAND_INC_50_TO_74, lambda: udfs.AgeBandInc50to74(Derived.AGE_AT_EVENT)),
    (Output.AGE_BAND_INC_WORKING_AGE_16_TO_60, lambda: udfs.AgeBandIncWorkingAge16to60(Derived.AGE_AT_EVENT)),
    (Output.AGE_BAND_INC_WORKING_AGE_16_TO_64, lambda: udfs.AgeBandIncWorkingAge16to64(Derived.AGE_AT_EVENT)),
    (Output.ETHNIC_CATEGORY_CODE, lambda: col(Submitted.ETHNIC_CATEGORY)),
    (Output.ETHNIC_CATEGORY_DESCRIPTION, lambda: udfs.EthnicCategoryDescription(Submitted.ETHNIC_CATEGORY)),
    (Output.ETHNIC_CATEGORY_GROUP, lambda: udfs.EthnicCategoryGroup(Submitted.ETHNIC_CATEGORY)),
    (Output.GENDER_CODE, lambda: col(Submitted.PERSON_GENDER_CODE).cast(ByteType())),
    (Output.GENDER_DESCRIPTION, lambda: udfs.GenderDescription(Submitted.PERSON_GENDER_CODE)),
    (Output.POSTCODE, lambda: col(Submitted.POSTCODE)),
    (Output.LOCATION_LSOA, lambda: col(Output.LOCATION_LSOA)),
    (Output.LOCATION_MSOA, lambda: col(Output.LOCATION_MSOA)),
    (Output.GP_CODE, lambda: col(Submitted.PRACTICE_CODE)),
    (Output.GP_DESCRIPTION, lambda: udfs.GPPracticeName(Submitted.PRACTICE_CODE)),
    (Output.CCG_CODE, lambda: col(Output.CCG_CODE)),
    (Output.CCG_NAME, lambda: udfs.OrganisationName(Output.CCG_CODE)),
    (Output.PATIENT_SOURCE_SETTING, lambda: col(Submitted.PATIENT_SOURCE_SETTING)),
    (Output.PATIENT_SOURCE_SETTING_DESCRIPTION,
     lambda: udfs.PatientSourceSettingDescription(Submitted.PATIENT_SOURCE_SETTING)),
    (Output.REFERRER_CODE, lambda: udfs.RefererCode(Submitted.REFERRER)),
    (Output.DERIVED_REFERRER_TYPE, lambda: udfs.RefererType(Submitted.REFERRER)),
    (Output.REFERRER_ORGANISATION_CODE, lambda: col(Submitted.REFERRING_ORG_CODE)),
    (Output.REFERRER_ORGANISATION_DESCRIPTION, lambda: udfs.OrganisationName(Submitted.REFERRING_ORG_CODE)),
    (Output.DIAGNOSTIC_TEST_REQUEST_DATE, lambda: col(Submitted.DIAGNOSTIC_TEST_REQUEST_DATE)),
    (Output.DIAGNOSTIC_TEST_REQUEST_RECEIVED_DATE, lambda: col(Submitted.DIAG_TEST_REQUEST_REC_DATE)),
    (Output.DIAGNOSTIC_TEST_DATE, lambda: col(Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.MONTH_OF_TEST, lambda: month(Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.YEAR_OF_TEST, lambda: year(Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.SERVICE_REPORT_ISSUE_DATE, lambda: col(Submitted.SERVICE_REPORT_DATE)),
    (Output.TEST_REQUEST_TO_TEST_REQUEST_RECEIVED,
     lambda: datediff(Submitted.DIAG_TEST_REQUEST_REC_DATE, Submitted.DIAGNOSTIC_TEST_REQUEST_DATE)),
    (Output.TEST_REQUEST_TO_TEST,
     lambda: datediff(Submitted.DIAGNOSTIC_TEST_DATE, Submitted.DIAGNOSTIC_TEST_REQUEST_DATE)),
    (Output.TEST_REQUEST_TO_SERVICE_REPORT_ISSUED,
     lambda: datediff(Submitted.SERVICE_REPORT_DATE, Submitted.DIAGNOSTIC_TEST_REQUEST_DATE)),
    (Output.TEST_REQUEST_RECEIVED_TO_TEST,
     lambda: datediff(Submitted.DIAGNOSTIC_TEST_DATE, Submitted.DIAG_TEST_REQUEST_REC_DATE)),
    (Output.TEST_REQUEST_RECEIVED_TO_SERVICE_REPORT_ISSUED,
     lambda: datediff(Submitted.SERVICE_REPORT_DATE, Submitted.DIAG_TEST_REQUEST_REC_DATE)),
    (Output.TEST_TO_SERVICE_REPORT_ISSUED,
     lambda: datediff(Submitted.SERVICE_REPORT_DATE, Submitted.DIAGNOSTIC_TEST_DATE)),
    (Output.IMAGING_CODE_NICIP, lambda: col(Submitted.IMAGING_CODE_NICIP)),
    (Output.IMAGING_CODE_NICIP_DESCRIPTION, lambda: when(~column_not_blank_rule_expr(Submitted.IMAGING_CODE_NICIP),
                                                             lit(None)).otherwise(col('Clinical.NicipDescription'))),

    (Output.IMAGING_CODE_SNOMED_CT, lambda: col(Submitted.IMAGING_CODE_SNOMED)),
    (Output.IMAGING_CODE_SNOMED_CT_DESCRIPTION, lambda: when(column_not_blank_rule_expr(Submitted.IMAGING_CODE_SNOMED),
                                                             udfs.snomed_ct_description(
                                                                 Submitted.IMAGING_CODE_SNOMED,
                                                                 Submitted.DIAGNOSTIC_TEST_DATE)).otherwise(lit(None))),

    (Output.MORPHOLOGY_ID, lambda: col('Clinical.MorphologyId')),
    (Output.MORPHOLOGY, lambda: col('Clinical.Morphology')),
    (Output.MODALITY_ID, lambda: col('Clinical.ModalityId')),
    (Output.MODALITY, lambda: col('Clinical.Modality')),
    (Output.SUB_MODALITY_ID, lambda: col('Clinical.SubModalityId')),
    (Output.SUB_MODALITY, lambda: col('Clinical.SubModality')),
    (Output.REGION_ID, lambda: col('Clinical.RegionId')),
    (Output.REGION, lambda: col('Clinical.Region')),
    (Output.SUB_REGION_ID, lambda: col('Clinical.SubRegionId')),
    (Output.SUB_REGION, lambda: col('Clinical.SubRegion')),
    (Output.SYSTEM_ID, lambda: col('Clinical.SystemId')),
    (Output.SYSTEM, lambda: col('Clinical.System')),
    (Output.SUB_SYSTEM_ID, lambda: col('Clinical.SubSystemId')),
    (Output.SUB_SYSTEM, lambda: col('Clinical.SubSystem')),
    (Output.SUB_SYSTEM_COMPONENT_ID, lambda: col('Clinical.SubSystemComponentId')),
    (Output.SUB_SYSTEM_COMPONENT, lambda: col('Clinical.SubSystemComponent')),
    (Output.FETAL_ID, lambda: col('Clinical.FetalId')),
    (Output.FETAL, lambda: col('Clinical.Fetal')),
    (Output.EARLY_CANCER_DIAGNOSIS, lambda: col('Clinical.EarlyDiagnosisOfCancer')),
    (Output.SUB_EARLY_CANCER_DIAGNOSIS, lambda: col('Clinical.SubEarlyDiagnosisOfCancer')),
    (Output.PROVIDER_SITE_CODE, lambda: col(Submitted.SITE_CODE_IMAGING)),
    (Output.PROVIDER_SITE_CODE_DESCRIPTION, lambda: udfs.SiteCodeDescription(Submitted.SITE_CODE_IMAGING)),
    (Output.PROVIDER_CODE, lambda: col(Output.PROVIDER_CODE)),
    (Output.PROVIDER_NAME, lambda: udfs.OrganisationName(Output.PROVIDER_CODE)),
    (Output.COMMISSIONING_ORGANISATION_CODE, lambda: lit(None).cast(StringType())),
    (Output.COMMISSIONING_ORGANISATION_NAME, lambda: lit(None).cast(StringType())),
    (Output.SUBMITTER_ORGANISATION, lambda: coalesce(col(Output.SUBMITTER_ORGANISATION), lit(None).cast(StringType()))),
    (Output.PROVIDER_SHA_CODE, lambda: lit(None).cast(StringType())),
    (Output.PROVIDER_SHA_NAME, lambda: lit(None).cast(StringType())),
    (Output.DERIVED_IMAGING_CODE_SNOMED_CT, lambda: col('Clinical.SnomedCtId')),
    (Output.DERIVED_IMAGING_CODE_SNOMED_CT_DESCRIPTION,
     lambda: udfs.snomed_ct_description(col('Clinical.SnomedCtId'),col(Submitted.DIAGNOSTIC_TEST_DATE))),
    (Output.DERIVED_REFERRER_CODE, lambda: udfs.DerivedReferrerCode(Submitted.REFERRER)),
    (Output.SUBMITTER_ORGANISATION_NAME, lambda: when(col(Output.SUBMITTER_ORGANISATION).isNotNull(),
     udfs.OrganisationName(Output.SUBMITTER_ORGANISATION)).otherwise(lit(None).cast(StringType()))),
    (Output.MIGRATION_DATE, lambda: lit(None).cast(TimestampType())),
    (Output.Person_ID, lambda: col(Output.Person_ID)),
    (Output.MPSConfidence, lambda: col(Output.MPSConfidence)),
    (Output.SUBMISSION_DATA_ID, lambda: lit(None).cast(LongType()))
]

DATATYPE_LOADERS = {
    FT.CSV: csv_loader,
    FT.XML: loader
}

DELTA_JOIN_COLUMNS = [
    Output.PROVIDER_SITE_CODE,
    Output.RADIOLOGICAL_ACCESSION_NUMBER
]
