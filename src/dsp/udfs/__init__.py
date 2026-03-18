from functools import partial
from uuid import uuid4
from typing import List

from dsp.udfs.age_bands import (
    age_band_5_years,
    age_band_10_years,
    age_band_children_inc0,
    age_band_inc_50_74,
    age_band_inc_working_age_16_60,
    age_band_inc_working_age_16_64,
    age_in_years_at_event,
)
from dsp.udfs.ahas import (
    derive_month_period,
    derive_part_year,
    derive_partyear,
    derive_period_end,
    derive_period_start,
    derive_submission_period,
    derive_year_period,
    format_boolean_to_character,
    format_boolean_to_integer,
    format_null_to_numeric,
    format_null_to_str,
    smart_cast_int,
)
from dsp.udfs.clinical import (
    clinical_struct_from_nicip_or_snomedct,
    is_nicip_valid,
    is_valid_nicip_active_with_offset,
    is_snomed_valid,
    is_valid_snomed_active_with_offset,
    valid_nicip_maps_to_valid_snomed,
)
from dsp.udfs.coded_clinical_entry import (
    is_valid_diag_code,
    is_valid_icd_snomed_diag_code,
    is_valid_find_code,
    is_valid_icd_snomed_find_code,
    is_valid_observation_code,
    is_valid_procedure_and_procedure_status_code,
    is_valid_procedure_code,
    is_valid_situation_code,
)
from dsp.udfs.ethnic_categories import (
    ethnic_category_description,
    ethnic_category_group,
)
from dsp.udfs.gender import gender_code_isvalid, gender_description
from dsp.udfs.lat_long import get_lat_long
from dsp.udfs.misc import (
    count_iapt_care_contact_dna_duplicates,
    datetime_to_iso8601_string,
    is_valid_nhs_number,
    is_valid_snomed_ct,
    nhs_number_status_description,
    nullandemptycheck,
    patient_source_setting_description,
    referer_code,
    referer_type_from_referer_code,
    sorted_json_string,
    timestamp_to_time_from_unix_epoch, derived_referrer_code,
)
from dsp.udfs.specialised_mental_health_code import (
    specialised_mental_health_exists,
)
from dsp.udfs.organisation import (
    ccg_code_from_gp_practice_code,
    gp_practice_name,
    is_practice_code_valid,
    is_site_code_valid,
    is_valid_ccg,
    is_valid_school,
    organisation_exists,
    organisation_exists_anytime_without_successor,
    organisation_name,
    pct_code_from_gp_practice_code,
    provider_code_from_site_code,
    sha_code_from_site_code,
    site_code_description, is_valid_gp_practice_active, is_valid_site_code_active, organisation_exists_anytime,
    is_valid_org_role,
    get_org_role_types,
    get_org_role_types_without_successor
)
from dsp.udfs.postcodes import active_at as postcode_active_at
from dsp.udfs.postcodes import (
    eastings_at,
    lsoa_at,
    msoa_at,
    northings_at,
    pcds_from_postcode_at,
)
from dsp.udfs.referring_org import (is_empty_or_valid_referring_org, is_valid_ref_org_active)
from dsp.udfs.unique_pregnancy_id import (
    get_distinct_upid_groups,
    get_first_item_from_distinct_upid_group,
)
from dsp.udfs.snomed import snomedct_description_from_snomedct_id
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

NullAndEmptyCheck = udf(nullandemptycheck, BooleanType())
SortedJsonString = udf(sorted_json_string, StringType())

AgeAtEvent = udf(age_in_years_at_event, IntegerType())
AgeBand5Years = udf(age_band_5_years, StringType())
AgeBand10Years = udf(age_band_10_years, StringType())
AgeBandChildrenInc0 = udf(age_band_children_inc0, StringType())
AgeBandInc50to74 = udf(age_band_inc_50_74, StringType())
AgeBandIncWorkingAge16to60 = udf(age_band_inc_working_age_16_60, StringType())
AgeBandIncWorkingAge16to64 = udf(age_band_inc_working_age_16_64, StringType())

FindCodeIsValid = udf(is_valid_find_code, BooleanType())
IcdSnomedFindCodeIsValid = udf(is_valid_icd_snomed_find_code, BooleanType())
DiagCodeIsValid = udf(is_valid_diag_code, BooleanType())
IcdSnomedDiagCodeIsValid = udf(is_valid_icd_snomed_diag_code, BooleanType())
ObsCodeIsValid = udf(is_valid_observation_code, BooleanType())
SitCodeIsValid = udf(is_valid_situation_code, BooleanType())
ProPlusStatusCodeIsValid = udf(is_valid_procedure_and_procedure_status_code, BooleanType())
ProCodeIsValid = udf(is_valid_procedure_code, BooleanType())
GetOrgRoles = udf(get_org_role_types, ArrayType(StringType()))
GetOrgRolesWithoutSuccessor = udf(get_org_role_types_without_successor, ArrayType(StringType()))

NHSNumberStatusDescription = udf(nhs_number_status_description, StringType())
NHSNumber = udf(is_valid_nhs_number, BooleanType())

EthnicCategoryDescription = udf(ethnic_category_description, StringType())
EthnicCategoryGroup = udf(ethnic_category_group, StringType())

GenderDescription = udf(gender_description, StringType())
GenderCode = udf(gender_code_isvalid, BooleanType())

PostcodeInReferenceData = udf(postcode_active_at, BooleanType())
PostcodeToLSOA = udf(lsoa_at, StringType())
PostcodeToMSOA = udf(msoa_at, StringType())

GPPracticeName = udf(gp_practice_name, StringType())
GPPracticeCodeValid = udf(is_practice_code_valid, BooleanType())
ValidRefOrgActive = udf(is_valid_ref_org_active, BooleanType())
ValidGPPracticeActive = udf(is_valid_gp_practice_active, BooleanType())

PatientSourceSettingDescription = udf(patient_source_setting_description, StringType())
OrganisationName = udf(organisation_name, StringType())

def default_org_exists(lookup: List[str], follow_successions:bool):
    return udf(
        lambda code, point_in_time: organisation_exists(code, point_in_time, lookup, follow_successions),
        BooleanType()
    )


def default_org_exists_anytime_without_successor(lookup=None):
    return udf(lambda code:
               organisation_exists_anytime_without_successor(code, lookup), BooleanType())


OrganisationExistsAnyTimeWithoutSuccessor = udf(organisation_exists_anytime_without_successor, BooleanType())
OrganisationExists = udf(organisation_exists, BooleanType())
SpecialisedMentalHealthServiceCategoryCodeExists = udf(specialised_mental_health_exists, BooleanType())
OrganisationExistsIgnoringSuccession = udf(partial(organisation_exists, follow_successions=False), BooleanType())
DefaultOrgValid = default_org_exists
DefaultOrgValidAnytimeWithoutSuccessor = default_org_exists_anytime_without_successor

SiteCodeValid = udf(is_site_code_valid, BooleanType())
SiteCodeValidIgnoringSuccession = udf(partial(is_site_code_valid, follow_successions=False), BooleanType())
SiteCodeValidAnytime = udf(organisation_exists_anytime, BooleanType())
SiteCodeActive = udf(is_valid_site_code_active, BooleanType())
SiteCodeDescription = udf(site_code_description, StringType())

SHACodeFromSiteCode = udf(sha_code_from_site_code, StringType())

CCGCodeFromGeneralPractice = udf(ccg_code_from_gp_practice_code, StringType())
CCGCodeValid = udf(is_valid_ccg, BooleanType())

PCTCodeFromGeneralPractice = udf(pct_code_from_gp_practice_code, StringType())

ProviderCodeFromSite = udf(provider_code_from_site_code, StringType())

PCDSFromPostcode = udf(pcds_from_postcode_at)

RefererCode = udf(referer_code, StringType())
RefererType = udf(referer_type_from_referer_code, StringType())
ReferringOrgCodeValid = udf(is_empty_or_valid_referring_org, BooleanType())
DerivedReferrerCode = udf(derived_referrer_code, StringType())

PostcodeToEastings = udf(eastings_at, StringType())

PostcodeToNorthings = udf(northings_at, StringType())

LatLongFromPostcode = udf(get_lat_long, StructType([
    StructField("Latitude", FloatType(), False),
    StructField("Longitude", FloatType(), False)
]))

ClinicalNicipValid = udf(is_nicip_valid, BooleanType())
ClinicalInvalidNicipValidWithOffset = udf(is_valid_nicip_active_with_offset, BooleanType())
ClinicalSnomedValid = udf(is_snomed_valid, BooleanType())
ClinicalInvalidSnomedValidWithOffset = udf(is_valid_snomed_active_with_offset, BooleanType())
ClinicalNicipMapsToSnomed = udf(valid_nicip_maps_to_valid_snomed, BooleanType())
ClinicalStructFromNicipOrSnomedCT = udf(clinical_struct_from_nicip_or_snomedct, StructType([
    StructField('NicipId', StringType(), True),
    StructField('NicipDescription', StringType(), True),
    StructField('SnomedCtId', StringType(), True),
    StructField('SnomedCtDescription', StringType(), True),
    StructField('ModalityId', StringType(), True),
    StructField('Modality', StringType(), True),
    StructField('SubModalityId', StringType(), True),
    StructField('SubModality', StringType(), True),
    StructField('RegionId', StringType(), True),
    StructField('Region', StringType(), True),
    StructField('SubRegionId', StringType(), True),
    StructField('SubRegion', StringType(), True),
    StructField('SystemId', StringType(), True),
    StructField('System', StringType(), True),
    StructField('SubSystemId', StringType(), True),
    StructField('SubSystem', StringType(), True),
    StructField('SubSystemComponentId', StringType(), True),
    StructField('SubSystemComponent', StringType(), True),
    StructField('MorphologyId', StringType(), True),
    StructField('Morphology', StringType(), True),
    StructField('FetalId', StringType(), True),
    StructField('Fetal', StringType(), True),
    StructField('EarlyDiagnosisOfCancer', StringType(), True),
    StructField('SubEarlyDiagnosisOfCancer', StringType(), True),
]))

IsValidSnomedCT = udf(is_valid_snomed_ct, BooleanType())

SchoolValid = udf(is_valid_school, BooleanType())

TimestampToTimeFromUnixEpoch = udf(timestamp_to_time_from_unix_epoch, TimestampType())
DatetimeToIso8601String = udf(datetime_to_iso8601_string, StringType())

UUID = udf(lambda: uuid4().hex, StringType())
GetDistinctUpidGroups = udf(get_distinct_upid_groups, ArrayType(ArrayType(StringType())))
GetFirstItemFromDistinctUpidGroup = udf(get_first_item_from_distinct_upid_group, StringType())

GetCountOfCareContactDnaDuplicates = udf(count_iapt_care_contact_dna_duplicates, IntegerType())

AHASFormatBooleanToInteger = udf(format_boolean_to_integer, IntegerType())
AHASFormatBooleanToCharacter = udf(format_boolean_to_character, StringType())
AHASDeriveMonthPeriod = udf(derive_month_period, IntegerType())
AHASDerivePartYear = udf(derive_part_year, StringType())
AHASDerivePartYearV2 = udf(derive_partyear, StringType())
AHASDeriveYearPeriod = udf(derive_year_period, StringType())
AHASFormatNullToNumeric = udf(format_null_to_numeric, IntegerType())
AHASFormatNullToString = udf(format_null_to_str, StringType())
AHASSmartCastInt = udf(smart_cast_int, LongType())
AHASDeriveSubmissionPeriod = udf(derive_submission_period, IntegerType())
AHASDerivePeriodStart = udf(derive_period_start, StringType())
AHASDerivePeriodEnd = udf(derive_period_end, StringType())
snomed_ct_description = udf(snomedct_description_from_snomedct_id, StringType())
