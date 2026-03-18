from datetime import datetime
import json
import re
from collections import OrderedDict, Counter
from typing import Any, Mapping, Optional, Dict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, MapType

from dsp.common import enum, verhoeff
from dsp.model.pds_record import PDSRecord
from dsp.udfs.organisation import postcode_from_org_code, ccg_code_from_gp_practice_code,  icb_code_from_sub_icb_code
from dsp.udfs.postcodes import ccg_at, unitary_authority_at, lsoa_at, icb_at


class NHSNumberStatus(enum.LabelledEnum):
    PRESENT_VERIFIED = '01'
    PRESENT_NOT_TRACED = '02'
    TRACE_REQUIRED = '03'
    TRACE_ATTEMPTED = '04'
    TRACE_TO_BE_RESOLVED = '05'
    TRACE_IN_PROGRESS = '06'
    NUMBER_NOT_PRESENT = '07'
    TRACE_POSTPONED = '08'

    __labels__ = {
        PRESENT_VERIFIED: 'Number present and verified',
        PRESENT_NOT_TRACED: 'Number present but not traced',
        TRACE_REQUIRED: 'Trace required',
        TRACE_ATTEMPTED: 'Trace attempted - No match or multiple match found',
        TRACE_TO_BE_RESOLVED: 'Trace needs to be resolved (NHS Number or patient detail conflict)',
        TRACE_IN_PROGRESS: 'Trace in progress',
        NUMBER_NOT_PRESENT: 'Number not present and trace not required',
        TRACE_POSTPONED: 'Trace postponed (baby under six weeks old)',
    }


_NHS_CHECK_DIGIT_WEIGHTINGS = [10, 9, 8, 7, 6, 5, 4, 3, 2]


def _calculate_check_digit(nhs_num: str) -> Optional[int]:
    digits = map(int, nhs_num[:9])

    check_digit_sum = sum(x * y for x, y in zip(digits, _NHS_CHECK_DIGIT_WEIGHTINGS))

    check_digit = 11 - (check_digit_sum % 11)

    if check_digit == 11:
        return 0
    elif check_digit == 10:
        return None  # invalid check digit
    else:
        return check_digit


def is_valid_nhs_number_old(nhs_num: str, palindrome_check=False) -> bool:
    if not nhs_num:
        return False

    # nhs_num = nhs_num.replace(' ', '').replace('-', '').strip()

    if len(nhs_num) != 10 or not nhs_num.isdigit():
        return False

    # palindrome check - disabled by default as per https://nhsd-jira.digital.nhs.uk/browse/DSP-1888
    if palindrome_check and nhs_num[:5] == ''.join([x for x in reversed(nhs_num[-5:])]):
        return False

    return int(nhs_num[-1]) == _calculate_check_digit(nhs_num)


def is_valid_nhs_number(nhs_num: str) -> bool:
    if not nhs_num:
        return False

    # nhs_num = nhs_num.replace(' ', '').replace('-', '').strip()

    if len(nhs_num) != 10 or not nhs_num.isdigit():
        return False

    # palindrome check - disabled by default as per https://nhsd-jira.digital.nhs.uk/browse/DSP-1888
    if nhs_num[:5] == ''.join([x for x in reversed(nhs_num[-5:])]):
        return False

    return int(nhs_num[-1]) == _calculate_check_digit(nhs_num)



def nhs_number_status_description(status):
    if not status:
        return None
    try:
        return NHSNumberStatus(status).label
    except ValueError:
        return None


def nullandemptycheck(field_value):
    if field_value is None:
        return False

    val = (str(field_value) or '').strip()
    if not val:
        return False

    return True


def sorted_json_string(data: Mapping[Any, Any]) -> str:
    """Convert a dict to a sorted json string. Useful for comparing two MapTypes.

    Args:
        data: the dictionary

    Returns:
        a string representation of the sorted dict
    """

    def to_ordered_list(ol: list):
        res = []
        for v in ol:
            if isinstance(v, Mapping):
                res.append(to_ordered_dict(v))
            elif isinstance(v, list):
                res.append(to_ordered_list(v))
            else:
                res.append(v)
        return res

    def to_ordered_dict(od):
        res = OrderedDict()
        for k, v in sorted(od.items()):
            if isinstance(v, Mapping):
                res[k] = to_ordered_dict(v)
            elif isinstance(v, list):
                res[k] = to_ordered_list(v)
            else:
                res[k] = v
        return res

    return json.dumps(to_ordered_dict(data))


class PatientSourceSetting(enum.LabelledEnum):
    INPATIENT = '01'
    DAY_CARE = '02'
    OUTPATIENT = '03'
    GP = '04'
    AE = '05'
    OTHER_HCP = '06'
    OTHER = '07'

    __labels__ = {
        INPATIENT: 'Admitted Patient Care - Inpatient (this Health Care Provider)',
        DAY_CARE: 'Admitted Patient Care - Day case (this Health Care Provider)',
        OUTPATIENT: 'Out-patient (this Health Care Provider)',
        GP: 'GP Direct Access',
        AE: 'Accident and Emergency Department (this Health Care Provider)',
        OTHER_HCP: 'Other Health Care Provider',
        OTHER: 'Other'
    }


def patient_source_setting_description(pss_code):
    if not pss_code:
        return None
    try:
        return PatientSourceSetting(pss_code).label
    except ValueError:
        pass


class RefererCodeValidityRegex:
    VALIDITY_REGEX = r"(?i)^\s*(?:C|D|G|S)\d{1,7}\s*$|^\s*(?:CH|DT|CD|PH|SL)\d{1,6}\s*$|" \
        r"^\s*\d{2}[A-Z]\d{4}[A-Z]\s*$|^\s*Z(?:E|N|S|W)\d{6}\s*$|^\s*$|" \
        r"^\s*H9999998\s*$|^\s*M9999998\s*$|^\s*R9999981\s*$|^\s*A9999998\s*$|" \
        r"^\s*P9999981\s*$|^\s*X9999998\s*$|^\s*N9999998\s*$"


DEFAULT_REFERER_CODE = '99'


def referer_code(code: str) -> str:
    """
    If the referer code consists of two letters followed by six integers, or one letter followed by seven numbers,
    return the original code. If not, return '99'. This replicates the behaviour of the legacy system.

    There is no need to strip whitespace from the referer code; this has already been done during the cleansing
    stage of the pipeline.

    Args:
        code: referrer code submitted by providers
    Returns:
        referer_code: The original code if it meets the specification, or '99' if not.
    """
    if not code:
        return None

    if re.compile(RefererCodeValidityRegex.VALIDITY_REGEX).match(code):
        return code
    else:
        return DEFAULT_REFERER_CODE


def derived_referrer_code(code: str) -> str:
    """
    If the referer code field is empty or invalid, then the derived referrer code will be set to the default referrer
    code.

    If a valid referrer code is submitted, the derived referrer code would be set to the referrer code.

    Args:
        code: referrer code submitted by providers
    Returns:
        referer_code: The original code if it meets the specification, or '99' if not.
    """
    if not code:
        return DEFAULT_REFERER_CODE

    if re.compile(RefererCodeValidityRegex.VALIDITY_REGEX).match(code):
        return code

    return DEFAULT_REFERER_CODE


# pylint: disable=C0301
class ReferrerCodeRegex(enum.LabelledEnum):
    """
        reg ex for referrer code checking length is 8 chars if not then it is not known code
    """
    CONSULTANT = re.compile(r'^[Cc]\d{1,7}$')  # matches first letter 'C'/'c' and upto 7 digits
    DENTAL_CONSULTANT = re.compile(r'^[Cc][Dd]\d{1,6}$')  # CD/cd followed by up to 6 digits (dental consultant)
    DENTIST = re.compile(r'^[Dd]\d{1,7}$')  # matches first letters as 'D' then upto 7 digits
    CHIROPODIST = re.compile(r'^[Cc][Hh]\d{1,6}$')  # matches first letters 'CH' and upto 6 digits
    DIETICIAN = re.compile(r'^[Dd][Tt]\d{1,6}$')  # matches first letters 'DT' and upto 6 digits
    GP = re.compile(r'^[Gg]\d{1,7}$')  # matches first letter 'G' and upto 7 digits
    GP_N_Ireland = re.compile(r'^[Zz][EeNnSsWw]\d{1,6}$')  # Z as 1st character, E,N,S or W as 2nd character,
    # followed by up to 6 digits (GP N.Ireland)
    GP_SCOTLAND=re.compile(r'^[Ss]\d{1,7}$')  # S followed by up to 7 digits (GP Scotland)
    NURSE = re.compile(r'^\d{2}[A-Za-z]\d{4}[A-Za-z]$|^[Nn]9999998$')  # default code for nurse as per data dictionary
    MINISTRY_DOCTOR = re.compile(r'^[Aa]9999998$')  # default code for ministry doctor
    PRISON_DOCTOR = re.compile(r'^[Pp]9999981$')  # default code for prison doctor
    PHYSIO = re.compile(r'^[Pp][Hh]\d{1,6}$')  # matches first letters 'PH' and upto 6 digits
    SPEECH_AND_LANGUAGE = re.compile(r'^[Ss][Ll]\d{1,6}$')  # matches first letters 'SL' and upto 6 digits
    NOT_APPLICABLE = re.compile(r'^[Xx]9999998$')  # default code for not known as per data dictionary
    MIDWIFE = re.compile(r'^[Mm]9999998$')
    OTHER = re.compile(r'^[Hh]9999998$|^[Rr]9999981$')  # default codes as per data dictionary

    __labels__ = {
        CHIROPODIST: 'Chiropodist',
        CONSULTANT: 'Consultant',
        DENTIST: 'Dentist',
        DENTAL_CONSULTANT: 'Dental Consultant',
        DIETICIAN: 'Dietician',
        GP: 'GP',
        GP_N_Ireland: 'GP N.Ireland',
        GP_SCOTLAND: 'GP Scotland',
        NURSE: 'Nurse',
        MIDWIFE: 'Midwife',
        MINISTRY_DOCTOR: 'Ministry of Defence Doctor',
        PHYSIO: 'Physiotherapist',
        PRISON_DOCTOR: 'Prison doctor',
        OTHER: 'Other health professionals',
        SPEECH_AND_LANGUAGE: 'Speech and Language Therapist',
        NOT_APPLICABLE: 'Not Known'
    }


def referer_type_from_referer_code(code: str) -> str:
    """
        validates referrer code against reg ex, if not found then Not Known is returned
    Args:
        code: referrer code submitted by providers

    Returns: if matches then respective label is returned else 'Not Known'

    """
    if code is None:
        return 'Not Known'

    for regex in ReferrerCodeRegex:
        if not regex.value.match(code):
            continue
        return regex.label

    return 'Not Known'

def timestamp_to_time_from_unix_epoch(dt: Optional[datetime]) -> Optional[datetime]:
    """
        Replaces date component of datetime with unix epoch

    Args:
        dt: a datetime value

    Returns: datetime value where the date component is replaced by unix timestamp

    """
    if not dt:
        return None

    return datetime.combine(datetime.fromtimestamp(0), dt.time())


def datetime_to_iso8601_string(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None

    return dt.strftime('%Y-%m-%dT%H:%M:%S')


def is_valid_snomed_ct(snomed_ct: str, min_length: int, max_length: int) -> bool:
    """
    Validates both snomed ct and post coordinated snomed ct value
        Example snomed ct value = "718431003"
        Example post coordinated snomed ct value = "961031000000108:408730004=718431003"

    Post coordinated snomed ct values combine 3 parts:
        * [concept identifier] = must be a SNOMED CT code
        * [attribute]= must be either "408730004", or "363589002"
        * [value] = must be a SNOMED CT code

    See technical glossary in IAPT V2.0 / MSDS V2.0 TOS for further information

    Validate snomed_ct against regex to determine if snomed_ct matches pattern for post coordinated snomed ct value

        If not found then perform simple snomed ct check:

        * The value is a numeric string between min_length and max_length digits in length
        * The last digit of the value is a valid Verhoeff checksum
        * The penultimate two digits of the value are either "00" or "10"

        If found perform post coordinated snomed ct check:

        * The concept identifier passes simple snomed ct check
        * The attribute is either '408730004' or '363589002'
        * The value passes simple snomed ct check
        * snomed_ct is a string between min_length and max_length digits in length

        Else if snomed_ct is None return False

    Args:
        snomed_ct: The value to be validated
        min_length: The min length of the code
        max_length: The max length of the code

    Returns:
        True: If value is a valid snomed ct or valid post coordinated snomed ct code
    """

    min_snomed_length = 6
    max_snomed_length = 18

    def is_valid_simple_snomed(simple_snomed: str) -> bool:
        return (simple_snomed is not None
                and simple_snomed.isdigit()
                and min_snomed_length <= len(simple_snomed) <= max_snomed_length
                and min_length <= len(simple_snomed) <= max_length
                and verhoeff.verify(simple_snomed)
                and (simple_snomed[-3:-1] in ("00", "10")))

    def is_valid_attribute(attrib: str) -> bool:
        return attrib in ('408730004', '363589002')

    if not snomed_ct:
        return False

    match = re.search(r'^(?P<concept_id>\d+):(?P<attribute>\d+)=(?P<value>\d+)$', snomed_ct)
    if match:
        concept_id = match.group('concept_id')
        attribute = match.group('attribute')
        value = match.group('value')

        return (min_length <= len(snomed_ct) <= max_length
                and is_valid_simple_snomed(concept_id)
                and is_valid_attribute(attribute)
                and is_valid_simple_snomed(value))
    else:
        return is_valid_simple_snomed(snomed_ct)


def count_iapt_care_contact_dna_duplicates(code_list: list):
    """
        Counts the number of duplicate IDS201CareContact AttendOrDNACode entries.
        Only multiples, or combinations of, 5 and 6 are classed as duplicates.
        Other repeating integers are NOT classed as duplicates.

    Args:
        code_list: a collected list of AttendOrDNACode values from a partitioned data set.

    Returns: a count of duplicated AttendOrDNACodes of 5 and 6, or a count 1 if no duplicates are found.

    """
    count = 0
    c = Counter(code_list)
    list_counts = dict(c)
    for key, value in list_counts.items():
        if key in ["5", "6"]:
            count += value

    return count


EXPANDED_PDS_RECORD_COLUMNS = {
    "PDS_NHS_NUMBER": StringType(),
    "PDS_POSTCODE": StringType(),
    "PDS_ADDRESS": ArrayType(StringType()),
    "PDS_GPCODE": StringType(),
    "PDS_DATE_OF_BIRTH": IntegerType(),
    "PDS_DEATH_STATUS": StringType(),
    "PDS_DATE_OF_DEATH": IntegerType(),
    "PDS_GENDER": StringType(),
    "PDS_FAMILY_NAME": StringType(),
    "PDS_GIVEN_NAMES": ArrayType(StringType()),
    "PDS_VISA_STATUS": StringType(),
    "PDS_INVALID": BooleanType(),
    "PDS_SENSITIVE": BooleanType(),
    "PDS_CONFIDENTIALITY": MapType(StringType(), StringType()),
}

EXPANDED_PDS_RECORD_SCHEMA = StructType(
    [StructField(col_name, col_type, True) for col_name, col_type in EXPANDED_PDS_RECORD_COLUMNS.items()]
)


def expand_pds_record_at(pds_record: str, event_datetime: datetime) -> Dict[str, Optional[str]]:
    """
        The pds.pds table has a column called "record" which is a string type but has structured data within it.
        Various functions exists (i.e. under PDSRecord) to retrieve this data. This function calls the relevant
        methods to retrieve the postcode, date of birth (dob), gender, and gp practice code (gpcode) from this record.
        This function can be turned into a spark udf and used to pull the relevant information directly into a
        structured column. A schema for this structured column is defined above and should be used for standardisation.
    Args:
        pds_record: str containing the pds.pds record value
        event_datetime: Point in time where the returned data should be valid at

    Returns:
        NHS_NUMBER, POSTCODE, ADDRESS, GPCODE, DATE_OF_BIRTH, DEATH_STATUS, DATE_OF_DEATH, GENDER, FAMILY_NAME,
        GIVEN_NAMES, VISA_STATUS, INVALID, SENSITIVE, CONFIDENTIALITY
    """
    if not pds_record:
        return {col_name: None for col_name in EXPANDED_PDS_RECORD_COLUMNS.keys()}

    pds_rec = PDSRecord.from_json(pds_record)

    expanded_pds_output = {
        "PDS_NHS_NUMBER": pds_rec.nhs_number(),
        "PDS_POSTCODE": pds_rec.postcode_at(event_datetime),
        "PDS_ADDRESS": pds_rec.address_at(event_datetime),
        "PDS_GPCODE": pds_rec.gp_code_at(event_datetime),
        "PDS_DATE_OF_BIRTH": pds_rec.date_of_birth(),
        "PDS_DEATH_STATUS": pds_rec.death_status(),
        "PDS_DATE_OF_DEATH": pds_rec.date_of_death(),
        "PDS_GENDER": pds_rec.gender_at(event_datetime),
        "PDS_FAMILY_NAME": pds_rec.family_name_at(event_datetime),
        "PDS_GIVEN_NAMES": pds_rec.given_names_at(event_datetime),
        "PDS_VISA_STATUS": pds_rec.visa_status_at(event_datetime),
        "PDS_INVALID": pds_rec.invalid_at(event_datetime),
        "PDS_SENSITIVE": pds_rec.sensitive_at(event_datetime),
        "PDS_CONFIDENTIALITY": pds_rec.confidentiality_at(event_datetime),
    }

    return expanded_pds_output


RESIDENCE_AND_REGISTRATION_COLUMNS = [
    "CCG_OF_RESIDENCE",
    "ICS_OF_RESIDENCE",
    "LA_OF_RESIDENCE",
    "LSOA_OF_RESIDENCE",
    "CCG_OF_REGISTRATION",
    "ICS_OF_REGISTRATION",
    "LA_OF_REGISTRATION",
    "LSOA_OF_REGISTRATION",
]
RESIDENCE_AND_REGISTRATION_CODES_SCHEMA = StructType([StructField(col, StringType(), True) for col in RESIDENCE_AND_REGISTRATION_COLUMNS])


def geography_codes_of_residence_and_registration(postcode_of_residence: str, registration_code: str, event_datetime: datetime) -> Dict[str, Optional[str]]:
    """
        Retrieve the various possible geographical codes (ccg, ics, la, lsoa) for a residence (patient) and
        registration (the patient's gp practice) at a specific point in time (i.e. a specific date in the past).
        This function can be turned into a spark udf and used to pull the relevant information directly into a
        structured column. A schema for this structured column is defined above and should be used for standardisation.
    Args:
        postcode_of_residence: i.e. The Patient's postcode
        registration_code: e.g. The patient's GP practice code
        event_datetime: The time at which these postcodes and registration codes were correct for that patient

    Returns:
        Residence CCG, ICS, LA, LSOA, Registration CCG, ICS, LA, LSOA in a dict

    Examples:
        from pyspark.sql.functions import udf
        geography_codes_udf = udf(geography_codes_of_residence_and_registration, RESIDENCE_AND_REGISTRATION_CODES_SCHEMA)
        df.withColumn("COMBINED_GEOGRAPHY_DATA", geography_codes_udf("PATIENT.POSTCODE", "PATIENT.GPCODE", "CREATEDDATETIME"))
    """
    postcode_of_registration = postcode_from_org_code(registration_code, event_datetime)

    ccg_code_from_gp_code = ccg_code_from_gp_practice_code(registration_code, event_datetime)
    # ccg and sub icb are equivalent
    icb_code_from_sub_icb = icb_code_from_sub_icb_code(ccg_code_from_gp_code, event_datetime)

    geography_output = {
        "CCG_OF_RESIDENCE": ccg_at(postcode_of_residence, event_datetime),
        "ICS_OF_RESIDENCE": icb_at(postcode_of_residence, event_datetime),
        "LA_OF_RESIDENCE": unitary_authority_at(postcode_of_residence, event_datetime),
        "LSOA_OF_RESIDENCE": lsoa_at(postcode_of_residence, event_datetime),
        "CCG_OF_REGISTRATION": ccg_code_from_gp_code if ccg_code_from_gp_code else ccg_at(postcode_of_registration, event_datetime),
        "ICS_OF_REGISTRATION": icb_code_from_sub_icb if icb_code_from_sub_icb else icb_at(postcode_of_registration, event_datetime),
        "LA_OF_REGISTRATION": unitary_authority_at(postcode_of_registration, event_datetime),
        "LSOA_OF_REGISTRATION": lsoa_at(postcode_of_registration, event_datetime),
    }

    return geography_output


def suppress_value(valuein: int, rc: str = "*", upper: int = 100000000) -> str:
    '''
   This function is lifted from this source: https://github.com/NHSDigital/codonPython/blob/master/codonPython/suppression.py

   The function is copied as faithfully as possible, with a minor change in the code. The minor code change is this additional line:

    if isinstance(rc, int):
        raise ValueError("The replacement character: {} is not a string.".format(rc))

   This is an addition that is implemented to raise a ValueError when the user imports suppress_value and defines the rc argument as an integer, like 0...
   ..instead of "0" as a string.


   Here is the original doc string:

   Suppress values less than or equal to 7, round all non-national values.

    This function suppresses value if it is less than or equal to 7.
    If value is 0 then it will remain as 0.
    If value is at national level it will remain unsuppressed.
    All other values will be rounded to the nearest 5.

    Parameters
    ----------
    valuein : int
        Metric value
    rc : str
        Replacement character if value needs suppressing
    upper : int
        Upper limit for suppression of numbers

    Returns
    -------
    out : str
        Suppressed value (*), 0 or valuein if greater than 7 or national

    Examples
    --------
    >>> suppress_value(3)
    '*'
    >>> suppress_value(24)
    '25'
    >>> suppress_value(0)
    '0'
     '''

    base = 5

    if not isinstance(valuein, int):
        raise ValueError("The input: {} is not an integer.".format(valuein))
    if isinstance(rc, int):
        raise ValueError("The replacement character: {} is not a string.".format(
            rc))  # additional condition added to the original code
    if valuein < 0:
        raise ValueError("The input: {} is less than 0.".format(valuein))
    elif valuein == 0:
        valueout = str(valuein)
    elif valuein >= 1 and valuein <= 7:
        valueout = rc
    elif valuein > 7 and valuein <= upper:
        valueout = str(base * round(valuein / base))
    else:
        raise ValueError("The input: {} is greater than: {}.".format(valuein, upper))
    return valueout

