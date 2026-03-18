from datetime import datetime, date
from typing import List

from dsp.datasets.digitrials.common.constants import TOTAL_VALIDATION_FAILED_RECORDS, INSUFFICIENT_DATA, \
    MAX_DATE_EARLIER_THAN_MIN_DATE, INCORRECT_STATUS_COLUMN, HEADER_SUPPLIED_MISMATCH, INCORRECT_GENDER, \
    FIELD_IS_DUPLICATE, NHS_NO_LENGTH, EXCEEDS_CHARACTER_LIMIT, INCORRECT_DATE_FORMAT, INVALID_UNIQUE_REFERENCE, \
    FIELD_IS_EMPTY
from dsp.common.structured_model import (
    _MPSConfidenceScores,
    AssignableAttribute,
    DSPStructuredModel,
    MPSConfidenceScores,
    SubmittedAttribute,
    BaseTableModel,
    DerivedAttribute,
)


class _DigitrialsDcmMPSModel(DSPStructuredModel):
    ROW_NUMBER = SubmittedAttribute('ROW_NUMBER', str)
    UNIQUE_REFERENCE = SubmittedAttribute('UNIQUE_REFERENCE', str)
    SUBMITTED_UNIQUE_REFERENCE = SubmittedAttribute('SUBMITTED_UNIQUE_REFERENCE', str)
    FAMILY_NAME = SubmittedAttribute('FAMILY_NAME', str)
    GIVEN_NAME = SubmittedAttribute('GIVEN_NAME', str)
    OTHER_GIVEN_NAME = SubmittedAttribute('OTHER_GIVEN_NAME', str)
    GENDER = SubmittedAttribute('GENDER', str)
    DATE_OF_BIRTH = SubmittedAttribute('DATE_OF_BIRTH', str)
    POSTCODE = SubmittedAttribute('POSTCODE', str)
    ADDRESS_LINE1 = SubmittedAttribute('ADDRESS_LINE1', str)
    ADDRESS_LINE2 = SubmittedAttribute('ADDRESS_LINE2', str)
    ADDRESS_LINE3 = SubmittedAttribute('ADDRESS_LINE3', str)
    ADDRESS_LINE4 = SubmittedAttribute('ADDRESS_LINE4', str)
    ADDRESS_LINE5 = SubmittedAttribute('ADDRESS_LINE5', str)
    NIC_NUMBER = SubmittedAttribute('NIC_NUMBER', str)
    STATUS = SubmittedAttribute('STATUS', str)
    RETRACE = SubmittedAttribute('RETRACE', str)
    PERSON_MAX_DATE = SubmittedAttribute('PERSON_MAX_DATE', str)
    PERSON_MIN_DATE = SubmittedAttribute('PERSON_MIN_DATE', str)

    Person_ID = AssignableAttribute('Person_ID', str)  # type: AssignableAttribute
    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)  # type: _MPSConfidenceScores
    SENSITIVTY_FLAG = AssignableAttribute('SENSITIVTY_FLAG', str)
    MATCHED_ALGORITHM_INDICATOR = AssignableAttribute('MATCHED_ALGORITHM_INDICATOR', str)
    MATCHED_CONFIDENCE_PERCENTAGE = AssignableAttribute('MATCHED_CONFIDENCE_PERCENTAGE', str)


class DigitrialsDcmMPSModel(_DigitrialsDcmMPSModel):
    __concrete__ = True
    __table__ = 'digitrials_dcm_mps'


class DigitrialsDcmProcessedCohortModel(BaseTableModel):
    __table__ = "dcm_processed_cohort"
    __concrete__ = True

    NIC_NUMBER = SubmittedAttribute('NIC_NUMBER', str)
    UNIQUE_REFERENCE = SubmittedAttribute('UNIQUE_REFERENCE', str)
    PERSON_ID = SubmittedAttribute('PERSON_ID', str)
    RECORD_START_DATE = SubmittedAttribute('RECORD_START_DATE', datetime)
    RECORD_END_DATE = SubmittedAttribute('RECORD_END_DATE', datetime)
    IS_CURRENT = SubmittedAttribute('IS_CURRENT', bool)
    FAMILY_NAME = SubmittedAttribute('FAMILY_NAME', str)
    GIVEN_NAME = SubmittedAttribute('GIVEN_NAME', str)
    OTHER_GIVEN_NAME = SubmittedAttribute('OTHER_GIVEN_NAME', str)
    GENDER = SubmittedAttribute('GENDER', str)
    DATE_OF_BIRTH = SubmittedAttribute('DATE_OF_BIRTH', str)
    POSTCODE = SubmittedAttribute('POSTCODE', str)
    ADDRESS_LINE1 = SubmittedAttribute('ADDRESS_LINE1', str)
    ADDRESS_LINE2 = SubmittedAttribute('ADDRESS_LINE2', str)
    ADDRESS_LINE3 = SubmittedAttribute('ADDRESS_LINE3', str)
    ADDRESS_LINE4 = SubmittedAttribute('ADDRESS_LINE4', str)
    ADDRESS_LINE5 = SubmittedAttribute('ADDRESS_LINE5', str)
    PERSON_MAX_DATE = SubmittedAttribute('PERSON_MAX_DATE', str)
    PERSON_MIN_DATE = SubmittedAttribute('PERSON_MIN_DATE', str)


class DigitrialsDcmLegacyCoveCohortsModel(BaseTableModel):
    __table__ = 'legacy_cove_cohorts'
    __concrete__ = True

    NIC_NUMBER = SubmittedAttribute('NIC_NUMBER', str)
    STATUS = SubmittedAttribute('STATUS', str)
    LOAD_TIME = SubmittedAttribute('LOAD_TIME', datetime)


class DigitrialsOutExtractInfoModel(BaseTableModel):
    __table__ = "extract_info"
    __concrete__ = True

    NIC_NUMBER = SubmittedAttribute('NIC_NUMBER', str)
    FILE_INSTRUCTION_ID = SubmittedAttribute('FILE_INSTRUCTION_ID', str)
    CPS_PRODUCTIONSTAGE = SubmittedAttribute('CPS_PRODUCTIONSTAGE', int)
    LAST_UPDATED_DATETIME = SubmittedAttribute('LAST_UPDATED_DATETIME', datetime)
    CPS_PROCESSED = SubmittedAttribute('CPS_PROCESSED', bool)


class ValidationFailureRecords(DSPStructuredModel):
    __table__ = "validation_failure_records"
    __concrete__ = True

    total = DerivedAttribute('total', int, TOTAL_VALIDATION_FAILED_RECORDS)
    insufficient_information = DerivedAttribute('insufficient_information', int, INSUFFICIENT_DATA)
    invalid_nhs_number = DerivedAttribute('invalid_nhs_number', int, NHS_NO_LENGTH)
    max_date_earlier_than_min_date = DerivedAttribute(
        'max_date_earlier_than_min_date', int, MAX_DATE_EARLIER_THAN_MIN_DATE)
    invalid_status_column = DerivedAttribute('invalid_status_column', int, INCORRECT_STATUS_COLUMN)
    unique_reference_duplicate = DerivedAttribute(
        'unique_reference_duplicate', int, f"UNIQUE_REFERENCE {FIELD_IS_DUPLICATE}")
    incorrect_headers = DerivedAttribute('incorrect_headers', int, HEADER_SUPPLIED_MISMATCH)
    incorrect_value_for_gender = DerivedAttribute('incorrect_value_for_gender', int, INCORRECT_GENDER)
    incorrect_format_for_date_of_birth = DerivedAttribute(
        'incorrect_format_for_date_of_birth', int, f"{INCORRECT_DATE_FORMAT} DATE_OF_BIRTH. Format should be yyyy/mm/dd")
    incorrect_format_for_person_max_date = DerivedAttribute(
        'incorrect_format_for_person_max_date', int, f"{INCORRECT_DATE_FORMAT} PERSON_MAX_DATE. Format should be yyyy/mm/dd")
    incorrect_format_for_person_min_date = DerivedAttribute(
        'incorrect_format_for_person_min_date', int, f"{INCORRECT_DATE_FORMAT} PERSON_MIN_DATE. Format should be yyyy/mm/dd")
    family_name_exceeds_limit = DerivedAttribute(
        'family_name_exceeds_limit', int, f"Family Name {EXCEEDS_CHARACTER_LIMIT} 40")
    given_name_exceeds_limit = DerivedAttribute(
        'given_name_exceeds_limit', int, f"Given Name {EXCEEDS_CHARACTER_LIMIT} 40")
    other_given_name_exceeds_limit = DerivedAttribute(
        'other_given_name_exceeds_limit', int, f"Other Given Name {EXCEEDS_CHARACTER_LIMIT} 100")
    postcode_exceeds_limit = DerivedAttribute('postcode_exceeds_limit', int, f"Postcode {EXCEEDS_CHARACTER_LIMIT} 8")
    null_unique_reference = DerivedAttribute('null_unique_reference', int, f"UNIQUE_REFERENCE {FIELD_IS_EMPTY}")
    null_status = DerivedAttribute('null_status', int, f"STATUS {FIELD_IS_EMPTY}")
    invalid_unique_reference = DerivedAttribute('invalid_unique_reference', int, INVALID_UNIQUE_REFERENCE)
    unique_reference_exceeds_limit = DerivedAttribute(
        'unique_reference_exceeds_limit', int, f"Unique Reference {EXCEEDS_CHARACTER_LIMIT} 100")


class DigitrialsDcmAggregateSummaryModel(BaseTableModel):
    __table__ = "aggregate_summary"
    __concrete__ = True

    batch_id = DerivedAttribute('batch_id', int, "submission_id")
    dataset_id = SubmittedAttribute('dataset_id', str)
    trial_name = SubmittedAttribute('trial_name', str)
    dsa_start_date = SubmittedAttribute('dsa_start_date', date)
    dsa_end_date = SubmittedAttribute('dsa_end_date', date)
    nic_number = SubmittedAttribute('nic_number', str)
    submitted_timestamp_utc = SubmittedAttribute('submitted_timestamp_utc', datetime)
    completion_timestamp_utc = SubmittedAttribute('completion_timestamp_utc', datetime)
    all_participant_data_providers = DerivedAttribute('all_participant_data_providers', List[str],
                                                      'cohort_submitter_emails')
    participant_data_provider = DerivedAttribute('participant_data_provider', List[str], 'user_emails')
    filename = SubmittedAttribute('filename', str)
    status = SubmittedAttribute('status', str)
    fields_submitted_in_error = SubmittedAttribute('fields_submitted_in_error', int)
    participants_passed_matching = DerivedAttribute('participants_passed_matching', int, 'participants_matching')
    participants_failed_matching = DerivedAttribute('participants_failed_matching', int, "participant_failed_matching")
    participants_removed = DerivedAttribute('participants_removed', int, "participants_successfully_removed")
    total_rows_submitted = DerivedAttribute('total_rows_submitted', int, 'total_rows_submitted')
    participants_failed_validation = DerivedAttribute(
        'participants_failed_validation', int, 'participant_failed_validation')
    validation_failure_records = SubmittedAttribute('validation_failure_records', ValidationFailureRecords)
