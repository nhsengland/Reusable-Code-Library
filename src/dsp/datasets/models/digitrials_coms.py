from datetime import datetime

from dsp.datasets.digitrials.common.constants import FIELD_IS_DUPLICATE, NHS_NO_LENGTH, EXCEEDS_CHARACTER_LIMIT, \
    INCORRECT_DATE_FORMAT, INVALID_COMMS_TYPE, FIELD_IS_EMPTY
from dsp.datasets.digitrials.pipelines.coms.constants import NO_FIXED_ABODE, ADDRESS_NOT_KNOWN, ENGLAND_NOT_SPECIFIED, \
    ADDRESS_LINES_NOT_KNOWN, ADDRESS_LINES_AND_POSTCODE_NOT_KNOWN, POSTCODE_NOT_KNOWN, NOT_ENGLISH_POSTCODE, \
    INSUFFICIENT_INFORMATION, POSTCODE_NOT_VALID, POSTCODE_NOT_REGISTERED, CONTACT_NUMBER_NOT_KNOWN, \
    EMAIL_ADDRESS_NOT_KNOWN, CONTACT_NUMBER_NOT_VALID, EMAIL_ADDRESS_NOT_VALID

from dsp.common.structured_model import (
    DSPStructuredModel,
    SubmittedAttribute,
    AssignableAttribute,
    BaseTableModel,
    DerivedAttribute
)

class _DigitrialsComsMPSModel(DSPStructuredModel):
    UNIQUE_REFERENCE = SubmittedAttribute('UNIQUE_REFERENCE', str)
    NHS_NO = SubmittedAttribute('NHS_NO', str)
    FAMILY_NAME = SubmittedAttribute('FAMILY_NAME', str)
    GIVEN_NAME = SubmittedAttribute('GIVEN_NAME', str)
    NATION = SubmittedAttribute('NATION', str)
    DATE_OF_BIRTH = SubmittedAttribute('DATE_OF_BIRTH', str)
    TITLE = SubmittedAttribute('TITLE', str)
    COMMS_TEMPLATE = SubmittedAttribute('COMMS_TEMPLATE', str)
    COMMS_TYPE = SubmittedAttribute('COMMS_TYPE', str)
    LANGUAGE = SubmittedAttribute('LANGUAGE', str)
    ACCESSIBILITY = SubmittedAttribute('ACCESSIBILITY', str)
    SUBMITTED_UNIQUE_REFERENCE = SubmittedAttribute('SUBMITTED_UNIQUE_REFERENCE', str)

    DATE_OF_DEATH = AssignableAttribute('DATE_OF_DEATH', str)  # type: AssignableAttribute
    ADDRESS_LINE1 = AssignableAttribute('ADDRESS_LINE1', str)  # type: AssignableAttribute
    ADDRESS_LINE2 = AssignableAttribute('ADDRESS_LINE2', str)  # type: AssignableAttribute
    ADDRESS_LINE3 = AssignableAttribute('ADDRESS_LINE3', str)  # type: AssignableAttribute
    ADDRESS_LINE4 = AssignableAttribute('ADDRESS_LINE4', str)  # type: AssignableAttribute
    POSTCODE = AssignableAttribute('POSTCODE', str)  # type: AssignableAttribute
    EMAIL_ADDRESS = AssignableAttribute('EMAIL_ADDRESS', str)  # type: AssignableAttribute
    CONTACT_NUMBER = AssignableAttribute('CONTACT_NUMBER', str)  # type: AssignableAttribute
    GP_PRACTICE_CODE = AssignableAttribute('GP_PRACTICE_CODE', str)  # type: AssignableAttribute
    MATCHED_CONFIDENCE_PERCENTAGE = AssignableAttribute('MATCHED_CONFIDENCE_PERCENTAGE', str)


class DigitrialsComsMPSModel(_DigitrialsComsMPSModel):
    __concrete__ = True
    __table__ = 'digitrials_coms_mps'


class ValidationFailureRecords(DSPStructuredModel):
    __table__ = "validation_failure_records"
    __concrete__ = True

    total = DerivedAttribute('total', int, "Total Validation Failure Records")
    unique_reference_duplicate = DerivedAttribute('unique_reference_duplicate', int, f"UNIQUE_REFERENCE {FIELD_IS_DUPLICATE}")
    invalid_nhs_number = DerivedAttribute('invalid_nhs_number', int, NHS_NO_LENGTH)
    invalid_date_of_birth = DerivedAttribute('invalid_date_of_birth', int, f"{INCORRECT_DATE_FORMAT} DATE_OF_BIRTH. Format should be yyyy/mm/dd")
    null_unique_reference = DerivedAttribute('null_unique_reference', int, f"UNIQUE_REFERENCE {FIELD_IS_EMPTY}")
    null_nhs_number = DerivedAttribute('null_nhs_number', int, f"NHS_NO {FIELD_IS_EMPTY}")
    null_date_of_birth = DerivedAttribute('null_date_of_birth', int, f"DATE_OF_BIRTH {FIELD_IS_EMPTY}")
    null_comms_template = DerivedAttribute('null_comms_template', int, f"COMMS_TEMPLATE {FIELD_IS_EMPTY}")
    null_comms_type = DerivedAttribute('null_comms_type', int, f"COMMS_TYPE {FIELD_IS_EMPTY}")
    invalid_comms_type = DerivedAttribute('invalid_comms_type', int, INVALID_COMMS_TYPE)
    family_name_exceeds_limit = DerivedAttribute('family_name_exceeds_limit', int, f"Family Name {EXCEEDS_CHARACTER_LIMIT} 40")
    given_name_exceeds_limit = DerivedAttribute('given_name_exceeds_limit', int, f"Given Name {EXCEEDS_CHARACTER_LIMIT} 40")
    postcode_exceeds_limit = DerivedAttribute('postcode_exceeds_limit', int, f"Postcode {EXCEEDS_CHARACTER_LIMIT} 8")


class UncontactableRecords(DSPStructuredModel):
    __table__ = "uncontactable_records"
    __concrete__ = True

    total = DerivedAttribute('total', int, "Total Uncontactable Records")
    no_fixed_abode = DerivedAttribute('no_fixed_abode', int, NO_FIXED_ABODE)
    address_not_known = DerivedAttribute('address_not_known', int, ADDRESS_NOT_KNOWN)
    address_not_specified = DerivedAttribute('address_not_specified', int, ENGLAND_NOT_SPECIFIED)
    address_lines_not_known = DerivedAttribute('address_lines_not_known', int, ADDRESS_LINES_NOT_KNOWN)
    address_lines_and_postcode_not_known = DerivedAttribute('address_lines_and_postcode_not_known', int, ADDRESS_LINES_AND_POSTCODE_NOT_KNOWN)
    postcode_not_known = DerivedAttribute('postcode_not_known', int, POSTCODE_NOT_KNOWN)
    non_english_postcode = DerivedAttribute('non_english_postcode', int, NOT_ENGLISH_POSTCODE)
    insufficient_information = DerivedAttribute('insufficient_information', int, INSUFFICIENT_INFORMATION)
    postcode_not_valid = DerivedAttribute('postcode_not_valid', int, POSTCODE_NOT_VALID)
    postcode_not_registered = DerivedAttribute('postcode_not_registered', int, POSTCODE_NOT_REGISTERED)
    contact_number_not_known = DerivedAttribute('contact_number_not_known', int, CONTACT_NUMBER_NOT_KNOWN)
    email_address_not_known = DerivedAttribute('email_address_not_known', int, EMAIL_ADDRESS_NOT_KNOWN)
    contact_number_not_valid = DerivedAttribute('contact_number_not_valid', int, CONTACT_NUMBER_NOT_VALID)
    email_address_not_valid = DerivedAttribute('email_address_not_valid', int, EMAIL_ADDRESS_NOT_VALID)


class DigitrialsComsAggregateSummary(BaseTableModel):
    __table__ = "aggregate_summary"
    __concrete__ = True

    batch_id = DerivedAttribute('batch_id', int, "submission_id")
    nic_number = SubmittedAttribute('nic_number', str)
    filename = SubmittedAttribute('filename', str)
    dataset_id = SubmittedAttribute('dataset_id', str)
    submitted_timestamp_utc = SubmittedAttribute('submitted_timestamp_utc', datetime)
    completion_timestamp_utc = SubmittedAttribute('completion_timestamp_utc', datetime)
    status = SubmittedAttribute('status', str)
    total_input_records = DerivedAttribute('total_input_records', int, "total_rows_submitted")
    total_contactable_records = DerivedAttribute('total_contactable_records', int, "Total Contactable Records")
    total_canaries = SubmittedAttribute('total_canaries', int)
    total_deceased_records = DerivedAttribute('total_deceased_records', int, "Total Deceased Records")
    validation_failure_records = SubmittedAttribute("validation_failure_records", ValidationFailureRecords)
    uncontactable_records = SubmittedAttribute("uncontactable_records", UncontactableRecords)
