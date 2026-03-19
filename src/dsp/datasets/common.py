from collections import namedtuple
from enum import Enum

from dsp.pipeline import ValidationResult


class Fields:
    META = 'META'
    EVENT_ID = 'EVENT_ID'
    RECORD_INDEX = 'RECORD_INDEX'
    EVENT_RECEIVED_TS = 'EVENT_RECEIVED_TS'
    DQ_ERR = '__DQ_ERR'
    DQ_WARN = '__DQ_WARN'
    DQ_UNIQ = '__DQ_UNIQ'
    DQ = 'DQ'
    DQ_TS = 'DQ_TS'
    DATASET = 'DATASET'
    ATTRIBUTE = 'ATTRIBUTE'
    FIELDS = 'FIELDS'
    CODE = 'CODE'
    MESSAGE = 'MESSAGE'
    TYPE = 'TYPE'
    EFFECTIVE_FROM = 'EFFECTIVE_FROM'
    EFFECTIVE_TO = 'EFFECTIVE_TO'
    SUBMISSION_ID = 'SUBMISSION_ID'
    EVENT_DATE = 'EVENT_DATE'
    MONTH_ID = 'MonthId'
    DATASET_VERSION = "DATASET_VERSION"
    RECORD_VERSION = "RECORD_VERSION"
    UNIQUE_REFERENCE = 'UNIQUE_REFERENCE'
    AS_AT_DATE = 'AS_AT_DATE'
    UNIQ_ID = 'UNIQ_ID'


class TableRelSpecFields:
    FLOATING = "floating"
    PRIMARY = "primary"
    FIELD_NAME = "field_name"
    TABLE = "table"
    CHILD_TABLES = "child_tables"
    ROOT = "root"
    TABLE_NAME_PREFIX = ".name"
    CARDINALITY = "cardinality"
    JOIN_KEY = "join_key"
    REFERENTIAL_RULES = "referential_rules"
    FOREIGN_TABLE = "foreign_table"
    FOREIGN_KEY = "foreign_key"
    LOCAL_KEY = "local_key"
    IS_PARENT = "is_parent"
    HEADER = "header",
    FIELD = "field"
    TYPE = "type"


class PDSCohortFields:
    NHS_NUMBER = 'NHS_NUMBER'
    ACTIVITY_DATE = 'ACTIVITY_DATE'
    SERIAL_CHANGE_NUMBER = 'SERIAL_CHANGE_NUMBER'
    DATE_OF_BIRTH = 'DATE_OF_BIRTH'
    DATE_OF_DEATH = 'DATE_OF_DEATH'
    PARSED_RECORD = 'parsed_record'
    RECORD = 'record'
    TRANSFER_ID = 'TRANSFER_ID'
    REPLACED_BY = 'replaced_by'
    EMAIL_ADDRESS = 'emailAddress'
    MOBILE_PHONE = 'mobilePhone'
    TELEPHONE = 'telephone'
    DOB = 'dob'
    DOD = 'dod'
    DATE = 'date'
    GP_CODES = 'gp_codes'
    VAL = 'val'
    FROM = 'from'
    TO = 'to'
    SCN = 'scn'
    ADDRESS_HISTORY = 'address_history'
    ADDR = 'addr'
    LINE = 'line'
    POSTAL_CODE = 'postalCode'
    NAME_HISTORY = 'name_history'
    GIVEN_NAME = 'givenName'
    NAME = 'name'
    FAMILY_NAME = 'familyName'
    GENDER_HISTORY = 'gender_history'
    GENDER = 'gender'
    CONFIDENTIALITY = 'confidentiality'
    MIGRANT_DATA = 'migrant_data'
    VISA_STATUS = 'visa_status'
    BRP_NO = 'brp_no'
    HOME_OFFICE_REF_NO = 'home_office_ref_no'
    NATIONALITY = 'nationality'
    VISA_FROM = 'visa_from'
    VISA_TO = 'visa_to'
    MONTH_OF_BIRTH = 'MONTH_OF_BIRTH'
    YEAR_OF_BIRTH = 'YEAR_OF_BIRTH'
    BIRTH_DECADE = 'BIRTH_DECADE'
    GP = 'gp'
    ADDRESS = 'address'
    SENSITIVE = 'sensitive'
    DEATH_STATUS = 'death_status'
    MPSID_DETAILS = 'mpsid_details'
    MPS_ID = 'mpsID'
    LPI = 'localPatientID'


DQRule = namedtuple('DQRule', ['attribute', 'fields', 'is_error', 'condition', 'code'])

DQMessage = namedtuple('DQMessage',
                       ['table', 'row_index', 'attribute', 'fields', 'code', 'message', 'field_values', 'type',
                        'action'])

UniquenessRule = namedtuple('UniquenessRule', ['unique_columns', 'code'])


class DQMessageType:
    Error = 'error'
    Warning = 'warning'


class Cardinality(Enum):
    ONE = 0
    MANY = 1


def reject_on_warning_or_error(validation_result: ValidationResult):
    return validation_result.dq_error_count or validation_result.dq_warning_count


def reject_on_error(validation_result: ValidationResult):
    return validation_result.dq_error_count


def reject_on_zero_dq_pass(validation_result: ValidationResult):
    return validation_result.dq_pass_count < 1
