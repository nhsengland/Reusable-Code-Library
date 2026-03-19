class SpineResponseCodes:
    """
    For a full description of status codes, see:
    https://nhsd-jira.digital.nhs.uk/secure/attachment/137986/SPINE-Phase4errorcodesinresponsefile-270319-1032-236.pdf
    """

    SUCCESS = '00'
    PARSE_ERROR = '01'
    INVALID_REQUEST_REFERENCE = '02'
    INVALID_WORKFLOW_ID = '03'
    DATA_RECORD_COUNT_MISMATCH = '04'
    UNEXPECTED_ERROR = '05'
    MAXIMUM_RECORDS_EXCEEDED = '06'
    FILENAME_REFERENCE_MISMATCH = '07'
    UNIQUE_REFERENCE_ERROR = '08'
    INVALID_TIMESTAMP = '09'
    REQUIRED_FIELD = '10'
    FIELD_LENGTH_EXCEEDED = '11'
    INVALID_GENDER_VALUES = '12'
    INVALID_FIELD_FORMAT = '13'
    DATASTORE_ERROR = '14'
    NO_TRACE_PERFORMED = '15'
    NOT_ENOUGH_FIELDS_PROVIDED = '16'
    NUMBER_OF_FIELDS_GREATER_THAN_ALLOWED = '17'
    SUCCESS_SUPERCEDED = '90'
    INVALID = '91'
    SENSITIVE = '92'
    NOT_ENOUGH_DATA = '96'
    MULTIPLE_CLOSE_MATCHES = '97'
    NOT_FOUND = '98'


SUCCESSFUL_RESPONSE_HEADER_CODES = {
    SpineResponseCodes.SUCCESS,
}

VALID_RESPONSE_CODES = {
    SpineResponseCodes.SUCCESS,
    SpineResponseCodes.NO_TRACE_PERFORMED,
    SpineResponseCodes.SUCCESS_SUPERCEDED,
    SpineResponseCodes.INVALID,
    SpineResponseCodes.SENSITIVE,
    SpineResponseCodes.NOT_ENOUGH_DATA,
    SpineResponseCodes.MULTIPLE_CLOSE_MATCHES,
    SpineResponseCodes.NOT_FOUND,
}

NO_MATCH_NHS_NUMBER = '0000000000'
MULTIPLE_MATCH_NHS_NUMBER = '9999999999'

RESULT_INDICATING_NHS_NUMBERS = {
    NO_MATCH_NHS_NUMBER,
    MULTIPLE_MATCH_NHS_NUMBER
}


class MatchedAlgorithmIndicator:
    NO_MATCH = 0
    CROSS_CHECK = 1
    SIMPLE_TRACE = 2
    ALPHANUMERIC = 3
    ALGORITHMIC = 4


VALID_MATCHED_ALGORITHM_INDICATORS = {
    MatchedAlgorithmIndicator.NO_MATCH,
    MatchedAlgorithmIndicator.CROSS_CHECK,
    MatchedAlgorithmIndicator.SIMPLE_TRACE,
    MatchedAlgorithmIndicator.ALPHANUMERIC,
    MatchedAlgorithmIndicator.ALGORITHMIC
}
