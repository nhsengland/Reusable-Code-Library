class FieldConstraints:
    """Where not specified, there are no constraints. We only care about the lengths of data that we SEND to MPS.
    """
    NHS_NO_LENGTH = 10
    FAMILY_NAME_LENGTH = 40
    GIVEN_NAME_LENGTH = 40
    OTHER_GIVEN_NAME_LENGTH = 100
    GENDER_LENGTH = 1
    DATE_OF_BIRTH_FORMAT = 'yyyyMMdd'
    POSTCODE_LENGTH = 8
    DATE_OF_DEATH_FORMAT = 'yyyyMMdd'
    GP_PRACTICE_CODE_LENGTH = 8
    NHAIS_POSTING_ID_LENGTH = 3
    AS_AT_DATE_FORMAT = 'yyyyMMdd'
    TELEPHONE_NUMBER_LENGTH = 32
    MOBILE_NUMBER_LENGTH = 32
    EMAIL_ADDRESS_LENGTH = 90
