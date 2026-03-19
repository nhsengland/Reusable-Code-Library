# pylint: disable=C0301
from dsp.common import enum


class DQErrs(enum.LabelledEnum):
    DQ_LTE = 'DQ1001'
    DQ_FMT_numeric = 'DQ5001'
    DQ_FMT_alpha_numeric = 'DQ5002'
    DQ_FMT_length = 'DQ5003'
    DQ_FMT_yyyyMMdd_dashed = 'DQ7001'
    DQ_NOT_UNIQUE = 'DQ6002'
    DQ_INVALID_DATE_yyyyMMdd_dashed = 'DQ50013'
    DQ_CCG_CODE_INVALID = 'DQ50017'

    DIDS001 = 'DIDS001'
    DIDS002 = 'DIDS002'
    DIDS003 = 'DIDS003'
    DIDS005 = 'DIDS005'
    DIDS006 = 'DIDS006'
    DIDS008 = 'DIDS008'
    DIDS009 = 'DIDS009'
    DIDS010 = 'DIDS010'
    DIDS012 = 'DIDS012'
    DIDS014 = 'DIDS014'
    DIDS016 = 'DIDS016'
    DIDS017 = 'DIDS017'
    DIDS018 = 'DIDS018'
    DIDS020 = 'DIDS020'
    DIDS021 = 'DIDS021'
    DIDS023 = 'DIDS023'
    DIDS024 = 'DIDS024'
    DIDS026 = 'DIDS026'
    DIDS027 = 'DIDS027'
    DIDS028 = 'DIDS028'
    DIDS031 = 'DIDS031'
    DIDS034 = 'DIDS034'
    DIDS035 = 'DIDS035'
    DIDS036 = 'DIDS036'
    DIDS038 = 'DIDS038'
    DIDS040 = 'DIDS040'
    DIDS041 = 'DIDS041'
    DIDS043 = 'DIDS043'
    DIDS044 = 'DIDS044'
    DIDS045 = 'DIDS045'
    DIDS048 = 'DIDS048'
    DIDS049 = 'DIDS049'
    DIDS050 = 'DIDS050'
    DIDS051 = 'DIDS051'
    DIDS055 = 'DIDS055'
    DIDS056 = 'DIDS056'
    DIDS057 = 'DIDS057'
    DIDS059 = 'DIDS059'
    DIDS061 = 'DIDS061'
    DIDS062 = 'DIDS062'
    DIDS066 = 'DIDS066'
    DIDS067 = 'DIDS067'
    DIDS068 = 'DIDS068'
    DIDS069 = 'DIDS069'
    DIDS070 = 'DIDS070'
    DIDS071 = 'DIDS071'
    DIDS072 = 'DIDS072'
    DIDS073 = 'DIDS073'
    DIDS074 = 'DIDS074'
    DIDS075 = 'DIDS075'
    DIDS076 = 'DIDS076'
    DIDS077 = 'DIDS077'
    DIDS078 = 'DIDS078'
    DIDS079 = 'DIDS079'
    DIDS080 = 'DIDS080'
    DIDS081 = 'DIDS081'
    DIDS082 = 'DIDS082'

    __labels__ = {
        ##########################
        # Data Quality - Formats #
        ##########################

        # Logical
        DQ_LTE: '{} should be less than or equal to {}',
        DQ_NOT_UNIQUE: '{} : is invalid, must be unique',

        # Field type
        DQ_FMT_numeric: '{} should be numeric',
        DQ_FMT_alpha_numeric: '{} should be alpha numeric',

        # Precise constraints
        DQ_FMT_length: "{} : length is invalid",
        DQ_FMT_yyyyMMdd_dashed: '{} does not match `yyyy-MM-dd`',

        ################
        # Data Quality #
        ################

        # DQs
        DQ_CCG_CODE_INVALID: '{} : is invalid, not present in reference data',
        DQ_INVALID_DATE_yyyyMMdd_dashed: '{} : is not a valid date, in the format `yyyy-MM-dd`',

        ######################
        # New DIDS DQ Codes  #
        ######################

        # Format
        DIDS001: 'Record rejected - The NHS Number  is invalid, it should be in line with NHS Number specification and '
                 'it must satisfy the modulus 11 algorithm.',
        DIDS002: 'Record rejected - The NHS Number has an incorrect data format.',
        DIDS003: 'Record rejected - The NHS Number Status Indicator Code is invalid, should be one of the following '
                 '01, 02, 03, 04, 05, 06, 07, 08.',
        DIDS005: 'Record rejected - Person Birth Date is not a valid date, in the format `yyyy-MM-dd`.',
        DIDS006: 'Record rejected - Person Birth Date should be less than or equal to the Diagnostic Test Date.',
        DIDS008: 'Record rejected - Person Birth Date should be less than or equal to the Service Report Issue Date.',
        DIDS009: 'Record rejected - Person Birth Date should not be on or before 1900-01-01.',
        DIDS010: 'Record rejected - Person Birth Date cannot be greater than 6 complete months after the Diagnostic '
                 'Test Request Date.',
        DIDS012: 'Record rejected - Ethnic Category is invalid, it should be one of the following A, B, C, D, E, F, G, '
                 'H, J, K, L, M, N, P, R, S, Z, 99.',
        DIDS014: 'Record rejected - The Person Gender Code is not valid.',
        DIDS016: 'Record rejected - Postcode of Usual Address is not a valid postcode in the reference data.',
        DIDS017: 'Record rejected - Postcode of Usual Address has incorrect data format.',
        DIDS018: 'Record rejected - General Medical Practice Code is invalid and not present in the reference data.',
        DIDS020: 'Record rejected - Mandatory field Patient Source Setting Type (Diagnostic Imaging) cannot be null.',
        DIDS021: 'Record rejected - Patient Source Setting Type (Diagnostic Imaging) should be one of the following '
                 '01, 02, 03, 04, 05, 06, 07.',
        DIDS023: 'Warning - The Referrer Code is not in a valid format. The value is now being changed to “99 '
                 'Not Known”. This will cause your whole file to be rejected in the future. Please ensure that the '
                 'code of Referrer is recorded in this field.',
        DIDS024: 'Record rejected - Referring Organisation Code is invalid and not present in the reference data.',
        DIDS026: 'Record rejected - Diagnostic Test Request Date is not a valid date, in the format `yyyy-MM-dd`.',
        DIDS027: 'Record rejected - Diagnostic Test Request Date should be less than or equal to Diagnostic Test '
                 'Request Received Date.',
        DIDS028: 'Warning - Diagnostic Test Request Date should not be greater than 1 year before Diagnostic Test '
                 'Date.',
        DIDS031: 'Record rejected - Diagnostic Test Request Date cannot be before 1970-01-01.',
        DIDS034: 'Record rejected - Diagnostic Test Request Date should be less than or equal to Diagnostic Test Date.',
        DIDS035: 'Record rejected - Service Report Issue Date should be greater than or equal to the Diagnostic Test '
                 'Request Date.',
        DIDS036: 'Record rejected - Diagnostic Test Request Received Date is not a valid date, in the format '
                 '`yyyy-MM-dd`.',
        DIDS038: 'Warning - Diagnostic Test Request Received Date should not be greater than 1 year before the '
                 'Diagnostic Test Date.',
        DIDS040: 'Record rejected - Diagnostic Test Request Received Date should not be more than 6 months before the '
                 'Person Birth Date.',
        DIDS041: 'Record rejected - Diagnostic Test Request Received Date should not be before 1970-01-01.',
        DIDS043: 'Record rejected - Diagnostic Test Request Received Date should be less than or equal to the '
                 'Diagnostic Test Date.',
        DIDS044: 'Record rejected - Diagnostic Test Request Received Date should be less than or equal to the Service '
                 'Report Issue Date.',
        DIDS045: 'Record rejected - Diagnostic Test Date is not a valid date, in the format `yyyy-MM-dd`.',
        DIDS048: 'Record rejected - Service Report Issue Date should be greater than or equal to the Diagnostic Test '
                 'Date.',
        DIDS049: 'Warning - Diagnostic Test Date should not be greater than 3 months before the beginning of the '
                 'submission month.',
        DIDS050: 'Record rejected - Mandatory field Diagnostic Test Date cannot be null.',
        DIDS051: 'Record rejected - Diagnostic Test Date cannot be after the submission date.',
        DIDS055: 'Record rejected - Diagnostic Test Date cannot be greater than 6 months before the beginning of the '
                 'submission month.',
        DIDS056: 'Record rejected - No clinical codes provided for either Imaging Code (NICIP)  or Imaging Code '
                 '(SNOMED).',
        DIDS057: 'Record rejected - The Imaging Code (NICIP) is an invalid code.',
        DIDS059: 'Record rejected - The Imaging Code (SNOMED) is an invalid code.',
        DIDS061: 'Record rejected - Service Report Issue Date is not a valid date, in the format `yyyy-MM-dd`.',
        DIDS062: 'Warning - Service Report Issue Date is more than 1 month after the Diagnostic Test Date.',
        DIDS066: 'Record rejected - Service Report Issue Date cannot be after the submission date.',
        DIDS067: 'Record rejected - Mandatory field Site Code of Imaging cannot be null.',
        DIDS068: 'Record rejected - Site Code of Imaging is invalid and not present in the reference data.',
        DIDS069: 'Record rejected - Site Code of Imaging is in an invalid format.',
        DIDS070: 'Record rejected - Mandatory field Radiological Accession Number cannot be null.',
        DIDS071: 'Record rejected - Duplicate combination of Radiological Accession Number and Site Code (of Imaging) '
                 'detected; each distinct spell of scanning activity must be represented by a unique accession number '
                 'and provider site code combination.',
        DIDS072: 'Record rejected - Radiological Accession Number is an invalid data type.',
        DIDS073: 'Record rejected - Failed Personal Demographic Required field Validation. At least one of the '
                 'following fields is required: Ethnic Category, NHS Number, Person Birth Date, Person Gender Code '
                 'Current, Postcode of Usual Address, General Medical Practice Code (Patient Registration).',
        DIDS074: 'Record rejectied - Failed Personal Demographic due to an invalid Postcode of Usual Address.',
        DIDS075: 'File rejected - Invalid .csv file - Incorrect number of columns submitted.',
        DIDS076: 'File rejected - Invalid .xml file - Unexpected file structure.',
        DIDS077: 'Warning - The Imaging Code (NICIP) is now an inactive code. Future submissions of this code will be '
                 'rejected.',
        DIDS078: 'Warning - The Imaging Code (SNOMED) is now an inactive code. Future submissions of this code will be '
                 'rejected.',
        DIDS079: 'Warning - Referring Organisation Code was not a live organisation at the relevant point in time '
                 '(Diagnostic Test Request Date if submitted, or Diagnostic Test Date).',
        DIDS080: 'Warning - General Medical Practice Code was not  a live organisation at the relevant point in time '
                 '(Diagnostic Test Date).',
        DIDS081: 'Warning - Site Code of Imaging was not  a live organisation at the relevant point in time '
                 '(Diagnostic Test Date).',
        DIDS082: 'Record rejected - Imaging Code (NICIP) does not map to Imaging Code (SNOMED).'

    }
