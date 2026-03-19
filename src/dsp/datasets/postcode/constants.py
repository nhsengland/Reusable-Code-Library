COVID19_TESTING_TRANSFORM_MAP = {
    'nhs_number': 'NHSNumber',
    'subject_id': 'SubjectID',
    'test_id': 'TestID',
    'test_centre_id': 'TestCentreID',
    'specimen_id': 'SpecimenID',
    'kw_id': 'KeyWorkerID',
    'kw_type': 'KeyWorkerType',
    'kw_name': 'KeyWorkerName',
    'kw_mobile': 'KeyWorkerMobile',
    'kw_organisation': 'KeyWorkerOrganisation',
    'address_1': 'AddressLine1',
    'address_2': 'AddressLine2',
    'care_home_location_id': 'CareHomeLocationID',
    'national_practice_code': 'GPCode',
    'employer': 'EmployerName',
    'industry': 'IndustrySector'
}


class NHSLFDTestsConstants:
    PRE_TRANSFORM_DQ_DF = 'nhs_lfd_tests_pre_transform_dq'
    POST_TRANSFORM_DQ_DF = 'nhs_lfd_tests_post_transform_dq'
    TRANSFORMED_DF = 'nhs_lfd_tests_transformed'
    UK_DATE_FORMAT = 'yyyy-MM-dd'
    US_DATE_FORMAT = 'yyyy-dd-MM'
    UK_DATE_HYPHEN_FORMAT = 'dd-MM-yyyy'
    UK_DATE_SLASH_FORMAT = 'dd/MM/yyyy'
    US_DATE_SLASH_FORMAT = 'MM/dd/yyyy'
    UK_DATE_FORMAT_YY = 'dd-MM-yy'
    UK_DATE_SLASH_FORMAT_YY = 'dd/MM/yy'
    UK_DATE_SINGLE_DIGIT_FORMAT = 'd-M-yyyy'
    UK_DATE_SLASH_SINGLE_DIGIT_FORMAT = 'd/M/yyyy'

