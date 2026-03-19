from dsp.common.structured_model import DSPStructuredModel, SubmittedAttribute, AssignableAttribute


class _PHECancerMPSModel(DSPStructuredModel):
    UNIQUE_REFERENCE = SubmittedAttribute('UNIQUE_REFERENCE', str)
    NHS_NO = SubmittedAttribute('NHS_NO', str)
    FAMILY_NAME = SubmittedAttribute('FAMILY_NAME', str)
    GIVEN_NAME = SubmittedAttribute('GIVEN_NAME', str)
    OTHER_GIVEN_NAME = SubmittedAttribute('OTHER_GIVEN_NAME', str)
    GENDER = SubmittedAttribute('GENDER', str)
    DATE_OF_BIRTH = SubmittedAttribute('DATE_OF_BIRTH', str)
    POSTCODE = SubmittedAttribute('POSTCODE', str)
    DATE_OF_DEATH = SubmittedAttribute('DATE_OF_DEATH', str)
    ADDRESS_LINE1 = SubmittedAttribute('ADDRESS_LINE1', str)
    ADDRESS_LINE2 = SubmittedAttribute('ADDRESS_LINE2', str)
    ADDRESS_LINE3 = SubmittedAttribute('ADDRESS_LINE3', str)
    ADDRESS_LINE4 = SubmittedAttribute('ADDRESS_LINE4', str)
    ADDRESS_LINE5 = SubmittedAttribute('ADDRESS_LINE5', str)
    ADDRESS_DATE = SubmittedAttribute('ADDRESS_DATE', str)
    GP_PRACTICE_CODE = SubmittedAttribute('GP_PRACTICE_CODE', str)
    NHAIS_POSTING_ID = SubmittedAttribute('NHAIS_POSTING_ID', str)
    AS_AT_DATE = SubmittedAttribute('AS_AT_DATE', str)
    LOCAL_PATIENT_ID = SubmittedAttribute('LOCAL_PATIENT_ID', str)
    INTERNAL_ID = SubmittedAttribute('INTERNAL_ID', str)
    TELEPHONE_NUMBER = SubmittedAttribute('TELEPHONE_NUMBER', str)
    MOBILE_NUMBER = SubmittedAttribute('MOBILE_NUMBER', str)
    EMAIL_ADDRESS = SubmittedAttribute('EMAIL_ADDRESS', str)

    Person_ID = AssignableAttribute('Person_ID', str)
    MPSConfidence = AssignableAttribute('MPSConfidence', str)


class PHECancerMPSModel(_PHECancerMPSModel):
    __concrete__ = True
    __table__ = 'phe_cancer_mps'
