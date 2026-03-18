from pyspark.sql.functions import struct
from pyspark.sql.types import StructType, StructField, StringType

from dsp.common.structured_model import DSPStructuredModel, SubmittedAttribute, AssignableAttribute, \
    META, date, DerivedAttributePlaceholder, CLINICAL, MPSConfidenceScores


class _DIDSMPSModel(DSPStructuredModel):

    META = SubmittedAttribute('META', META)  # type: META
    NHS_NUMBER = SubmittedAttribute('NHS_NUMBER', str)
    NHS_NUMBER_STATUS_IND = SubmittedAttribute('NHS_NUMBER_STATUS', str)
    PERSON_BIRTH_DATE = SubmittedAttribute('DATE_OF_BIRTH', date)
    ETHNIC_CATEGORY = SubmittedAttribute('ETHNIC_CATEGORY_CODE', str)
    PERSON_GENDER_CODE = SubmittedAttribute('GENDER_CODE', int)
    POSTCODE = SubmittedAttribute('POSTCODE', str)
    PRACTICE_CODE = SubmittedAttribute('GP_CODE', str)
    PATIENT_SOURCE_SETTING = SubmittedAttribute('PATIENT_SOURCE_SETTING', str)
    REFERRER = SubmittedAttribute('REFERRER_CODE', str)
    REFERRING_ORG_CODE = SubmittedAttribute('REFERRER_ORGANISATION_CODE', str)
    DIAGNOSTIC_TEST_REQUEST_DATE = SubmittedAttribute('DIAGNOSTIC_TEST_REQUEST_DATE', date)
    DIAG_TEST_REQUEST_REC_DATE = SubmittedAttribute('DIAGNOSTIC_TEST_REQUEST_RECEIVED_DATE', date)
    DIAGNOSTIC_TEST_DATE = SubmittedAttribute('DIAGNOSTIC_TEST_DATE', date)
    IMAGING_CODE_NICIP = SubmittedAttribute('IMAGING_CODE_NICIP', str)
    IMAGING_CODE_SNOMED = SubmittedAttribute('IMAGING_CODE_SNOMED_CT', str)
    SERVICE_REPORT_DATE = SubmittedAttribute('SERVICE_REPORT_ISSUE_DATE', date)
    SITE_CODE_IMAGING = SubmittedAttribute('PROVIDER_SITE_CODE', str)
    RADIOLOGICAL_ACCESSION_NUM = SubmittedAttribute('RADIOLOGICAL_ACCESSION_NUMBER', str)
    SUBMITTER_ORGANISATION = SubmittedAttribute('SUBMITTER_ORGANISATION', str)
    AGE_AT_EVENT = DerivedAttributePlaceholder('AGE_AT_EVENT', int)
    CCG_CODE = DerivedAttributePlaceholder('CCG_CODE', str)
    COMMISSIONING_ORGANISATION_CODE = DerivedAttributePlaceholder('COMMISSIONING_ORGANISATION_CODE', str)
    PROVIDER_CODE = DerivedAttributePlaceholder('PROVIDER_CODE', str)
    CLINICAL = AssignableAttribute('CLINICAL', CLINICAL) # type: AssignableAttribute
    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)
    Person_ID = SubmittedAttribute('Person_ID', str)
    LOCATION_LSOA = DerivedAttributePlaceholder('LOCATION_LSOA', str)
    LOCATION_MSOA = DerivedAttributePlaceholder('LOCATION_MSOA', str)


class DIDSMPSModel(_DIDSMPSModel):
    __concrete__ = True
    __table__ = 'dids'




