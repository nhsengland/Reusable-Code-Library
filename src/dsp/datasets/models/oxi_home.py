from dsp.datasets.common import Fields as CommonFields
from dsp.common.structured_model import _META, DSPStructuredModel, META, SubmittedAttribute


class _Oximeter(DSPStructuredModel):
    META = SubmittedAttribute(CommonFields.META, _META)
    Pat_NHSNum = SubmittedAttribute('Pat_NHSNum', int)
    Pat_Fname = SubmittedAttribute('Pat_Fname', str)
    Pat_Lname = SubmittedAttribute('Pat_Lname', str)
    Pat_PCode = SubmittedAttribute('Pat_Pcode', str)
    Pat_DOB = SubmittedAttribute('Pat_DOB', str)
    Pat_Gender = SubmittedAttribute('Pat_Gender', str)
    Pat_TestResult = SubmittedAttribute('Pat_TestResult', str)
    Pat_RefSourc = SubmittedAttribute('Pat_RefSource', str)
    Pat_OnboardingDate = SubmittedAttribute('Pat_OnboardingDate', int)
    Pat_O2RestSat = SubmittedAttribute('Pat_O2RestSat', str)
    Pat_SymptomOnDate = SubmittedAttribute('Pat_SymptomOnDate', int)
    PatOnB_PatServiceType = SubmittedAttribute('PatOnB_PatServiceType', int)
    PatOffB_SelfDischarge = SubmittedAttribute('PatOffB_SelfDischarge', str)
    PatOffB_OffboardingDate = SubmittedAttribute('PatOffB_OffboardingDate', str)
    PatOffB_PatServiceType = SubmittedAttribute('PatOffB_PatServiceType', str)
    COVID_Virtual_Ward = SubmittedAttribute('COVID_Virtual_Ward', str)


class Oximeter(_Oximeter):
    __concrete__ = True
    META = SubmittedAttribute('META', META)