from dsp.common.structured_model import DSPStructuredModel, SubmittedAttribute, AssignableAttribute, META
from datetime import date
from typing import List

class _RapidCancerDiagMPSModel(DSPStructuredModel):
    nhsnumber = SubmittedAttribute('nhsnumber', str)
    dateofbirth = SubmittedAttribute('dateofbirth', date)
    providercode = SubmittedAttribute('providercode', str)
    fasterdiagnosis_enddate = SubmittedAttribute('fasterdiagnosis_enddate', date)
    dateofreferral = SubmittedAttribute('dateofreferral', date)
    canceralliancecode = SubmittedAttribute('canceralliancecode', str)

    META = AssignableAttribute('META', META)
    Person_ID = AssignableAttribute('Person_ID', str)

class _RapidCancerDiagSiteSpecificMPSModel(_RapidCancerDiagMPSModel):
    sitespecificreferralpathway = SubmittedAttribute('sitespecificreferralpathway', str)

class _RapidCancerDiagNonSiteSpecificMPSModel(_RapidCancerDiagMPSModel):
    consultationsbeforereferral = SubmittedAttribute('consultationsbeforereferral', str)
    finaldiagnosisstatus = SubmittedAttribute('finaldiagnosisstatus', str)
    historyofalcoholconsumption = SubmittedAttribute('historyofalcoholconsumption', str)
    performancestatus_adult = SubmittedAttribute('performancestatus_adult', str)
    referralresult = SubmittedAttribute('referralresult', str)
    smokingstatus = SubmittedAttribute('smokingstatus', str)
    symptoms_other = SubmittedAttribute('symptoms_other', str)
    dateoffirstsymptoms = SubmittedAttribute('dateoffirstsymptoms', date)
    dateofonwardreferralordischarge = SubmittedAttribute('dateofonwardreferralordischarge', date)
    dateoftriage = SubmittedAttribute('dateoftriage', date)
    comorbidities = SubmittedAttribute('comorbidities', List[str])
    filterfunctiontests = SubmittedAttribute('filterfunctiontests', List[str])
    symptoms = SubmittedAttribute('symptoms', List[str])
    test = SubmittedAttribute('test', str)
    test_date = SubmittedAttribute('test_date', date)
    other_test = SubmittedAttribute('other_test', str)
    other_test_date = SubmittedAttribute('other_test_date', date)

class RapidCancerDiagSiteSpecificMPSModel(_RapidCancerDiagSiteSpecificMPSModel):
    __concrete__ = True
    __table__ = 'rapid_cancer_diag_mps'

class RapidCancerDiagNonSiteSpecificMPSModel(_RapidCancerDiagNonSiteSpecificMPSModel):
    __concrete__ = True
    __table__ = 'rapid_cancer_diag_mps'
