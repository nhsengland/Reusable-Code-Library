# pylint: disable=line-too-long,too-many-lines

import inspect
import sys
from typing import Generator, List, Type

from dsp.datasets.models.mhsds_v6_base import *
from dsp.common.expressions import UniqMonth, Select
from dsp.common.structured_model import (
    META,
    SubmittedAttribute,
    RepeatingSubmittedAttribute,
    AssignableAttribute,
    MPSConfidenceScores,
    DerivedAttribute)
from dsp.shared import safe_issubclass


__all__ = [
    'AbsenceWithoutLeave',
    'AccommodationStatus',
    'AnonymousSelfAssessment',
    'Assault',
    'AssignedCareProfessional',
    'AssistiveTechnologyToSupportDisabilityType',
    'CareActivity',
    'CareContact',
    'CarePlanAgreement',
    'CarePlanType',
    'CareProgramApproachCareEpisode',
    'CareProgramApproachReview',
    'ClinicallyReadyforDischarge',
    'CodedScoredAssessmentCareActivity',
    'CodedScoredAssessmentReferral',
    'CommunityTreatmentOrder',
    'CommunityTreatmentOrderRecall',
    'ConditionalDischarge',
    'DisabilityType',
    'DischargePlanAgreement',
    'EmploymentStatus',
    'GPPracticeRegistration',
    'GroupSession',
    'Header',
    'HomeLeave',
    'HospitalProviderSpell',
    'HospitalProviderSpellCommissionerAssignmentPeriod',
    'IndirectActivity',
    'LeaveOfAbsence',
    'MasterPatientIndex',
    'MedicalHistoryPreviousDiagnosis',
    'MentalHealthActLegalStatusClassificationAssignmentPeriod',
    'MentalHealthCareCoordinator',
    'MentalHealthDropInContact',
    'MentalHealthResponsibleClinicianAssignmentPeriod',
    'OnwardReferral',
    'OtherInAttendance',
    'OtherReasonForReferral',
    'OverseasVisitorChargingCategory',
    'PatientIndicators',
    'PatSelfDirectedDigitalIntervention',
    'PoliceAssistanceRequest',
    'PresentingComplaint',
    'PrimaryDiagnosis',
    'Referral',
    'ReferralToTreatment',
    'RestrictiveInterventionIncident',
    'RestrictiveInterventionType',
    'SecondaryDiagnosis',
    'SelfHarm',
    'ServiceOrTeamDetails',
    'OtherServiceType',
    'SocialAndPersonalCircumstances',
    'SpecialisedMentalHealthExceptionalPackageOfCare',
    'StaffActivity',
    'StaffDetails',
    'SubstanceMisuse',
    'TrialLeave',
    'WardDetails',
    'WardStay',
    'eMED3FitNote',
    'get_all_models',
    'get_anonymous_models'
]


class Header(_Header):
    """MHS000Header"""

    # -- auto-generated --

    __table__ = "MHS000Header"
    __concrete__ = True

    # ^^ auto-generated ^^


class GPPracticeRegistration(_GPPracticeRegistration):
    """MHS002GP"""

    # -- auto-generated --

    __table__ = "MHS002GP"
    __concrete__ = True

    # ^^ auto-generated ^^


class AccommodationStatus(_AccommodationStatus):
    """MHS003AccommStatus"""

    # -- auto-generated --

    __table__ = "MHS003AccommStatus"
    __concrete__ = True

    # ^^ auto-generated ^^


class EmploymentStatus(_EmploymentStatus):
    """MHS004EmpStatus"""

    # -- auto-generated --

    __table__ = "MHS004EmpStatus"
    __concrete__ = True

    # ^^ auto-generated ^^


class PatientIndicators(_PatientIndicators):
    """MHS005PatInd"""

    # -- auto-generated --

    __table__ = "MHS005PatInd"
    __concrete__ = True

    # ^^ auto-generated ^^


class MentalHealthCareCoordinator(_MentalHealthCareCoordinator):
    """MHS006MHCareCoord"""

    # -- auto-generated --

    __table__ = "MHS006MHCareCoord"
    __concrete__ = True

    # ^^ auto-generated ^^


class DisabilityType(_DisabilityType):
    """MHS007DisabilityType"""

    # -- auto-generated --

    __table__ = "MHS007DisabilityType"
    __concrete__ = True

    # ^^ auto-generated ^^


class CarePlanAgreement(_CarePlanAgreement):
    """MHS009CarePlanAgreement"""

    # -- auto-generated --

    __table__ = "MHS009CarePlanAgreement"
    __concrete__ = True

    # ^^ auto-generated ^^


class AssistiveTechnologyToSupportDisabilityType(_AssistiveTechnologyToSupportDisabilityType):
    """MHS010AssTechToSupportDisTyp"""

    # -- auto-generated --

    __table__ = "MHS010AssTechToSupportDisTyp"
    __concrete__ = True

    # ^^ auto-generated ^^


class SocialAndPersonalCircumstances(_SocialAndPersonalCircumstances):
    """MHS011SocPerCircumstances"""

    # -- auto-generated --

    __table__ = "MHS011SocPerCircumstances"
    __concrete__ = True

    # ^^ auto-generated ^^


class OverseasVisitorChargingCategory(_OverseasVisitorChargingCategory):
    """MHS012OverseasVisitorChargCat"""

    # -- auto-generated --

    __table__ = "MHS012OverseasVisitorChargCat"
    __concrete__ = True

    # ^^ auto-generated ^^


class eMED3FitNote(_eMED3FitNote):
    """MHS014eMED3FitNote"""

    # -- auto-generated --

    __table__ = "MHS014eMED3FitNote"
    __concrete__ = True

    # ^^ auto-generated ^^


class OtherServiceType(_OtherServiceType):
    """MHS102OtherServiceType"""

    # -- auto-generated --

    __table__ = "MHS102OtherServiceType"
    __concrete__ = True

    # ^^ auto-generated ^^


class OtherReasonForReferral(_OtherReasonForReferral):
    """MHS103OtherReasonReferral"""

    # -- auto-generated --

    __table__ = "MHS103OtherReasonReferral"
    __concrete__ = True

    # ^^ auto-generated ^^


class ReferralToTreatment(_ReferralToTreatment):
    """MHS104RTT"""

    # -- auto-generated --

    __table__ = "MHS104RTT"
    __concrete__ = True

    # ^^ auto-generated ^^


class OnwardReferral(_OnwardReferral):
    """MHS105OnwardReferral"""

    # -- auto-generated --

    __table__ = "MHS105OnwardReferral"
    __concrete__ = True

    # ^^ auto-generated ^^


class DischargePlanAgreement(_DischargePlanAgreement):
    """MHS106DischargePlanAgreement"""

    # -- auto-generated --

    __table__ = "MHS106DischargePlanAgreement"
    __concrete__ = True

    # ^^ auto-generated ^^


class OtherInAttendance(_OtherInAttendance):
    """MHS203OtherAttend"""

    # -- auto-generated --

    __table__ = "MHS203OtherAttend"
    __concrete__ = True

    # ^^ auto-generated ^^


class IndirectActivity(_IndirectActivity):
    """MHS204IndirectActivity"""

    # -- auto-generated --

    __table__ = "MHS204IndirectActivity"
    __concrete__ = True

    # ^^ auto-generated ^^


class PatSelfDirectedDigitalIntervention(_PatSelfDirectedDigitalIntervention):
    """MHS205PatientSDDI"""

    # -- auto-generated --

    __table__ = "MHS205PatientSDDI"
    __concrete__ = True

    # ^^ auto-generated ^^


class StaffActivity(_StaffActivity):
    """MHS206StaffActivity"""

    # -- auto-generated --

    __table__ = "MHS206StaffActivity"
    __concrete__ = True

    # ^^ auto-generated ^^


class MentalHealthResponsibleClinicianAssignmentPeriod(_MentalHealthResponsibleClinicianAssignmentPeriod):
    """MHS402RespClinicianAssignPeriod"""

    # -- auto-generated --

    __table__ = "MHS402RespClinicianAssignPeriod"
    __concrete__ = True

    # ^^ auto-generated ^^


class ConditionalDischarge(_ConditionalDischarge):
    """MHS403ConditionalDischarge"""

    # -- auto-generated --

    __table__ = "MHS403ConditionalDischarge"
    __concrete__ = True

    # ^^ auto-generated ^^


class CommunityTreatmentOrder(_CommunityTreatmentOrder):
    """MHS404CommTreatOrder"""

    # -- auto-generated --

    __table__ = "MHS404CommTreatOrder"
    __concrete__ = True

    # ^^ auto-generated ^^


class CommunityTreatmentOrderRecall(_CommunityTreatmentOrderRecall):
    """MHS405CommTreatOrderRecall"""

    # -- auto-generated --

    __table__ = "MHS405CommTreatOrderRecall"
    __concrete__ = True

    # ^^ auto-generated ^^


class AssignedCareProfessional(_AssignedCareProfessional):
    """MHS503AssignedCareProf"""

    # -- auto-generated --

    __table__ = "MHS503AssignedCareProf"
    __concrete__ = True

    # ^^ auto-generated ^^


class Assault(_Assault):
    """MHS506Assault"""

    # -- auto-generated --

    __table__ = "MHS506Assault"
    __concrete__ = True

    # ^^ auto-generated ^^


class SelfHarm(_SelfHarm):
    """MHS507SelfHarm"""

    # -- auto-generated --

    __table__ = "MHS507SelfHarm"
    __concrete__ = True

    # ^^ auto-generated ^^


class HomeLeave(_HomeLeave):
    """MHS509HomeLeave"""

    # -- auto-generated --

    __table__ = "MHS509HomeLeave"
    __concrete__ = True

    # ^^ auto-generated ^^


class LeaveOfAbsence(_LeaveOfAbsence):
    """MHS510LeaveOfAbsence"""

    # -- auto-generated --

    __table__ = "MHS510LeaveOfAbsence"
    __concrete__ = True

    # ^^ auto-generated ^^


class AbsenceWithoutLeave(_AbsenceWithoutLeave):
    """MHS511AbsenceWithoutLeave"""

    # -- auto-generated --

    __table__ = "MHS511AbsenceWithoutLeave"
    __concrete__ = True

    # ^^ auto-generated ^^


class HospitalProviderSpellCommissionerAssignmentPeriod(_HospitalProviderSpellCommissionerAssignmentPeriod):
    """MHS512HospSpellCommAssPer"""

    # -- auto-generated --

    __table__ = "MHS512HospSpellCommAssPer"
    __concrete__ = True

    # ^^ auto-generated ^^


class SubstanceMisuse(_SubstanceMisuse):
    """MHS513SubstanceMisuse"""

    # -- auto-generated --

    __table__ = "MHS513SubstanceMisuse"
    __concrete__ = True

    # ^^ auto-generated ^^


class TrialLeave(_TrialLeave):
    """MHS514TrialLeave"""

    # -- auto-generated --

    __table__ = "MHS514TrialLeave"
    __concrete__ = True

    # ^^ auto-generated ^^


class RestrictiveInterventionType(_RestrictiveInterventionType):
    """MHS515RestrictiveInterventType"""

    # -- auto-generated --

    __table__ = "MHS515RestrictiveInterventType"
    __concrete__ = True

    # ^^ auto-generated ^^


class PoliceAssistanceRequest(_PoliceAssistanceRequest):
    """MHS516PoliceAssistanceRequest"""

    # -- auto-generated --

    __table__ = "MHS516PoliceAssistanceRequest"
    __concrete__ = True

    # ^^ auto-generated ^^


class SpecialisedMentalHealthExceptionalPackageOfCare(_SpecialisedMentalHealthExceptionalPackageOfCare):
    """MHS517SMHExceptionalPackOfCare"""

    # -- auto-generated --

    __table__ = "MHS517SMHExceptionalPackOfCare"
    __concrete__ = True

    # ^^ auto-generated ^^


class ClinicallyReadyforDischarge(_ClinicallyReadyforDischarge):
    """MHS518ClinReadyforDischarge"""

    # -- auto-generated --

    __table__ = "MHS518ClinReadyforDischarge"
    __concrete__ = True

    # ^^ auto-generated ^^


class MedicalHistoryPreviousDiagnosis(_MedicalHistoryPreviousDiagnosis):
    """MHS601MedHistPrevDiag"""

    # -- auto-generated --

    __table__ = "MHS601MedHistPrevDiag"
    __concrete__ = True

    # ^^ auto-generated ^^


class PrimaryDiagnosis(_PrimaryDiagnosis):
    """MHS604PrimDiag"""

    # -- auto-generated --

    __table__ = "MHS604PrimDiag"
    __concrete__ = True

    # ^^ auto-generated ^^


class SecondaryDiagnosis(_SecondaryDiagnosis):
    """MHS605SecDiag"""

    # -- auto-generated --

    __table__ = "MHS605SecDiag"
    __concrete__ = True

    # ^^ auto-generated ^^


class CodedScoredAssessmentReferral(_CodedScoredAssessmentReferral):
    """MHS606CodedScoreAssessmentRefer"""

    # -- auto-generated --

    __table__ = "MHS606CodedScoreAssessmentRefer"
    __concrete__ = True

    # ^^ auto-generated ^^


class ServiceOrTeamDetails(_ServiceOrTeamDetails):
    """MHS902ServiceTeamDetails"""

    # -- auto-generated --

    __table__ = "MHS902ServiceTeamDetails"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_ServiceOrTeamDetails.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute


class WardDetails(_WardDetails):
    """MHS903WardDetails"""

    # -- auto-generated --

    __table__ = "MHS903WardDetails"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_WardDetails.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

class CodedScoredAssessmentCareActivity(_CodedScoredAssessmentCareActivity):
    """MHS607CodedScoreAssessmentAct"""

    # -- auto-generated --

    __table__ = "MHS607CodedScoreAssessmentAct"
    __concrete__ = True

    # ^^ auto-generated ^^


class PresentingComplaint(_PresentingComplaint):
    """MHS609PresComp"""

    # -- auto-generated --

    __table__ = "MHS609PresComp"
    __concrete__ = True

    # ^^ auto-generated ^^


class CareProgramApproachReview(_CareProgramApproachReview):
    """MHS702CPAReview"""

    # -- auto-generated --

    __table__ = "MHS702CPAReview"
    __concrete__ = True

    # ^^ auto-generated ^^


class CareProgramApproachCareEpisode(_CareProgramApproachCareEpisode):
    """MHS701CPACareEpisode"""

    # -- auto-generated --

    __table__ = "MHS701CPACareEpisode"
    __concrete__ = True

    CPAReviews = RepeatingSubmittedAttribute('CPAReviews', CareProgramApproachReview)  # type: List[CareProgramApproachReview]

    # ^^ auto-generated ^^


class RestrictiveInterventionIncident(_RestrictiveInterventionIncident):
    """MHS505RestrictiveInterventInc"""

    # -- auto-generated --

    __table__ = "MHS505RestrictiveInterventInc"
    __concrete__ = True

    RestrictiveInterventionTypes = RepeatingSubmittedAttribute('RestrictiveInterventionTypes', RestrictiveInterventionType)  # type: List[RestrictiveInterventionType]

    # ^^ auto-generated ^^


class WardStay(_WardStay):
    """MHS502WardStay"""

    # -- auto-generated --

    __table__ = "MHS502WardStay"
    __concrete__ = True

    Assaults = RepeatingSubmittedAttribute('Assaults', Assault)  # type: List[Assault]
    SelfHarms = RepeatingSubmittedAttribute('SelfHarms', SelfHarm)  # type: List[SelfHarm]
    HomeLeaves = RepeatingSubmittedAttribute('HomeLeaves', HomeLeave)  # type: List[HomeLeave]
    LeaveOfAbsences = RepeatingSubmittedAttribute('LeaveOfAbsences', LeaveOfAbsence)  # type: List[LeaveOfAbsence]
    AbsenceWithoutLeaves = RepeatingSubmittedAttribute('AbsenceWithoutLeaves', AbsenceWithoutLeave)  # type: List[AbsenceWithoutLeave]
    SubstanceMisuses = RepeatingSubmittedAttribute('SubstanceMisuses', SubstanceMisuse)  # type: List[SubstanceMisuse]
    TrialLeaves = RepeatingSubmittedAttribute('TrialLeaves', TrialLeave)  # type: List[TrialLeave]
    PoliceAssistanceRequests = RepeatingSubmittedAttribute('PoliceAssistanceRequests', PoliceAssistanceRequest)  # type: List[PoliceAssistanceRequest]

    # ^^ auto-generated ^^


class HospitalProviderSpell(_HospitalProviderSpell):
    """MHS501HospProvSpell"""

    # -- auto-generated --

    __table__ = "MHS501HospProvSpell"
    __concrete__ = True

    WardStays = RepeatingSubmittedAttribute('WardStays', WardStay)  # type: List[WardStay]
    AssignedCareProfessionals = RepeatingSubmittedAttribute('AssignedCareProfessionals', AssignedCareProfessional)  # type: List[AssignedCareProfessional]
    RestrictiveInterventionIncidents = RepeatingSubmittedAttribute('RestrictiveInterventionIncidents', RestrictiveInterventionIncident)  # type: List[RestrictiveInterventionIncident]
    HospitalProviderSpellCommissionersAssignmentPeriods = RepeatingSubmittedAttribute('HospitalProviderSpellCommissionersAssignmentPeriods', HospitalProviderSpellCommissionerAssignmentPeriod)  # type: List[HospitalProviderSpellCommissionerAssignmentPeriod]
    SpecialisedMentalHealthExceptionalPackageOfCares = RepeatingSubmittedAttribute('SpecialisedMentalHealthExceptionalPackageOfCares', SpecialisedMentalHealthExceptionalPackageOfCare)  # type: List[SpecialisedMentalHealthExceptionalPackageOfCare]
    ClinicallyReadyforDischarge = RepeatingSubmittedAttribute('ClinicallyReadyforDischarge', ClinicallyReadyforDischarge)  # type: List[ClinicallyReadyforDischarge]

    # ^^ auto-generated ^^


class MentalHealthActLegalStatusClassificationAssignmentPeriod(_MentalHealthActLegalStatusClassificationAssignmentPeriod):
    """MHS401MHActPeriod"""

    # -- auto-generated --

    __table__ = "MHS401MHActPeriod"
    __concrete__ = True

    ResponsibleClinicianAssignmentPeriods = RepeatingSubmittedAttribute('ResponsibleClinicianAssignmentPeriods', MentalHealthResponsibleClinicianAssignmentPeriod)  # type: List[MentalHealthResponsibleClinicianAssignmentPeriod]
    ConditionalDischarges = RepeatingSubmittedAttribute('ConditionalDischarges', ConditionalDischarge)  # type: List[ConditionalDischarge]
    CommunityTreatmentOrders = RepeatingSubmittedAttribute('CommunityTreatmentOrders', CommunityTreatmentOrder)  # type: List[CommunityTreatmentOrder]
    CommunityTreatmentOrderRecalls = RepeatingSubmittedAttribute('CommunityTreatmentOrderRecalls', CommunityTreatmentOrderRecall)  # type: List[CommunityTreatmentOrderRecall]

    # ^^ auto-generated ^^


class CareActivity(_CareActivity):
    """MHS202CareActivity"""

    # -- auto-generated --

    __table__ = "MHS202CareActivity"
    __concrete__ = True

    StaffActivities = RepeatingSubmittedAttribute('StaffActivities', StaffActivity)  # type: List[StaffActivity]
    CodedScoredAssessmentCareActivities = RepeatingSubmittedAttribute('CodedScoredAssessmentCareActivities', CodedScoredAssessmentCareActivity)  # type: List[CodedScoredAssessmentCareActivity]

    # ^^ auto-generated ^^


class CareContact(_CareContact):
    """MHS201CareContact"""

    # -- auto-generated --

    __table__ = "MHS201CareContact"
    __concrete__ = True

    CareActivities = RepeatingSubmittedAttribute('CareActivities', CareActivity)  # type: List[CareActivity]
    OtherAttendances = RepeatingSubmittedAttribute('OtherAttendances', OtherInAttendance)  # type: List[OtherInAttendance]

    # ^^ auto-generated ^^


class CarePlanType(_CarePlanType):
    """MHS008CarePlanType"""

    # -- auto-generated --

    __table__ = "MHS008CarePlanType"
    __concrete__ = True

    CarePlanAgreements = RepeatingSubmittedAttribute('CarePlanAgreements', CarePlanAgreement)  # type: List[CarePlanAgreement]

    # ^^ auto-generated ^^


class MasterPatientIndex(_MasterPatientIndex):
    """MHS001MPI"""

    # -- auto-generated --

    __table__ = "MHS001MPI"
    __concrete__ = True

    GPPracticeRegistrations = RepeatingSubmittedAttribute('GPPracticeRegistrations', GPPracticeRegistration)  # type: List[GPPracticeRegistration]
    AccommodationStatuses = RepeatingSubmittedAttribute('AccommodationStatuses', AccommodationStatus)  # type: List[AccommodationStatus]
    EmploymentStatuses = RepeatingSubmittedAttribute('EmploymentStatuses', EmploymentStatus)  # type: List[EmploymentStatus]
    PatientIndicators = RepeatingSubmittedAttribute('PatientIndicators', PatientIndicators)  # type: List[PatientIndicators]
    MentalHealthCareCoordinators = RepeatingSubmittedAttribute('MentalHealthCareCoordinators', MentalHealthCareCoordinator)  # type: List[MentalHealthCareCoordinator]
    DisabilityTypes = RepeatingSubmittedAttribute('DisabilityTypes', DisabilityType)  # type: List[DisabilityType]
    CarePlanTypes = RepeatingSubmittedAttribute('CarePlanTypes', CarePlanType)  # type: List[CarePlanType]
    AssistiveTechnologiesToSupportDisabilityTypes = RepeatingSubmittedAttribute('AssistiveTechnologiesToSupportDisabilityTypes', AssistiveTechnologyToSupportDisabilityType)  # type: List[AssistiveTechnologyToSupportDisabilityType]
    SocialAndPersonalCircumstances = RepeatingSubmittedAttribute('SocialAndPersonalCircumstances', SocialAndPersonalCircumstances)  # type: List[SocialAndPersonalCircumstances]
    OverseasVisitorChargingCategories = RepeatingSubmittedAttribute('OverseasVisitorChargingCategories', OverseasVisitorChargingCategory)  # type: List[OverseasVisitorChargingCategory]
    eMED3FitNotes = RepeatingSubmittedAttribute('eMED3FitNotes', eMED3FitNote)  # type: List[eMED3FitNote]
    MentalHealthActLegalStatusClassificationAssignmentPeriods = RepeatingSubmittedAttribute('MentalHealthActLegalStatusClassificationAssignmentPeriods', MentalHealthActLegalStatusClassificationAssignmentPeriod)  # type: List[MentalHealthActLegalStatusClassificationAssignmentPeriod]
    MedicalHistoryPreviousDiagnoses = RepeatingSubmittedAttribute('MedicalHistoryPreviousDiagnoses', MedicalHistoryPreviousDiagnosis)  # type: List[MedicalHistoryPreviousDiagnosis]
    CPACareEpisodes = RepeatingSubmittedAttribute('CPACareEpisodes', CareProgramApproachCareEpisode)  # type: List[CareProgramApproachCareEpisode]

    # ^^ auto-generated ^^

    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)  # type: SubmittedAttribute


class Referral(_Referral):
    """MHS101Referral"""

    # -- auto-generated --

    __table__ = "MHS101Referral"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    Patient = SubmittedAttribute('Patient', MasterPatientIndex)  # type: MasterPatientIndex
    OtherServiceType = RepeatingSubmittedAttribute('OtherServiceType', OtherServiceType)  # type: List[OtherServiceType]
    OtherReasonsForReferral = RepeatingSubmittedAttribute('OtherReasonsForReferral', OtherReasonForReferral)  # type: List[OtherReasonForReferral]
    ReferralsToTreatment = RepeatingSubmittedAttribute('ReferralsToTreatment', ReferralToTreatment)  # type: List[ReferralToTreatment]
    OnwardReferrals = RepeatingSubmittedAttribute('OnwardReferrals', OnwardReferral)  # type: List[OnwardReferral]
    DischargePlanAgreements = RepeatingSubmittedAttribute('DischargePlanAgreements', DischargePlanAgreement)  # type: List[DischargePlanAgreement]
    CareContacts = RepeatingSubmittedAttribute('CareContacts', CareContact)  # type: List[CareContact]
    IndirectActivities = RepeatingSubmittedAttribute('IndirectActivities', IndirectActivity)  # type: List[IndirectActivity]
    PatSelfDirectedDigitalInterventions = RepeatingSubmittedAttribute('PatSelfDirectedDigitalInterventions', PatSelfDirectedDigitalIntervention)  # type: List[PatSelfDirectedDigitalIntervention]
    HospitalProviderSpells = RepeatingSubmittedAttribute('HospitalProviderSpells', HospitalProviderSpell)  # type: List[HospitalProviderSpell]
    PrimaryDiagnoses = RepeatingSubmittedAttribute('PrimaryDiagnoses', PrimaryDiagnosis)  # type: List[PrimaryDiagnosis]
    SecondaryDiagnoses = RepeatingSubmittedAttribute('SecondaryDiagnoses', SecondaryDiagnosis)  # type: List[SecondaryDiagnosis]
    CodedScoredAssessmentReferrals = RepeatingSubmittedAttribute('CodedScoredAssessmentReferrals', CodedScoredAssessmentReferral)  # type: List[CodedScoredAssessmentReferral]
    PresentingComplaints = RepeatingSubmittedAttribute('PresentingComplaints', PresentingComplaint)  # type: List[PresentingComplaint]

    # ^^ auto-generated ^^

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_Referral.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute


class GroupSession(_GroupSession):
    """MHS301GroupSession"""

    # -- auto-generated --

    __table__ = "MHS301GroupSession"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_GroupSession.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute


class MentalHealthDropInContact(_MentalHealthDropInContact):
    """MHS302MHDropInContact"""

    # -- auto-generated --

    __table__ = "MHS302MHDropInContact"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_MentalHealthDropInContact.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute


class AnonymousSelfAssessment(_AnonymousSelfAssessment):
    """MHS608AnonSelfAssess"""

    # -- auto-generated --

    __table__ = "MHS608AnonSelfAssess"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_AnonymousSelfAssessment.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute


class StaffDetails(_StaffDetails):
    """MHS901StaffDetails"""

    # -- auto-generated --

    __table__ = "MHS901StaffDetails"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_StaffDetails.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute



def get_all_models() -> Generator[Type[BaseMHSDS_V6], None, None]:
    clsmembers = [
        name for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
        if obj.__module__ is __name__
    ]

    for member in clsmembers:
        if not member.startswith('_') and not member.startswith('BaseMHSDS_V6'):
            cls_obj = getattr(sys.modules[__name__], member)
            if safe_issubclass(cls_obj, BaseMHSDS_V6):
                yield cls_obj


def get_anonymous_models():
    return [AnonymousSelfAssessment, GroupSession, MentalHealthDropInContact, ServiceOrTeamDetails, StaffDetails, WardDetails]
