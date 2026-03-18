# pylint: disable=line-too-long,too-many-lines

import inspect
import sys
from datetime import date
from typing import Generator, List, Type

from dsp.datasets.models.mhsds_v5_base import *
from dsp.model.ons_record import ONSRecordPaths
from dsp.common.mhsds_v5_expressions import *
from dsp.common.expressions import *
from dsp.common.structured_model import (
    META,
    SubmittedAttribute,
    RepeatingSubmittedAttribute,
    DerivedAttribute,
    AssignableAttribute,
    MPSConfidenceScores, Decimal_23_0
)
from dsp.shared import safe_issubclass
from dsp.shared.constants import DS
from dsp.common.constants import CCG_ICB_SWITCHOVER_PERIOD_END


__all__ = [
    'AbsenceWithoutLeave',
    'AccommodationStatus',
    'AnonymousSelfAssessment',
    'Assault',
    'AssignedCareProfessional',
    'AssistiveTechnologyToSupportDisabilityType',
    'CareActivity',
    'CareCluster',
    'CareContact',
    'CarePlanAgreement',
    'CarePlanType',
    'CareProgramApproachCareEpisode',
    'CareProgramApproachReview',
    'ClusterAssess',
    'ClusterTool',
    'CodedScoredAssessmentCareActivity',
    'CodedScoredAssessmentReferral',
    'CommunityTreatmentOrder',
    'CommunityTreatmentOrderRecall',
    'ConditionalDischarge',
    'DelayedDischarge',
    'DisabilityType',
    'DischargePlanAgreement',
    'EmploymentStatus',
    'FiveForensicPathways',
    'GP',
    'GroupSession',
    'Header',
    'HomeLeave',
    'HospitalProviderSpell',
    'HospitalProviderSpellCommissionerAssignmentPeriod',
    'IndirectActivity',
    'LeaveOfAbsence',
    'MasterPatientIndex',
    'MedicalHistoryPreviousDiagnosis',
    'MedicationPrescription',
    'MentalHealthActLegalStatusClassificationAssignmentPeriod',
    'MentalHealthCareCoordinator',
    'MentalHealthCurrencyModel',
    'MentalHealthDropInContact',
    'MentalHealthResponsibleClinicianAssignmentPeriod',
    'OnwardReferral',
    'OtherInAttendance',
    'OtherReasonForReferral',
    'OverseasVisitorChargingCategory',
    'PatientIndicators',
    'PoliceAssistanceRequest',
    'PrimaryDiagnosis',
    'ProvisionalDiagnosis',
    'Referral',
    'ReferralToTreatment',
    'RestrictiveInterventionIncident',
    'RestrictiveInterventionType',
    'SecondaryDiagnosis',
    'SelfHarm',
    'ServiceTypeReferredTo',
    'SocialAndPersonalCircumstances',
    'SpecialisedMentalHealthExceptionalPackageOfCare',
    'StaffDetails',
    'SubstanceMisuse',
    'TrialLeave',
    'WardStay',
    'get_all_models',
    'get_anonymous_models'
]


class Header(_Header):
    """MHS000Header"""

    # -- auto-generated --

    __table__ = "MHS000Header"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqSubmissionID = DerivedAttribute(
        'UniqSubmissionID', int, SubmissionId(meta_event_id_expr=Select(_Header.Root.META.EVENT_ID))
    )  # type: DerivedAttribute

    MHS000UniqID = DerivedAttribute(
        'MHS000UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Header.UniqSubmissionID),
            row_number_expr=Select(_Header.RowNumber))
    )  # type: DerivedAttribute

    FileType = DerivedAttribute(
        'FileType', int,
        FileTypeExpressionYTD(
            Select(_Header.Root.META.EVENT_RECEIVED_TS), Select(_Header.ReportingPeriodStartDate), DS.MHSDS_V1_TO_V5_AS_V6
        )
    )  # type: DerivedAttribute


class GP(_GP):
    """MHS002GP"""

    # -- auto-generated --

    __table__ = "MHS002GP"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS002UniqID = DerivedAttribute(
        'MHS002UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_GP.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_GP.RowNumber))
    )  # type: DerivedAttribute

    GPDistanceHome = DerivedAttribute('GPDistanceHome', int, (
        OrgDistance(org_code_expr=Select(_GP.GMPCodeReg), from_postcode_expr=Select(_GP.Parent.Postcode),
                    point_in_time_expr=Select(_GP.Root.Header.ReportingPeriodStartDate))))  # type: DerivedAttribute

    OrgIDCCGGPPractice = DerivedAttribute('OrgIDCCGGPPractice', str, (
        If(
            DateBeforeOrEqualTo(
                eval_expr=(Select(_GP.Root.Header.ReportingPeriodStartDate)), check_date_expr=Literal(CCG_ICB_SWITCHOVER_PERIOD_END)),
            then=(CCGFromGPPracticeCode(
                gp_practice_code=Select(_GP.GMPCodeReg),
                event_date=Select(_GP.Root.Header.ReportingPeriodStartDate),
                enforce_icb_switchover_period=Literal(True))),
            otherwise=Literal(None))))  # type: DerivedAttribute

    OrgIDSubICBLocGP = DerivedAttribute('OrgIDSubICBLocGP', str, SubICBFromGPPracticeCode(
        gp_practice_code=Select(_GP.GMPCodeReg),
        event_date=Select(_GP.Root.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    OrgIDICBGPPractice = DerivedAttribute('OrgIDICBGPPractice', str, ICBFromSubICB(
        sub_icb=Select(_GP.OrgIDSubICBLocGP),
        event_date=Select(_GP.Root.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    LADistrictAuthGPPractice = DerivedAttribute(
        'LADistrictAuthGPPractice',
        str,
        UnitaryAuthorityFromGPPracticeCode(
            gp_practice_code=Select(_GP.GMPCodeReg),
            event_date=Select(_GP.Root.Header.ReportingPeriodStartDate))
    )  # type: DerivedAttribute


class AccommodationStatus(_AccommodationStatus):
    """MHS003AccommStatus"""

    # -- auto-generated --

    __table__ = "MHS003AccommStatus"
    __concrete__ = True

    # ^^ auto-generated ^^

    AgeAccomTypeDate = DerivedAttribute('AgeAccomTypeDate', int,
                                          AgeAtDate(
                                              date_of_birth_expr=Select(_AccommodationStatus.Parent.PersonBirthDate),
                                              date_for_age_expr=Select(_AccommodationStatus.AccommodationTypeDate),
                                              ignore_time=True))  # type: DerivedAttribute

    MHS003UniqID = DerivedAttribute(
        'MHS003UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_AccommodationStatus.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_AccommodationStatus.RowNumber))
    )  # type: DerivedAttribute


class EmploymentStatus(_EmploymentStatus):
    """MHS004EmpStatus"""

    # -- auto-generated --

    __table__ = "MHS004EmpStatus"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS004UniqID = DerivedAttribute(
        'MHS004UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_EmploymentStatus.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_EmploymentStatus.RowNumber))
    )  # type: DerivedAttribute


class PatientIndicators(_PatientIndicators):
    """MHS005PatInd"""

    # -- auto-generated --

    __table__ = "MHS005PatInd"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS005UniqID = DerivedAttribute(
        'MHS005UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_PatientIndicators.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_PatientIndicators.RowNumber))
    )  # type: DerivedAttribute


class MentalHealthCareCoordinator(_MentalHealthCareCoordinator):
    """MHS006MHCareCoord"""

    # -- auto-generated --

    __table__ = "MHS006MHCareCoord"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqCareProfLocalID = DerivedAttribute(
        'UniqCareProfLocalID',
        str,
        Concat(
            [
                Select(_MentalHealthCareCoordinator.Root.Header.OrgIDProvider),
                Select(_MentalHealthCareCoordinator.CareProfLocalId)]
        )
    )  # type: DerivedAttribute

    MHS006UniqID = DerivedAttribute(
        'MHS006UniqID',
        int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_MentalHealthCareCoordinator.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MentalHealthCareCoordinator.RowNumber))
    )  # type: DerivedAttribute

    InactTimeCC = DerivedAttribute('InactTimeCC', date, If(
        NotNull(Select(_MentalHealthCareCoordinator.EndDateAssCareCoord)),
        then=Literal(None),
        otherwise=AddToDate(days_to_add_expr=Literal(1),
                      input_date_expr=Select(_MentalHealthCareCoordinator.Root.Header.ReportingPeriodEndDate))
    ))  # type: DerivedAttribute


class DisabilityType(_DisabilityType):
    """MHS007DisabilityType"""

    # -- auto-generated --

    __table__ = "MHS007DisabilityType"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS007UniqID = DerivedAttribute(
        'MHS007UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_DisabilityType.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_DisabilityType.RowNumber))
    )  # type: DerivedAttribute


class CarePlanAgreement(_CarePlanAgreement):
    """MHS009CarePlanAgreement"""

    # -- auto-generated --

    __table__ = "MHS009CarePlanAgreement"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqCarePlanID = DerivedAttribute(
        'UniqCarePlanID', str,
        Concat([Select(_CarePlanAgreement.Root.Header.OrgIDProvider),
                Select(_CarePlanAgreement.CarePlanID)])
    )  # type: DerivedAttribute

    MHS009UniqID = DerivedAttribute(
        'MHS009UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_CarePlanAgreement.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CarePlanAgreement.RowNumber))
    )  # type: DerivedAttribute


class AssistiveTechnologyToSupportDisabilityType(_AssistiveTechnologyToSupportDisabilityType):
    """MHS010AssTechToSupportDisTyp"""

    # -- auto-generated --

    __table__ = "MHS010AssTechToSupportDisTyp"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS010UniqID = DerivedAttribute(
        'MHS010UniqID',
        int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_AssistiveTechnologyToSupportDisabilityType.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_AssistiveTechnologyToSupportDisabilityType.RowNumber)
        )
    )  # type: DerivedAttribute


class SocialAndPersonalCircumstances(_SocialAndPersonalCircumstances):
    """MHS011SocPerCircumstances"""

    # -- auto-generated --

    __table__ = "MHS011SocPerCircumstances"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS011UniqID = DerivedAttribute(
        'MHS011UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_SocialAndPersonalCircumstances.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_SocialAndPersonalCircumstances.RowNumber)))  # type: DerivedAttribute


class OverseasVisitorChargingCategory(_OverseasVisitorChargingCategory):
    """MHS012OverseasVisitorChargCat"""

    # -- auto-generated --

    __table__ = "MHS012OverseasVisitorChargCat"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS012UniqID = DerivedAttribute(
        'MHS012UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_OverseasVisitorChargingCategory.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_OverseasVisitorChargingCategory.RowNumber)))  # type: DerivedAttribute


class MentalHealthCurrencyModel(_MentalHealthCurrencyModel):
    """MHS013MHCurrencyModel"""

    # -- auto-generated --

    __table__ = "MHS013MHCurrencyModel"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS013UniqID = DerivedAttribute(
        'MHS013UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_MentalHealthCurrencyModel.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MentalHealthCurrencyModel.RowNumber)))  # type: DerivedAttribute


class ServiceTypeReferredTo(_ServiceTypeReferredTo):
    """MHS102ServiceTypeReferredTo"""

    # -- auto-generated --

    __table__ = "MHS102ServiceTypeReferredTo"
    __concrete__ = True

    # ^^ auto-generated ^^

    AgeServReferClosure = DerivedAttribute('AgeServReferClosure', int,
                                           AgeAtDate(
                                               date_of_birth_expr=Select(
                                                   _ServiceTypeReferredTo.Root.Patient.PersonBirthDate),
                                               date_for_age_expr=Select(_ServiceTypeReferredTo.ReferClosureDate),
                                               ignore_time=True))  # type: DerivedAttribute

    AgeServReferRejection = DerivedAttribute('AgeServReferRejection', int,
                                             AgeAtDate(
                                                 date_of_birth_expr=Select(
                                                     _ServiceTypeReferredTo.Root.Patient.PersonBirthDate),
                                                 date_for_age_expr=Select(_ServiceTypeReferredTo.ReferRejectionDate),
                                                 ignore_time=True))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_ServiceTypeReferredTo.Root.Header.OrgIDProvider),
                                             Select(
                                                 _ServiceTypeReferredTo.ServiceRequestId)]))  # type: DerivedAttribute

    UniqCareProfTeamID = DerivedAttribute('UniqCareProfTeamID', str,
                                          Concat([Select(_ServiceTypeReferredTo.Root.Header.OrgIDProvider),
                                                  Select(_ServiceTypeReferredTo.CareProfTeamLocalId)
                                                  ]))  # type: DerivedAttribute

    MHS102UniqID = DerivedAttribute(
        'MHS102UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_ServiceTypeReferredTo.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_ServiceTypeReferredTo.RowNumber))
    )  # type: DerivedAttribute

    InactTimeST = DerivedAttribute('InactTimeST', date, If(
        NotNull(Select(_ServiceTypeReferredTo.ReferClosureDate)),
        then=Literal(None),
        otherwise=AddToDate(
            days_to_add_expr=Literal(1),
            input_date_expr=Select(_ServiceTypeReferredTo.Root.Header.ReportingPeriodEndDate)))
    )  # type: DerivedAttribute

    ServiceTypeName = DerivedAttribute(
        'ServiceTypeName',
        str,
        ServiceTeamType(service_team_ref_expr=Select(_ServiceTypeReferredTo.ServTeamTypeRefToMH))
    )


class OtherReasonForReferral(_OtherReasonForReferral):
    """MHS103OtherReasonReferral"""

    # -- auto-generated --

    __table__ = "MHS103OtherReasonReferral"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_OtherReasonForReferral.Root.Header.OrgIDProvider),
                                             Select(
                                                 _OtherReasonForReferral.ServiceRequestId)]))  # type: DerivedAttribute

    MHS103UniqID = DerivedAttribute(
        'MHS103UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_OtherReasonForReferral.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_OtherReasonForReferral.RowNumber))
    )  # type: DerivedAttribute


class ReferralToTreatment(_ReferralToTreatment):
    """MHS104RTT"""

    # -- auto-generated --

    __table__ = "MHS104RTT"
    __concrete__ = True

    # ^^ auto-generated ^^

    AgeReferTreatStartDate = DerivedAttribute('AgeReferTreatStartDate', int,
                                              AgeAtDate(
                                                  date_of_birth_expr=Select(
                                                      _ReferralToTreatment.Root.Patient.PersonBirthDate),
                                                  date_for_age_expr=Select(
                                                      _ReferralToTreatment.ReferToTreatPeriodStartDate),
                                                  ignore_time=True))  # type: DerivedAttribute

    AgeReferTreatEndDate = DerivedAttribute('AgeReferTreatEndDate', int,
                                            AgeAtDate(
                                                date_of_birth_expr=Select(
                                                    _ReferralToTreatment.Root.Patient.PersonBirthDate),
                                                date_for_age_expr=Select(
                                                    _ReferralToTreatment.ReferToTreatPeriodEndDate),
                                                ignore_time=True))  # type: DerivedAttribute

    TimeReferStartAndEndDate = DerivedAttribute('TimeReferStartAndEndDate', str,
                                                DaysBetween(
                                                    Select(_ReferralToTreatment.ReferToTreatPeriodEndDate),
                                                    Select(_ReferralToTreatment.ReferToTreatPeriodStartDate)
                                                ))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_ReferralToTreatment.Root.Header.OrgIDProvider),
                                             Select(
                                                 _ReferralToTreatment.ServiceRequestId)]))  # type: DerivedAttribute

    MHS104UniqID = DerivedAttribute(
        'MHS104UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_ReferralToTreatment.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_ReferralToTreatment.RowNumber))
    )  # type: DerivedAttribute


class OnwardReferral(_OnwardReferral):
    """MHS105OnwardReferral"""

    # -- auto-generated --

    __table__ = "MHS105OnwardReferral"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute(
        'UniqServReqID',
        str,
        Concat([Select(_OnwardReferral.Root.Header.OrgIDProvider),
                Select(_OnwardReferral.ServiceRequestId)])
    )  # type: DerivedAttribute

    MHS105UniqID = DerivedAttribute(
        'MHS105UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_OnwardReferral.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_OnwardReferral.RowNumber))
    )  # type: DerivedAttribute


class DischargePlanAgreement(_DischargePlanAgreement):
    """MHS106DischargePlanAgreement"""

    # -- auto-generated --

    __table__ = "MHS106DischargePlanAgreement"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS106UniqID = DerivedAttribute(
        'MHS106UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_DischargePlanAgreement.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_DischargePlanAgreement.RowNumber))
    )  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute(
        'UniqServReqID', str,
        Concat([Select(_DischargePlanAgreement.Root.Header.OrgIDProvider),
                Select(_DischargePlanAgreement.ServiceRequestId)]))  # type: DerivedAttribute


class MedicationPrescription(_MedicationPrescription):
    """MHS107MedicationPrescription"""

    # -- auto-generated --

    __table__ = "MHS107MedicationPrescription"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_MedicationPrescription.Root.Header.OrgIDProvider),
                                             Select(
                                                 _MedicationPrescription.ServiceRequestId)]))  # type: DerivedAttribute

    MHS107UniqID = DerivedAttribute(
        'MHS107UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_MedicationPrescription.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MedicationPrescription.RowNumber))
    )  # type: DerivedAttribute


class OtherInAttendance(_OtherInAttendance):
    """MHS203OtherAttend"""

    # -- auto-generated --

    __table__ = "MHS203OtherAttend"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Select(_OtherInAttendance.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqCareContID = DerivedAttribute('UniqCareContID', str,
                                      Concat([Select(_OtherInAttendance.Root.Header.OrgIDProvider),
                                              Select(_OtherInAttendance.CareContactId)
                                              ]))  # type: DerivedAttribute

    MHS203UniqID = DerivedAttribute(
        'MHS203UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_OtherInAttendance.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_OtherInAttendance.RowNumber))
    )  # type: DerivedAttribute


class IndirectActivity(_IndirectActivity):
    """MHS204IndirectActivity"""

    # -- auto-generated --

    __table__ = "MHS204IndirectActivity"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqCareProfLocalID = DerivedAttribute(
        'UniqCareProfLocalID', str,
        Concat([Select(_IndirectActivity.Root.Header.OrgIDProvider),
                Select(_IndirectActivity.CareProfLocalId)]))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute(
        'UniqServReqID',
        str,
        Concat(
            [Select(_IndirectActivity.Root.Header.OrgIDProvider), Select(_IndirectActivity.ServiceRequestId)]
        )
    )  # type: DerivedAttribute

    MHS204UniqID = DerivedAttribute(
        'MHS204UniqID',
        int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_IndirectActivity.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_IndirectActivity.RowNumber)
        )
    )  # type: DerivedAttribute

    AgeAtIndirectActivity = DerivedAttribute(
        'AgeAtIndirectActivity',
        int,
        AgeAtDate(
            date_of_birth_expr=Select(_IndirectActivity.Root.Patient.PersonBirthDate),
            date_for_age_expr=Select(_IndirectActivity.IndirectActDate),
            ignore_time=True
        )
    )  # type: DerivedAttribute

    MapSnomedCTFindingCode = DerivedAttribute('MapSnomedCTFindingCode', str, DefaultNone())  # type: DerivedAttribute
    MapSnomedCTFindingTerm = DerivedAttribute('MapSnomedCTFindingTerm', str, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTFindingCode = DerivedAttribute('MasterSnomedCTFindingCode', int, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTFindingTerm = DerivedAttribute('MasterSnomedCTFindingTerm', str, DefaultNone())  # type: DerivedAttribute
    MapICD10FindingCode = DerivedAttribute('MapICD10FindingCode', str, DefaultNone())  # type: DerivedAttribute
    MapICD10FindingDesc = DerivedAttribute('MapICD10FindingDesc', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10FindingCode = DerivedAttribute('MasterICD10FindingCode', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10FindingDesc = DerivedAttribute('MasterICD10FindingDesc', str, DefaultNone())  # type: DerivedAttribute


class MentalHealthResponsibleClinicianAssignmentPeriod(_MentalHealthResponsibleClinicianAssignmentPeriod):
    """MHS402RespClinicianAssignPeriod"""

    # -- auto-generated --

    __table__ = "MHS402RespClinicianAssignPeriod"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqCareProfLocalID = DerivedAttribute(
        'UniqCareProfLocalID', str,
        Concat([Select(_MentalHealthResponsibleClinicianAssignmentPeriod.Root.Header.OrgIDProvider),
                Select(_MentalHealthResponsibleClinicianAssignmentPeriod.CareProfLocalId)])
    )  # type: DerivedAttribute

    UniqMHActEpisodeID = \
        DerivedAttribute(
            'UniqMHActEpisodeID',
            str,
            Concat(
                [
                    Select(_MentalHealthResponsibleClinicianAssignmentPeriod.Root.Header.OrgIDProvider),
                    Select(_MentalHealthResponsibleClinicianAssignmentPeriod.MHActLegalStatusClassPeriodId),
                ]
            )
        )  # type: DerivedAttribute

    MHS402UniqID = DerivedAttribute(
        'MHS402UniqID',
        int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_MentalHealthResponsibleClinicianAssignmentPeriod.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MentalHealthResponsibleClinicianAssignmentPeriod.RowNumber))
    )  # type: DerivedAttribute


class ConditionalDischarge(_ConditionalDischarge):
    """MHS403ConditionalDischarge"""

    # -- auto-generated --

    __table__ = "MHS403ConditionalDischarge"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqMHActEpisodeID = DerivedAttribute('UniqMHActEpisodeID', str,
                                          Concat([Select(_ConditionalDischarge.Root.Header.OrgIDProvider),
                                                  Select(_ConditionalDischarge.MHActLegalStatusClassPeriodId),
                                                  ]))  # type: DerivedAttribute

    MHS403UniqID = DerivedAttribute(
        'MHS403UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_ConditionalDischarge.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_ConditionalDischarge.RowNumber))
    )  # type: DerivedAttribute


class CommunityTreatmentOrder(_CommunityTreatmentOrder):
    """MHS404CommTreatOrder"""

    # -- auto-generated --

    __table__ = "MHS404CommTreatOrder"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqMHActEpisodeID = DerivedAttribute('UniqMHActEpisodeID', str,
                                          Concat([Select(_CommunityTreatmentOrder.Root.Header.OrgIDProvider),
                                                  Select(_CommunityTreatmentOrder.MHActLegalStatusClassPeriodId),
                                                  ]))  # type: DerivedAttribute

    MHS404UniqID = DerivedAttribute(
        'MHS404UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_CommunityTreatmentOrder.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CommunityTreatmentOrder.RowNumber))
    )  # type: DerivedAttribute


class CommunityTreatmentOrderRecall(_CommunityTreatmentOrderRecall):
    """MHS405CommTreatOrderRecall"""

    # -- auto-generated --

    __table__ = "MHS405CommTreatOrderRecall"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqMHActEpisodeID = DerivedAttribute('UniqMHActEpisodeID', str,
                                          Concat([Select(_CommunityTreatmentOrderRecall.Root.Header.OrgIDProvider),
                                                  Select(_CommunityTreatmentOrderRecall.MHActLegalStatusClassPeriodId),
                                                  ]))  # type: DerivedAttribute

    MHS405UniqID = DerivedAttribute(
        'MHS405UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_CommunityTreatmentOrderRecall.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CommunityTreatmentOrderRecall.RowNumber))
    )  # type: DerivedAttribute


class AssignedCareProfessional(_AssignedCareProfessional):
    """MHS503AssignedCareProf"""

    # -- auto-generated --

    __table__ = "MHS503AssignedCareProf"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqCareProfLocalID = DerivedAttribute(
        'UniqCareProfLocalID', str,
        Concat([Select(_AssignedCareProfessional.Root.Header.OrgIDProvider),
                Select(_AssignedCareProfessional.CareProfLocalId)])
    )  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute(
        'UniqServReqID',
        str,
        Select(_AssignedCareProfessional.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqHospProvSpellID = \
        DerivedAttribute(
            'UniqHospProvSpellID',
            str,
            Concat(
                [Select(_AssignedCareProfessional.Root.Header.OrgIDProvider),
                 Select(_AssignedCareProfessional.HospProvSpellID)]
            )
        )  # type: DerivedAttribute

    MHS503UniqID = DerivedAttribute(
        'MHS503UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AssignedCareProfessional.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_AssignedCareProfessional.RowNumber))
    )  # type: DerivedAttribute


class DelayedDischarge(_DelayedDischarge):
    """MHS504DelayedDischarge"""

    # -- auto-generated --

    __table__ = "MHS504DelayedDischarge"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute(
        'UniqServReqID', str, Select(_DelayedDischarge.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqHospProvSpellID = \
        DerivedAttribute(
            'UniqHospProvSpellID',
            str,
            Concat(
                [
                    Select(_DelayedDischarge.Root.Header.OrgIDProvider),
                    Select(_DelayedDischarge.HospProvSpellID)
                ]
            )
        )  # type: DerivedAttribute

    MHS504UniqID = DerivedAttribute(
        'MHS504UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_DelayedDischarge.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_DelayedDischarge.RowNumber))
    )  # type: DerivedAttribute

    BedDaysDDRP = DerivedAttribute('BedDaysDDRP', int,
                                   DaysBetween(
                                       If(
                                           NotNull(Select(_DelayedDischarge.EndDateDelayDisch)),
                                           then=(Select(_DelayedDischarge.EndDateDelayDisch)),
                                           otherwise=AddToDate(
                                               days_to_add_expr=Literal(1),
                                               input_date_expr=Select(_WardStay.Root.Header.ReportingPeriodEndDate)
                                           )
                                       ),
                                       Max([
                                           Select(_DelayedDischarge.StartDateDelayDisch),
                                           Select(_WardStay.Root.Header.ReportingPeriodStartDate)
                                       ])
                                   )
                                   )  # type: DerivedAttribute


class RestrictiveInterventionType(_RestrictiveInterventionType):
    """MHS515RestrictiveInterventType"""

    # -- auto-generated --

    __table__ = "MHS515RestrictiveInterventType"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS515UniqID = DerivedAttribute(
        'MHS515UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_RestrictiveInterventionType.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_RestrictiveInterventionType.RowNumber)))  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute(
        'UniqHospProvSpellID', str, Select(_RestrictiveInterventionType.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute(
        'UniqServReqID', str, Select(_RestrictiveInterventionType.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqRestrictiveIntIncID = DerivedAttribute(
        'UniqRestrictiveIntIncID', str,
        Concat([Select(_RestrictiveInterventionType.Root.Header.OrgIDProvider),
                Select(_RestrictiveInterventionType.RestrictiveIntIncID)])
    )  # type: DerivedAttribute

    UniqRestrictiveIntTypeID = DerivedAttribute(
        'UniqRestrictiveIntTypeID', str,
        Concat([Select(_RestrictiveInterventionType.Root.Header.OrgIDProvider),
                Select(_RestrictiveInterventionType.RestrictiveIntTypeID)])
    )  # type: DerivedAttribute


class PoliceAssistanceRequest(_PoliceAssistanceRequest):
    """MHS516PoliceAssistanceRequest"""

    # -- auto-generated --

    __table__ = "MHS516PoliceAssistanceRequest"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS516UniqID = DerivedAttribute(
        'MHS516UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_PoliceAssistanceRequest.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_PoliceAssistanceRequest.RowNumber)))  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute(
        'UniqHospProvSpellID', str, Select(_PoliceAssistanceRequest.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute(
        'UniqServReqID', str, Select(_PoliceAssistanceRequest.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute('UniqWardStayID', str, Concat(
        [Select(_PoliceAssistanceRequest.Root.Header.OrgIDProvider),
            Select(_PoliceAssistanceRequest.WardStayId)]))  # type: DerivedAttribute


class Assault(_Assault):
    """MHS506Assault"""

    # -- auto-generated --

    __table__ = "MHS506Assault"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute(
        'UniqServReqID', str, Concat([
            Select(_Assault.Root.Header.OrgIDProvider),
            Select(_Assault.Root.ServiceRequestId)
        ])
    )  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str,
                                           Select(_Assault.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute('UniqWardStayID', str,
                                      Concat([Select(_Assault.Root.Header.OrgIDProvider),
                                              Select(
                                                  _Assault.WardStayId)]))  # type: DerivedAttribute

    MHS506UniqID = DerivedAttribute(
        'MHS506UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Assault.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_Assault.RowNumber))
    )  # type: DerivedAttribute


class SelfHarm(_SelfHarm):
    """MHS507SelfHarm"""

    # -- auto-generated --

    __table__ = "MHS507SelfHarm"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute(
        'UniqServReqID', str, Concat(
            [
                Select(_SelfHarm.Root.Header.OrgIDProvider),
                Select(_SelfHarm.Root.ServiceRequestId)
            ])
    )  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute(
        'UniqHospProvSpellID', str, Select(_SelfHarm.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute(
        'UniqWardStayID', str,
        Concat([
            Select(_SelfHarm.Root.Header.OrgIDProvider),
            Select(_SelfHarm.WardStayId)
        ])
    )  # type: DerivedAttribute

    MHS507UniqID = DerivedAttribute(
        'MHS507UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_SelfHarm.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_SelfHarm.RowNumber))
    )  # type: DerivedAttribute


class HomeLeave(_HomeLeave):
    """MHS509HomeLeave"""

    # -- auto-generated --

    __table__ = "MHS509HomeLeave"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_HomeLeave.Root.Header.OrgIDProvider),
                                             Select(
                                                 _HomeLeave.Root.ServiceRequestId)]))  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str,
                                            Select(_HomeLeave.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute('UniqWardStayID', str,
                                      Concat([Select(_HomeLeave.Root.Header.OrgIDProvider),
                                              Select(
                                                  _HomeLeave.WardStayId)]))  # type: DerivedAttribute

    MHS509UniqID = DerivedAttribute(
        'MHS509UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_HomeLeave.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_HomeLeave.RowNumber)))  # type: DerivedAttribute

    HomeLeaveDaysEndRP = DerivedAttribute(
        'HomeLeaveDaysEndRP', int,
        DaysBetween(
            If(
                NotNull(Select(_HomeLeave.EndDateHomeLeave)),
                then=(Select(_HomeLeave.EndDateHomeLeave)),
                otherwise=AddToDate(
                    days_to_add_expr=Literal(1),
                    input_date_expr=Select(_HomeLeave.Root.Header.ReportingPeriodEndDate)
                )
            ),
            Max([
                Select(_HomeLeave.StartDateHomeLeave),
                Select(_HomeLeave.Root.Header.ReportingPeriodStartDate)
            ])
        )
        )  # type: DerivedAttribute


class LeaveOfAbsence(_LeaveOfAbsence):
    """MHS510LeaveOfAbsence"""

    # -- auto-generated --

    __table__ = "MHS510LeaveOfAbsence"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_LeaveOfAbsence.Root.Header.OrgIDProvider),
                                             Select(
                                                 _LeaveOfAbsence.Root.ServiceRequestId)]))  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str, Select(
        _LeaveOfAbsence.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute('UniqWardStayID', str,
                                      Concat([Select(_LeaveOfAbsence.Root.Header.OrgIDProvider),
                                              Select(
                                                  _LeaveOfAbsence.WardStayId)]))  # type: DerivedAttribute

    MHS510UniqID = DerivedAttribute(
        'MHS510UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_LeaveOfAbsence.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_LeaveOfAbsence.RowNumber))
    )  # type: DerivedAttribute

    LOADaysRP = DerivedAttribute('LOADaysRP', int,
                                 DaysBetween(
                                     If(
                                         NotNull(Select(_LeaveOfAbsence.EndDateMHLeaveAbs)),
                                         then=(Select(_LeaveOfAbsence.EndDateMHLeaveAbs)),
                                         otherwise=AddToDate(
                                             days_to_add_expr=Literal(1),
                                             input_date_expr=Select(_LeaveOfAbsence.Root.Header.ReportingPeriodEndDate)
                                         )
                                     ),
                                     Max([
                                         Select(_LeaveOfAbsence.StartDateMHLeaveAbs),
                                         Select(_LeaveOfAbsence.Root.Header.ReportingPeriodStartDate)
                                     ])
                                 )
                                 )  # type: DerivedAttribute


class AbsenceWithoutLeave(_AbsenceWithoutLeave):
    """MHS511AbsenceWithoutLeave"""

    # -- auto-generated --

    __table__ = "MHS511AbsenceWithoutLeave"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str, Select(
        _AbsenceWithoutLeave.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute('UniqWardStayID', str,
                                      Concat([Select(_AbsenceWithoutLeave.Root.Header.OrgIDProvider),
                                              Select(
                                                  _AbsenceWithoutLeave.WardStayId)]))  # type: DerivedAttribute

    MHS511UniqID = DerivedAttribute(
        'MHS511UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_AbsenceWithoutLeave.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_AbsenceWithoutLeave.RowNumber))
    )  # type: DerivedAttribute

    AWOLDaysEndRP = DerivedAttribute(
        'AWOLDaysEndRP', int,
        DaysBetween(
            If(
                NotNull(Select(_AbsenceWithoutLeave.EndDateMHAbsWOLeave)),
                then=(Select(_AbsenceWithoutLeave.EndDateMHAbsWOLeave)),
                otherwise=AddToDate(
                    days_to_add_expr=Literal(1),
                    input_date_expr=Select(_AbsenceWithoutLeave.Root.Header.ReportingPeriodEndDate)
                )
            ),
            Max([
                Select(_AbsenceWithoutLeave.StartDateMHAbsWOLeave),
                Select(_AbsenceWithoutLeave.Root.Header.ReportingPeriodStartDate)
            ])
        )
    )  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute(
        'UniqServReqID', str,
        Concat([Select(_AbsenceWithoutLeave.Root.Header.OrgIDProvider),
                Select(_AbsenceWithoutLeave.Root.ServiceRequestId)]))  # type: DerivedAttribute


class HospitalProviderSpellCommissionerAssignmentPeriod(_HospitalProviderSpellCommissionerAssignmentPeriod):
    """MHS512HospSpellCommAssPer"""

    # -- auto-generated --

    __table__ = "MHS512HospSpellCommAssPer"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str, Select(
        _HospitalProviderSpellCommissionerAssignmentPeriod.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str, Select(
        _HospitalProviderSpellCommissionerAssignmentPeriod.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    MHS512UniqID = DerivedAttribute(
        'MHS512UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_HospitalProviderSpellCommissionerAssignmentPeriod.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_HospitalProviderSpellCommissionerAssignmentPeriod.RowNumber))
    )  # type: DerivedAttribute


class SubstanceMisuse(_SubstanceMisuse):
    """MHS513SubstanceMisuse"""

    # -- auto-generated --

    __table__ = "MHS513SubstanceMisuse"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_SubstanceMisuse.Root.Header.OrgIDProvider),
                                             Select(
                                                 _SubstanceMisuse.Root.ServiceRequestId)]))  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str, Select(
        _SubstanceMisuse.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute('UniqWardStayID', str,
                                      Concat([Select(_SubstanceMisuse.Root.Header.OrgIDProvider),
                                              Select(
                                                  _SubstanceMisuse.WardStayId)]))  # type: DerivedAttribute

    MHS513UniqID = DerivedAttribute(
        'MHS513UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_SubstanceMisuse.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_SubstanceMisuse.RowNumber))
    )  # type: DerivedAttribute


class TrialLeave(_TrialLeave):
    """MHS514TrialLeave"""

    # -- auto-generated --

    __table__ = "MHS514TrialLeave"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_TrialLeave.Root.Header.OrgIDProvider),
                                             Select(
                                                 _TrialLeave.Root.ServiceRequestId)]))  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str,
                                            Select(_TrialLeave.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute('UniqWardStayID', str,
                                      Concat([Select(_TrialLeave.Root.Header.OrgIDProvider),
                                              Select(
                                                  _TrialLeave.WardStayId)]))  # type: DerivedAttribute

    MHS514UniqID = DerivedAttribute(
        'MHS514UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_TrialLeave.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_TrialLeave.RowNumber))
    )  # type: DerivedAttribute


class SpecialisedMentalHealthExceptionalPackageOfCare(_SpecialisedMentalHealthExceptionalPackageOfCare):
    """MHS517SMHExceptionalPackOfCare"""

    # -- auto-generated --

    __table__ = "MHS517SMHExceptionalPackOfCare"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS517UniqID = DerivedAttribute(
        'MHS517UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_SpecialisedMentalHealthExceptionalPackageOfCare.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_SpecialisedMentalHealthExceptionalPackageOfCare.RowNumber))
    )  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute(
        'UniqHospProvSpellID', str, Concat(
            [Select(_SpecialisedMentalHealthExceptionalPackageOfCare.Root.Header.OrgIDProvider),
                Select(_SpecialisedMentalHealthExceptionalPackageOfCare.HospProvSpellID)]))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute('UniqServReqID', str, Select(
        _SpecialisedMentalHealthExceptionalPackageOfCare.Parent.UniqServReqID))  # type: DerivedAttribute


class MedicalHistoryPreviousDiagnosis(_MedicalHistoryPreviousDiagnosis):
    """MHS601MedHistPrevDiag"""

    # -- auto-generated --

    __table__ = "MHS601MedHistPrevDiag"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS601UniqID = DerivedAttribute(
        'MHS601UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_MedicalHistoryPreviousDiagnosis.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MedicalHistoryPreviousDiagnosis.RowNumber))
    )  # type: DerivedAttribute

    MapSnomedCTPrevDiagCode = DerivedAttribute('MapSnomedCTPrevDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MapSnomedCTPrevDiagTerm = DerivedAttribute('MapSnomedCTPrevDiagTerm', str, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTPrevDiagCode = DerivedAttribute('MasterSnomedCTPrevDiagCode', int, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTPrevDiagTerm = DerivedAttribute('MasterSnomedCTPrevDiagTerm', str, DefaultNone())  # type: DerivedAttribute
    MapICD10PrevDiagCode = DerivedAttribute('MapICD10PrevDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MapICD10PrevDiagDesc = DerivedAttribute('MapICD10PrevDiagDesc', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10PrevDiagCode = DerivedAttribute('MasterICD10PrevDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10PrevDiagDesc = DerivedAttribute('MasterICD10PrevDiagDesc', str, DefaultNone())  # type: DerivedAttribute


class ProvisionalDiagnosis(_ProvisionalDiagnosis):
    """MHS603ProvDiag"""

    # -- auto-generated --

    __table__ = "MHS603ProvDiag"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_ProvisionalDiagnosis.Root.Header.OrgIDProvider),
                                             Select(
                                                 _ProvisionalDiagnosis.ServiceRequestId)]))  # type: DerivedAttribute

    MHS603UniqID = DerivedAttribute(
        'MHS603UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_ProvisionalDiagnosis.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_ProvisionalDiagnosis.RowNumber))
    )  # type: DerivedAttribute

    MapSnomedCTProvDiagCode = DerivedAttribute('MapSnomedCTProvDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MapSnomedCTProvDiagTerm = DerivedAttribute('MapSnomedCTProvDiagTerm', str, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTProvDiagCode = DerivedAttribute('MasterSnomedCTProvDiagCode', int, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTProvDiagTerm = DerivedAttribute('MasterSnomedCTProvDiagTerm', str, DefaultNone())  # type: DerivedAttribute
    MapICD10ProvDiagCode = DerivedAttribute('MapICD10ProvDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MapICD10ProvDiagDesc = DerivedAttribute('MapICD10ProvDiagDesc', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10ProvDiagCode = DerivedAttribute('MasterICD10ProvDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10ProvDiagDesc = DerivedAttribute('MasterICD10ProvDiagDesc', str, DefaultNone())  # type: DerivedAttribute


class PrimaryDiagnosis(_PrimaryDiagnosis):
    """MHS604PrimDiag"""

    # -- auto-generated --

    __table__ = "MHS604PrimDiag"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_PrimaryDiagnosis.Root.Header.OrgIDProvider),
                                             Select(
                                                 _PrimaryDiagnosis.ServiceRequestId)]))  # type: DerivedAttribute

    MHS604UniqID = DerivedAttribute(
        'MHS604UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_PrimaryDiagnosis.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_PrimaryDiagnosis.RowNumber))
    )  # type: DerivedAttribute

    MapSnomedCTPrimDiagCode = DerivedAttribute('MapSnomedCTPrimDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MapSnomedCTPrimDiagTerm = DerivedAttribute('MapSnomedCTPrimDiagTerm', str, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTPrimDiagCode = DerivedAttribute('MasterSnomedCTPrimDiagCode', int, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTPrimDiagTerm = DerivedAttribute('MasterSnomedCTPrimDiagTerm', str, DefaultNone())  # type: DerivedAttribute
    MapICD10PrimDiagCode = DerivedAttribute('MapICD10PrimDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MapICD10PrimDiagDesc = DerivedAttribute('MapICD10PrimDiagDesc', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10PrimDiagCode = DerivedAttribute('MasterICD10PrimDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10PrimDiagDesc = DerivedAttribute('MasterICD10PrimDiagDesc', str, DefaultNone())  # type: DerivedAttribute


class SecondaryDiagnosis(_SecondaryDiagnosis):
    """MHS605SecDiag"""

    # -- auto-generated --

    __table__ = "MHS605SecDiag"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_SecondaryDiagnosis.Root.Header.OrgIDProvider),
                                             Select(
                                                 _SecondaryDiagnosis.ServiceRequestId)]))  # type: DerivedAttribute

    MHS605UniqID = DerivedAttribute(
        'MHS605UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_SecondaryDiagnosis.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_SecondaryDiagnosis.RowNumber))
    )  # type: DerivedAttribute

    MapSnomedCTSecDiagCode = DerivedAttribute('MapSnomedCTSecDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MapSnomedCSecDiagTerm = DerivedAttribute('MapSnomedCSecDiagTerm', str, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTSecDiagCode = DerivedAttribute('MasterSnomedCTSecDiagCode', int, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTSecDiagTerm = DerivedAttribute('MasterSnomedCTSecDiagTerm', str, DefaultNone())  # type: DerivedAttribute
    MapICD10SecDiagCode = DerivedAttribute('MapICD10SecDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MapICD10SecDiagDesc = DerivedAttribute('MapICD10SecDiagDesc', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10SecDiagCode = DerivedAttribute('MasterICD10SecDiagCode', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10SecDiagDesc = DerivedAttribute('MasterICD10SecDiagDesc', str, DefaultNone())  # type: DerivedAttribute


class CodedScoredAssessmentReferral(_CodedScoredAssessmentReferral):
    """MHS606CodedScoreAssessmentRefer"""

    # -- auto-generated --

    __table__ = "MHS606CodedScoreAssessmentRefer"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqCareProfLocalID = DerivedAttribute(
        'UniqCareProfLocalID', str,
        Concat([Select(_CodedScoredAssessmentReferral.Root.Header.OrgIDProvider),
                Select(_CodedScoredAssessmentReferral.CareProfLocalId)])
    )  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute(
        'UniqServReqID',
        str,
        Concat(
            [
                Select(_CodedScoredAssessmentReferral.Root.Header.OrgIDProvider),
                Select(_CodedScoredAssessmentReferral.ServiceRequestId)
            ]
        )
    )  # type: DerivedAttribute

    MHS606UniqID = DerivedAttribute(
        'MHS606UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessmentReferral.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CodedScoredAssessmentReferral.RowNumber))
    )  # type: DerivedAttribute

    AgeAssessToolReferCompDate = DerivedAttribute(
        'AgeAssessToolReferCompDate',
        int,
        AgeAtDate(
            date_of_birth_expr=Select(_CodedScoredAssessmentReferral.Root.Patient.PersonBirthDate),
            date_for_age_expr=NaiveDatetimeFromTimestampString(timestamp_str_exp=Select(_CodedScoredAssessmentReferral.AssToolCompTimestamp)),
            ignore_time=True
        )
    )  # type: DerivedAttribute


class CodedScoredAssessmentCareActivity(_CodedScoredAssessmentCareActivity):
    """MHS607CodedScoreAssessmentAct"""

    # -- auto-generated --

    __table__ = "MHS607CodedScoreAssessmentAct"
    __concrete__ = True

    # ^^ auto-generated ^^

    AgeAssessToolCont = \
        DerivedAttribute(
            'AgeAssessToolCont',
            int,
            EvaluateIfValue(
                [Select(_CodedScoredAssessmentCareActivity.Parent.CodeObs)],
                AgeAtDate(
                    date_of_birth_expr=Select(_CodedScoredAssessmentCareActivity.Root.Patient.PersonBirthDate),
                    date_for_age_expr=Select(_CodedScoredAssessmentCareActivity.Parent.Parent.CareContDate),
                    ignore_time=True)))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute('UniqServReqID', str, Select(
        _CodedScoredAssessmentCareActivity.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqCareContID = DerivedAttribute('UniqCareContID', str, Select(
        _CodedScoredAssessmentCareActivity.Parent.UniqCareContID))  # type: DerivedAttribute

    UniqCareActID = DerivedAttribute('UniqCareActID', str,
                                     Concat([Select(_CodedScoredAssessmentCareActivity.Root.Header.OrgIDProvider),
                                             Select(_CodedScoredAssessmentCareActivity.CareActId)
                                             ]))  # type: DerivedAttribute

    MHS607UniqID = DerivedAttribute(
        'MHS607UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessmentCareActivity.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CodedScoredAssessmentCareActivity.RowNumber))
    )  # type: DerivedAttribute


class CareProgramApproachReview(_CareProgramApproachReview):
    """MHS702CPAReview"""

    # -- auto-generated --

    __table__ = "MHS702CPAReview"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqCareProfLocalID = \
        DerivedAttribute(
            'UniqCareProfLocalID',
            str,
            Concat(
                [
                    Select(_CareProgramApproachReview.Root.Header.OrgIDProvider),
                    Select(_CareProgramApproachReview.CareProfLocalId)]))  # type: DerivedAttribute

    UniqCPAEpisodeID = \
        DerivedAttribute(
            'UniqCPAEpisodeID',
            str,
            Concat(
                [
                    Select(_CareProgramApproachReview.Root.Header.OrgIDProvider),
                    Select(_CareProgramApproachReview.CPAEpisodeId)]
            )
        )  # type: DerivedAttribute

    MHS702UniqID = DerivedAttribute(
        'MHS702UniqID',
        int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareProgramApproachReview.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareProgramApproachReview.RowNumber))
    )  # type: DerivedAttribute


class ClusterAssess(_ClusterAssess):
    """MHS802ClusterAssess"""

    # -- auto-generated --

    __table__ = "MHS802ClusterAssess"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqClustID = DerivedAttribute(
        'UniqClustID', str, Concat([Select(_ClusterAssess.Root.Header.OrgIDProvider),
                                    Select(_ClusterAssess.ClustId)]))  # type: DerivedAttribute

    MHS802UniqID = DerivedAttribute(
        'MHS802UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_ClusterAssess.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_ClusterAssess.RowNumber))
    )  # type: DerivedAttribute


class CareCluster(_CareCluster):
    """MHS803CareCluster"""

    # -- auto-generated --

    __table__ = "MHS803CareCluster"
    __concrete__ = True

    # ^^ auto-generated ^^

    ClusterDaysRP = \
        DerivedAttribute('ClusterDaysRP', int,
            DaysBetween(
                If(
                    NotNull(Select(_CareCluster.EndDateCareClust)),
                    then=Select(_CareCluster.EndDateCareClust),
                    otherwise=AddToDate(days_to_add_expr=Literal(1),
                                        input_date_expr=Select(_CareCluster.Root.Header.ReportingPeriodEndDate))
                ),
                Max([
                    Select(_CareCluster.Root.Header.ReportingPeriodStartDate),
                    Select(_CareCluster.StartDateCareClust),
                ])
            )
        )  # type: DerivedAttribute

    ClusterEndRPFlag = DerivedAttribute('ClusterEndRPFlag',
                                        bool,
                                        If(
                                            DateInBetween(
                                                Select(_CareCluster.EndDateCareClust),
                                                Select(_CareCluster.Root.Header.ReportingPeriodStartDate),
                                                Select(_CareCluster.Root.Header.ReportingPeriodEndDate)
                                            ),
                                            then=Literal(True),
                                            otherwise=Literal(False)
                                        )
                                        )  # type: DerivedAttribute

    ClusterStartRPFlag = DerivedAttribute('ClusterStartRPFlag',
                                          bool,
                                          If(
                                              DateInBetween(
                                                  Select(_CareCluster.StartDateCareClust),
                                                  Select(_CareCluster.Root.Header.ReportingPeriodStartDate),
                                                  Select(_CareCluster.Root.Header.ReportingPeriodEndDate)
                                              ),
                                              then=Literal(True),
                                              otherwise=Literal(False)
                                          )
                                          )  # type: DerivedAttribute

    ClusterOpenEndRPFlag = DerivedAttribute('ClusterOpenEndRPFlag',
                                            bool,
                                            If(
                                                NotNull(Select(_CareCluster.EndDateCareClust)),
                                                then=Literal(False),
                                                otherwise=Literal(True)
                                            )
                                            )  # type: DerivedAttribute

    UniqClustID = DerivedAttribute('UniqClustID', str,
                                   Concat([Select(_CareCluster.Root.Header.OrgIDProvider),
                                           Select(
                                               _CareCluster.ClustId)]))  # type: DerivedAttribute

    MHS803UniqID = DerivedAttribute(
        'MHS803UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareCluster.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareCluster.RowNumber))
    )  # type: DerivedAttribute

    InactTimeCC = DerivedAttribute('InactTimeCC', date, If(
        NotNull(Select(_CareCluster.EndDateCareClust)),
        then=Literal(None),
        otherwise=AddToDate(days_to_add_expr=Literal(1),
                            input_date_expr=Select(_CareCluster.Root.Header.ReportingPeriodEndDate))
    ))  # type: DerivedAttribute


class FiveForensicPathways(_FiveForensicPathways):
    """MHS804FiveForensicPathways"""

    # -- auto-generated --

    __table__ = "MHS804FiveForensicPathways"
    __concrete__ = True

    # ^^ auto-generated ^^

    MHS804UniqID = DerivedAttribute(
        'MHS804UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_FiveForensicPathways.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_FiveForensicPathways.RowNumber)
        )
    )  # type: DerivedAttribute


class ClusterTool(_ClusterTool):
    """MHS801ClusterTool"""

    # -- auto-generated --

    __table__ = "MHS801ClusterTool"
    __concrete__ = True

    ClusterAssesses = RepeatingSubmittedAttribute('ClusterAssesses', ClusterAssess)  # type: List[ClusterAssess]
    CareClusters = RepeatingSubmittedAttribute('CareClusters', CareCluster)  # type: List[CareCluster]

    # ^^ auto-generated ^^

    UniqClustID = DerivedAttribute('UniqClustID', str,
                                   Concat([Select(_ClusterTool.Root.Header.OrgIDProvider),
                                           Select(
                                               _ClusterTool.ClustId)]))  # type: DerivedAttribute

    MHS801UniqID = DerivedAttribute(
        'MHS801UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_ClusterTool.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_ClusterTool.RowNumber)
        )
    )  # type: DerivedAttribute


class CareProgramApproachCareEpisode(_CareProgramApproachCareEpisode):
    """MHS701CPACareEpisode"""

    # -- auto-generated --

    __table__ = "MHS701CPACareEpisode"
    __concrete__ = True

    CPAReviews = RepeatingSubmittedAttribute('CPAReviews', CareProgramApproachReview)  # type: List[CareProgramApproachReview]

    # ^^ auto-generated ^^

    UniqCPAEpisodeID = \
        DerivedAttribute(
            'UniqCPAEpisodeID',
            str,
            Concat([Select(_CareProgramApproachCareEpisode.Root.Header.OrgIDProvider),
                    Select(_CareProgramApproachCareEpisode.CPAEpisodeId)]
                   )
        )  # type: DerivedAttribute

    MHS701UniqID = DerivedAttribute(
        'MHS701UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareProgramApproachCareEpisode.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareProgramApproachCareEpisode.RowNumber))
    )  # type: DerivedAttribute


class RestrictiveInterventionIncident(_RestrictiveInterventionIncident):
    """MHS505RestrictiveInterventInc"""

    # -- auto-generated --

    __table__ = "MHS505RestrictiveInterventInc"
    __concrete__ = True

    RestrictiveInterventionTypes = RepeatingSubmittedAttribute('RestrictiveInterventionTypes', RestrictiveInterventionType)  # type: List[RestrictiveInterventionType]

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute(
        'UniqServReqID', str,
        Concat(
            [
                Select(_RestrictiveInterventionIncident.Root.Header.OrgIDProvider),
                Select(_RestrictiveInterventionIncident.Root.ServiceRequestId)
            ]
        )
    )  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str, Select(
        _RestrictiveInterventionIncident.Parent.UniqHospProvSpellID))  # type: DerivedAttribute

    MHS505UniqID = DerivedAttribute(
        'MHS505UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_RestrictiveInterventionIncident.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_RestrictiveInterventionIncident.RowNumber))
    )  # type: DerivedAttribute

    UniqRestrictiveIntIncID = DerivedAttribute(
        'UniqRestrictiveIntIncID', str,
        Concat([Select(_RestrictiveInterventionIncident.Root.Header.OrgIDProvider),
                Select(_RestrictiveInterventionIncident.RestrictiveIntIncID)])
    )  # type: DerivedAttribute


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

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Select(_WardStay.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqHospProvSpellID = DerivedAttribute('UniqHospProvSpellID', str,
                                            Concat([Select(_WardStay.Root.Header.OrgIDProvider),
                                                    Select(
                                                        _WardStay.HospProvSpellID)]))  # type: DerivedAttribute

    UniqWardStayID = DerivedAttribute('UniqWardStayID', str,
                                      Concat([Select(_WardStay.Root.Header.OrgIDProvider),
                                              Select(
                                                  _WardStay.WardStayId)]))  # type: DerivedAttribute

    MHS502UniqID = DerivedAttribute(
        'MHS502UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_WardStay.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_WardStay.RowNumber))
    )  # type: DerivedAttribute

    WardLocDistanceHome = DerivedAttribute('WardLocDistanceHome', int,
                                           (OrgDistance(org_code_expr=Select(_WardStay.SiteIDOfTreat),
                                                        from_postcode_expr=Select(_WardStay.Root.Patient.Postcode),
                                                        point_in_time_expr=Select(
                                                            _WardStay.Root.Header.ReportingPeriodStartDate
                                                        )
                                                        )
                                            ))  # type: DerivedAttribute

    InactTimeWS = DerivedAttribute('InactTimeWS', date, If(
        NotNull(Select(_WardStay.EndDateWardStay)),
        then=Literal(None),
        otherwise=AddToDate(
            days_to_add_expr=Literal(1),
            input_date_expr=Select(_WardStay.Root.Header.ReportingPeriodEndDate)))
    )  # type: DerivedAttribute

    BedDaysWSEndRP = DerivedAttribute('BedDaysWSEndRP', int,
                                      DaysBetween(
                                          If(
                                              NotNull(Select(_WardStay.EndDateWardStay)),
                                              then=(Select(_WardStay.EndDateWardStay)),
                                              otherwise=AddToDate(
                                                  days_to_add_expr=Literal(1),
                                                  input_date_expr=Select(_WardStay.Root.Header.ReportingPeriodEndDate)
                                              )
                                          ),
                                          Max([
                                              Select(_WardStay.StartDateWardStay),
                                              Select(_WardStay.Root.Header.ReportingPeriodStartDate)
                                          ])
                                      )
                                      )  # type: DerivedAttribute

    BedTypeAdultDischarge = DerivedAttribute('BedTypeAdultDischarge', str,
                                             BedTypeAdultDischargeExpression(
                                                 rp_start_date=Select(_WardStay.Root.Header.ReportingPeriodStartDate),
                                                 rp_end_date=Select(_WardStay.Root.Header.ReportingPeriodEndDate),
                                                 care_prof_expr=Select(_WardStay.Parent.AssignedCareProfessionals),
                                                 end_date_ws=Select(_WardStay.EndDateWardStay),
                                                 ward_age_expr=Select(_WardStay.WardAge),
                                                 ward_type_expr=Select(_WardStay.WardType),
                                                 ward_sec_level_expr=Select(_WardStay.WardSecLevel),
                                                 intend_clin_code_mh_expr=Select(_WardStay.IntendClinCareIntenCodeMH)
                                             ))  # type: DerivedAttribute

    BedTypeAdultEndRP = DerivedAttribute('BedTypeAdultEndRP', str,
                                         BedTypeAdultEndRPExpression(
                                             rp_start_date=Select(_WardStay.Root.Header.ReportingPeriodStartDate),
                                             rp_end_date=Select(_WardStay.Root.Header.ReportingPeriodEndDate),
                                             care_prof_expr=Select(_WardStay.Parent.AssignedCareProfessionals),
                                             end_date_ws=Select(_WardStay.EndDateWardStay),
                                             ward_age_expr=Select(_WardStay.WardAge),
                                             ward_type_expr=Select(_WardStay.WardType),
                                             ward_sec_level_expr=Select(_WardStay.WardSecLevel),
                                             intend_clin_code_mh_expr=Select(_WardStay.IntendClinCareIntenCodeMH)
                                         ))  # type: DerivedAttribute

    SpecialisedMHServiceName = DerivedAttribute('SpecialisedMHServiceName', str,
                                                SpecialisedMHService(service_code=Select(_WardStay.SpecialisedMHServiceCode))
                                                ) # type: DerivedAttribute

    WardSayBedTypesLkup = DerivedAttribute('WardSayBedTypesLkup', int,
                                           WardStayBedType(hosp_id=Select(_WardStay.HospProvSpellID),
                                                           ward_sec=Select(_WardStay.WardSecLevel),
                                                           clin_care_code=Select(_WardStay.IntendClinCareIntenCodeMH),
                                                           ward_type=Select(_WardStay.WardType),
                                                           care_prof_expr=Select(_WardStay.Parent.AssignedCareProfessionals)))

    HospitalBedTypeName = DerivedAttribute('HospitelBedTypeName', str,
                                           NationalCodeDescription(national_code=Select(_WardStay.HospitalBedTypeMH))
                                           ) # type: DerivedAttribute

    WardStayServiceAreasOpenEndRPLDA = DerivedAttribute('WardStayServiceAreasOpenEndRPLDA', bool, DefaultNone()
                                                        ) # type: DerivedAttribute

    WardStayServiceAreasStartingInRPLDA = DerivedAttribute('WardStayServiceAreasStartingInRPLDA', bool, DefaultNone()
                                                           ) # type: DerivedAttribute


class HospitalProviderSpell(_HospitalProviderSpell):
    """MHS501HospProvSpell"""

    # -- auto-generated --

    __table__ = "MHS501HospProvSpell"
    __concrete__ = True

    WardStays = RepeatingSubmittedAttribute('WardStays', WardStay)  # type: List[WardStay]
    AssignedCareProfessionals = RepeatingSubmittedAttribute('AssignedCareProfessionals', AssignedCareProfessional)  # type: List[AssignedCareProfessional]
    DelayedDischarges = RepeatingSubmittedAttribute('DelayedDischarges', DelayedDischarge)  # type: List[DelayedDischarge]
    RestrictiveInterventionIncidents = RepeatingSubmittedAttribute('RestrictiveInterventionIncidents', RestrictiveInterventionIncident)  # type: List[RestrictiveInterventionIncident]
    HospitalProviderSpellCommissionersAssignmentPeriods = RepeatingSubmittedAttribute('HospitalProviderSpellCommissionersAssignmentPeriods', HospitalProviderSpellCommissionerAssignmentPeriod)  # type: List[HospitalProviderSpellCommissionerAssignmentPeriod]
    SpecialisedMentalHealthExceptionalPackageOfCares = RepeatingSubmittedAttribute('SpecialisedMentalHealthExceptionalPackageOfCares', SpecialisedMentalHealthExceptionalPackageOfCare)  # type: List[SpecialisedMentalHealthExceptionalPackageOfCare]

    # ^^ auto-generated ^^

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Concat([Select(_HospitalProviderSpell.Root.Header.OrgIDProvider),
                                             Select(
                                                 _HospitalProviderSpell.ServiceRequestId)]))  # type: DerivedAttribute

    UniqHospProvSpellID = \
        DerivedAttribute(
            'UniqHospProvSpellID',
            str,
            Concat(
                [
                    Select(_HospitalProviderSpell.Root.Header.OrgIDProvider),
                    Select(_HospitalProviderSpell.HospProvSpellID)]
            )
        )  # type: DerivedAttribute

    MHS501UniqID = DerivedAttribute(
        'MHS501UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_HospitalProviderSpell.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_HospitalProviderSpell.RowNumber))
    )  # type: DerivedAttribute

    PostcodeDistrictMainVisitor = DerivedAttribute('PostcodeDistrictMainVisitor', str, (
        DerivePostcodeDistrict(postcode_expr=Select(_HospitalProviderSpell.PostcodeMainVisitor),
                               point_in_time_expr=Select(_HospitalProviderSpell.Root.Header.ReportingPeriodStartDate))
    ))  # type: DerivedAttribute

    PostcodeDistrictDischDest = DerivedAttribute('PostcodeDistrictDischDest', str, (
        DerivePostcodeDistrict(postcode_expr=Select(_HospitalProviderSpell.PostcodeDischDestHospProvSpell),
                               point_in_time_expr=Select(_HospitalProviderSpell.Root.Header.ReportingPeriodStartDate))
    ))  # type: DerivedAttribute

    InactTimeHPS = DerivedAttribute('InactTimeHPS', date, If(
        NotNull(Select(_HospitalProviderSpell.DischDateHospProvSpell)),
        then=Literal(None),
        otherwise=AddToDate(
            days_to_add_expr=Literal(1),
            input_date_expr=Select(_HospitalProviderSpell.Root.Header.ReportingPeriodEndDate)))
    )  # type: DerivedAttribute

    LOSDischHosSpell = DerivedAttribute('LOSDischHosSpell', int,
                                        DaysBetween(
                                            Select(_HospitalProviderSpell.DischDateHospProvSpell),
                                            Select(_HospitalProviderSpell.StartDateHospProvSpell)
                                        ))  # type: DerivedAttribute

    LOSHosSpellEoRP = DerivedAttribute('LOSHosSpellEoRP', int,
                                       DaysBetween(
                                           Select(_HospitalProviderSpell.Root.Header.ReportingPeriodEndDate),
                                           Select(_HospitalProviderSpell.StartDateHospProvSpell)
                                       ))  # type: DerivedAttribute

    TimeEstDischDate = DerivedAttribute('TimeEstDischDate', int,
                                        DaysBetween(
                                            Select(_HospitalProviderSpell.EstimatedDischDateHospProvSpell),
                                            Select(_HospitalProviderSpell.StartDateHospProvSpell),
                                            only_positive_results=True
                                        ))  # type: DerivedAttribute

    TimePlanDischDate = DerivedAttribute('TimePlanDischDate', int,
                                         DaysBetween(
                                             Select(_HospitalProviderSpell.PlannedDischDateHospProvSpell),
                                             Select(_HospitalProviderSpell.StartDateHospProvSpell),
                                             only_positive_results=True
                                         ))  # type: DerivedAttribute


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

    UniqMHActEpisodeID = \
        DerivedAttribute(
            'UniqMHActEpisodeID',
            str,
            Concat(
                [
                    Select(_MentalHealthActLegalStatusClassificationAssignmentPeriod.Root.Header.OrgIDProvider),
                    Select(_MentalHealthActLegalStatusClassificationAssignmentPeriod.MHActLegalStatusClassPeriodId),
                ]
            )
        )  # type: DerivedAttribute

    MHS401UniqID = DerivedAttribute(
            'MHS401UniqID',
            int,
            RecordNumberOrUniqueID(
                submission_id_expr=Select(_MentalHealthActLegalStatusClassificationAssignmentPeriod.Root.Header.UniqSubmissionID),
                row_number_expr=Select(_MentalHealthActLegalStatusClassificationAssignmentPeriod.RowNumber)
            )
        )  # type: DerivedAttribute

    InactTimeMHAPeriod = DerivedAttribute('InactTimeMHAPeriod', date, If(
        NotNull(Select(_MentalHealthActLegalStatusClassificationAssignmentPeriod.
                       EndDateMHActLegalStatusClass)),
        then=Literal(None),
        otherwise=AddToDate(
            days_to_add_expr=Literal(1),
            input_date_expr=Select(_MentalHealthActLegalStatusClassificationAssignmentPeriod.
                                   Root.Header.ReportingPeriodEndDate)))
    )  # type: DerivedAttribute

    NHSDLegalStatus = DerivedAttribute(
        'NHSDLegalStatus', str, NHSDLegalStatusExpression(
            Select(_MentalHealthActLegalStatusClassificationAssignmentPeriod.LegalStatusCode))
    )  # type: DerivedAttribute


class CareActivity(_CareActivity):
    """MHS202CareActivity"""

    # -- auto-generated --

    __table__ = "MHS202CareActivity"
    __concrete__ = True

    CodedScoredAssessmentCareActivities = RepeatingSubmittedAttribute('CodedScoredAssessmentCareActivities', CodedScoredAssessmentCareActivity)  # type: List[CodedScoredAssessmentCareActivity]

    # ^^ auto-generated ^^

    UniqCareProfLocalID = DerivedAttribute('UniqCareProfLocalID', str,
                                           Concat([Select(_CareActivity.Root.Header.OrgIDProvider),
                                                   Select(_CareActivity.CareProfLocalId)]))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute('UniqServReqID', str,
                                     Select(_CareActivity.Parent.UniqServReqID))  # type: DerivedAttribute

    UniqCareContID = DerivedAttribute('UniqCareContID', str,
                                      Concat([Select(_CareActivity.Root.Header.OrgIDProvider),
                                              Select(_CareActivity.CareContactId)
                                              ]))  # type: DerivedAttribute

    UniqCareActID = DerivedAttribute('UniqCareActID', str,
                                     Concat([Select(_CareActivity.Root.Header.OrgIDProvider),
                                             Select(_CareActivity.CareActId)
                                             ]))  # type: DerivedAttribute

    MHS202UniqID = DerivedAttribute(
        'MHS202UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareActivity.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareActivity.RowNumber))
    )  # type: DerivedAttribute

    MapSnomedCTFindingCode = DerivedAttribute('MapSnomedCTFindingCode', str, DefaultNone())  # type: DerivedAttribute
    MapSnomedCTFindingTerm = DerivedAttribute('MapSnomedCTFindingTerm', str, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTFindingCode = DerivedAttribute('MasterSnomedCTFindingCode', int, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTFindingTerm = DerivedAttribute('MasterSnomedCTFindingTerm', str, DefaultNone())  # type: DerivedAttribute
    MapICD10FindingCode = DerivedAttribute('MapICD10FindingCode', str, DefaultNone())  # type: DerivedAttribute
    MapICD10FindingDesc = DerivedAttribute('MapICD10FindingDesc', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10FindingCode = DerivedAttribute('MasterICD10FindingCode', str, DefaultNone())  # type: DerivedAttribute
    MasterICD10FindingDesc = DerivedAttribute('MasterICD10FindingDesc', str, DefaultNone())  # type: DerivedAttribute
    MapSnomedCTObsCode = DerivedAttribute('MapSnomedCTObsCode', str, DefaultNone())  # type: DerivedAttribute
    MapSnomedCTObsTerm = DerivedAttribute('MapSnomedCTObsTerm', str, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTObsCode = DerivedAttribute('MasterSnomedCTObsCode', str, DefaultNone())  # type: DerivedAttribute
    MasterSnomedCTObsTerm = DerivedAttribute('MasterSnomedCTObsTerm', str, DefaultNone())  # type: DerivedAttribute


class CareContact(_CareContact):
    """MHS201CareContact"""

    # -- auto-generated --

    __table__ = "MHS201CareContact"
    __concrete__ = True

    CareActivities = RepeatingSubmittedAttribute('CareActivities', CareActivity)  # type: List[CareActivity]
    OtherAttendances = RepeatingSubmittedAttribute('OtherAttendances', OtherInAttendance)  # type: List[OtherInAttendance]

    # ^^ auto-generated ^^

    AgeCareContDate = DerivedAttribute('AgeCareContDate', int,
                                       AgeAtDate(
                                           date_of_birth_expr=Select(
                                               _CareContact.Root.Patient.PersonBirthDate),
                                           date_for_age_expr=Select(
                                               _CareContact.CareContDate),
                                           ignore_time=True))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute(
        'UniqServReqID',
        str,
        Concat(
            [
                Select(_CareContact.Root.Header.OrgIDProvider),
                Select(_CareContact.ServiceRequestId)]))  # type: DerivedAttribute

    UniqCareProfTeamID = DerivedAttribute(
        'UniqCareProfTeamID', str,
        Concat([Select(_CareContact.Root.Header.OrgIDProvider),
                Select(_CareContact.CareProfTeamLocalId)])
    )  # type: DerivedAttribute

    UniqCareContID = DerivedAttribute('UniqCareContID', str,
                                      Concat([Select(_CareContact.Root.Header.OrgIDProvider),
                                              Select(_CareContact.CareContactId)
                                              ]))  # type: DerivedAttribute

    MHS201UniqID = DerivedAttribute(
        'MHS201UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareContact.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareContact.RowNumber)
        )
    )  # type: DerivedAttribute

    ContLocDistanceHome = DerivedAttribute(
        'ContLocDistanceHome', int,
        (OrgDistance(
            org_code_expr=Select(_CareContact.SiteIDOfTreat),
            from_postcode_expr=Select(_CareContact.Root.Patient.Postcode),
            point_in_time_expr=Select(_CareContact.Root.Header.ReportingPeriodStartDate)
        ))
    )  # type: DerivedAttribute

    TimeReferAndCareContact = DerivedAttribute('TimeReferAndCareContact', int,
                                               DaysBetween(
                                                   Select(_CareContact.CareContDate),
                                                   Select(_CareContact.Root.ReferralRequestReceivedDate)
                                               ))  # type: DerivedAttribute


class CarePlanType(_CarePlanType):
    """MHS008CarePlanType"""

    # -- auto-generated --

    __table__ = "MHS008CarePlanType"
    __concrete__ = True

    CarePlanAgreements = RepeatingSubmittedAttribute('CarePlanAgreements', CarePlanAgreement)  # type: List[CarePlanAgreement]

    # ^^ auto-generated ^^

    UniqCarePlanID = DerivedAttribute(
        'UniqCarePlanID',
        str,
        Concat(
            [Select(_CarePlanType.Root.Header.OrgIDProvider),
             Select(_CarePlanType.CarePlanID)]
        )
    )  # type: DerivedAttribute

    MHS008UniqID = DerivedAttribute(
        'MHS008UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CarePlanType.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CarePlanType.RowNumber)))  # type: DerivedAttribute


class MasterPatientIndex(_MasterPatientIndex):
    """MHS001MPI"""

    # -- auto-generated --

    __table__ = "MHS001MPI"
    __concrete__ = True

    GPs = RepeatingSubmittedAttribute('GPs', GP)  # type: List[GP]
    AccommodationStatuses = RepeatingSubmittedAttribute('AccommodationStatuses', AccommodationStatus)  # type: List[AccommodationStatus]
    EmploymentStatuses = RepeatingSubmittedAttribute('EmploymentStatuses', EmploymentStatus)  # type: List[EmploymentStatus]
    PatientIndicators = RepeatingSubmittedAttribute('PatientIndicators', PatientIndicators)  # type: List[PatientIndicators]
    MentalHealthCareCoordinators = RepeatingSubmittedAttribute('MentalHealthCareCoordinators', MentalHealthCareCoordinator)  # type: List[MentalHealthCareCoordinator]
    DisabilityTypes = RepeatingSubmittedAttribute('DisabilityTypes', DisabilityType)  # type: List[DisabilityType]
    CarePlanTypes = RepeatingSubmittedAttribute('CarePlanTypes', CarePlanType)  # type: List[CarePlanType]
    AssistiveTechnologiesToSupportDisabilityTypes = RepeatingSubmittedAttribute('AssistiveTechnologiesToSupportDisabilityTypes', AssistiveTechnologyToSupportDisabilityType)  # type: List[AssistiveTechnologyToSupportDisabilityType]
    SocialAndPersonalCircumstances = RepeatingSubmittedAttribute('SocialAndPersonalCircumstances', SocialAndPersonalCircumstances)  # type: List[SocialAndPersonalCircumstances]
    OverseasVisitorChargingCategories = RepeatingSubmittedAttribute('OverseasVisitorChargingCategories', OverseasVisitorChargingCategory)  # type: List[OverseasVisitorChargingCategory]
    MentalHealthCurrencyModels = RepeatingSubmittedAttribute('MentalHealthCurrencyModels', MentalHealthCurrencyModel)  # type: List[MentalHealthCurrencyModel]
    MentalHealthActLegalStatusClassificationAssignmentPeriods = RepeatingSubmittedAttribute('MentalHealthActLegalStatusClassificationAssignmentPeriods', MentalHealthActLegalStatusClassificationAssignmentPeriod)  # type: List[MentalHealthActLegalStatusClassificationAssignmentPeriod]
    MedicalHistoryPreviousDiagnoses = RepeatingSubmittedAttribute('MedicalHistoryPreviousDiagnoses', MedicalHistoryPreviousDiagnosis)  # type: List[MedicalHistoryPreviousDiagnosis]
    CPACareEpisodes = RepeatingSubmittedAttribute('CPACareEpisodes', CareProgramApproachCareEpisode)  # type: List[CareProgramApproachCareEpisode]
    ClusteringToolAssessments = RepeatingSubmittedAttribute('ClusteringToolAssessments', ClusterTool)  # type: List[ClusterTool]
    FiveForensicPathways = RepeatingSubmittedAttribute('FiveForensicPathways', FiveForensicPathways)  # type: List[FiveForensicPathways]

    # ^^ auto-generated ^^

    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)  # type: SubmittedAttribute

    AgeRepPeriodStart = DerivedAttribute('AgeRepPeriodStart', int,
                                         AgeAtDate(date_of_birth_expr=Select(_MasterPatientIndex.PersonBirthDate),
                                                   date_for_age_expr=Select(_MasterPatientIndex.Root.Header.
                                                                            ReportingPeriodStartDate),
                                                   ignore_time=True))  # type: DerivedAttribute

    AgeRepPeriodEnd = DerivedAttribute('AgeRepPeriodEnd', int,
                                       AgeAtDate(date_of_birth_expr=Select(_MasterPatientIndex.PersonBirthDate),
                                                 date_for_age_expr=Select(_MasterPatientIndex.Root.Header.
                                                                          ReportingPeriodEndDate),
                                                 ignore_time=True))  # type: DerivedAttribute

    AgeDeath = DerivedAttribute('AgeDeath', int,
                                AgeAtDate(date_of_birth_expr=Select(_MasterPatientIndex.PersonBirthDate),
                                          date_for_age_expr=Select(_MasterPatientIndex.PersDeathDate),
                                          ignore_time=True))  # type: DerivedAttribute

    NHSDEthnicity = DerivedAttribute('NHSDEthnicity', str,
                                     DMSEthnicity(ethnic_category_expr=Select(_MasterPatientIndex.EthnicCategory))
                                     )  # type: DerivedAttribute

    OrgIDCCGRes = DerivedAttribute('OrgIDCCGRes', str, (
        If(
            DateBeforeOrEqualTo(
                eval_expr=(Select(_MasterPatientIndex.Root.Header.ReportingPeriodStartDate)), check_date_expr=Literal(CCG_ICB_SWITCHOVER_PERIOD_END)),
            then=(CcgFromPostcode(
                postcode_expr=Select(_MasterPatientIndex.Postcode),
                event_date=Select(_MasterPatientIndex.Root.Header.ReportingPeriodStartDate),
                enforce_icb_switchover_period=Literal(True))),
            otherwise=Literal(None))))  # type: DerivedAttribute

    OrgIDSubICBLocResidence = DerivedAttribute('OrgIDSubICBLocResidence', str, SubICBFromPostcode(
        postcode_expr=Select(_MasterPatientIndex.Postcode),
        event_date=Select(_MasterPatientIndex.Root.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    OrgIDICBRes = DerivedAttribute('OrgIDICBRes', str, ICBFromPostcode(
        postcode_expr=Select(_MasterPatientIndex.Postcode),
        event_date=Select(_MasterPatientIndex.Root.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute
    
    MHS001UniqID = DerivedAttribute(
        'MHS001UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_MasterPatientIndex.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MasterPatientIndex.RowNumber))
    )  # type: DerivedAttribute

    PostcodeDistrict = DerivedAttribute('PostcodeDistrict', str, (
        DerivePostcodeDistrict(postcode_expr=Select(_MasterPatientIndex.Postcode),
                               point_in_time_expr=Select(_MasterPatientIndex.Root.Header.ReportingPeriodStartDate))
    ))  # type: DerivedAttribute

    LSOA2011 = DerivedAttribute('LSOA2011', str, (
        DerivePostcode(postcode_expr=Select(_MasterPatientIndex.Postcode),
                       point_in_time_expr=Select(_MasterPatientIndex.Root.Header.ReportingPeriodStartDate),
                       field_name=ONSRecordPaths.LOWER_LAYER_SOA)))  # type: DerivedAttribute

    LADistrictAuth = DerivedAttribute('LADistrictAuth', str, (
        DerivePostcode(postcode_expr=Select(_MasterPatientIndex.Postcode),
                       point_in_time_expr=Select(_MasterPatientIndex.Root.Header.ReportingPeriodStartDate),
                       field_name=ONSRecordPaths.UNITARY_AUTHORITY)))  # type: DerivedAttribute

    County = DerivedAttribute('County', str,
                              (DerivePostcode(postcode_expr=Select(_MasterPatientIndex.Postcode),
                                              point_in_time_expr=Select(
                                                  _MasterPatientIndex.Root.Header.ReportingPeriodStartDate
                                              ),
                                              field_name=ONSRecordPaths.RESIDENCE_COUNTY)))  # type: DerivedAttribute

    ElectoralWard = DerivedAttribute('ElectoralWard', str,
                                     DerivePostcode(postcode_expr=Select(_MasterPatientIndex.Postcode),
                                                    point_in_time_expr=Select(
                                                        _MasterPatientIndex.Root.Header.ReportingPeriodStartDate),
                                                    field_name=ONSRecordPaths.OS_WARD_2011))  # type: DerivedAttribute

    IMDQuart = DerivedAttribute('IMDQuart', str,
                                GetIMDQuartile(postcode_expr=Select(_MasterPatientIndex.Postcode),
                                               point_in_time_expr=Select(
                                                   _MasterPatientIndex.Root.Header.ReportingPeriodStartDate))
                                )  # type: DerivedAttribute

    DefaultPostcode = DerivedAttribute('DefaultPostcode', str,
                                       GetDefaultPostcode(postcode_expr=Select(_MasterPatientIndex.Postcode))
                                       )  # type: DerivedAttribute

    LDAFlag = DerivedAttribute('LDAFlag', bool, DefaultNone()) # type: DerivedAttribute

    RecordNumber = DerivedAttribute(
        'RecordNumber', Decimal_23_0,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_MasterPatientIndex.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MasterPatientIndex.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    EthnicityHigher = DerivedAttribute('EthnicityHigher', str,
                                       EthnicityHigher(DMSEthnicity(ethnic_category_expr=Select(_MasterPatientIndex.EthnicCategory)))
                                       )  # type: DerivedAttribute

    EthnicityLow = DerivedAttribute('EthnicityLow', str,
                                    EthnicityLow(DMSEthnicity(ethnic_category_expr=Select(_MasterPatientIndex.EthnicCategory)))
                                    )  # type: DerivedAttribute

    PatMRecInRP = DerivedAttribute('PatMRecInRP', bool, Literal(False))  # type: DerivedAttribute

    EmploymentNationalLatest = DerivedAttribute('EmploymentNationalLatest', str, DefaultNone())  # type: DerivedAttribute

    EmploymentProviderLatest = DerivedAttribute('EmploymentProviderLatest', str, DefaultNone())  # type: DerivedAttribute

    AccommodationNationalLatest = DerivedAttribute('AccommodationNationalLatest', str, DefaultNone())  # type: DerivedAttribute

    AccommodationProviderLatest = DerivedAttribute('AccommodationProviderLatest', str, DefaultNone())  # type: DerivedAttribute

    ICRECCCG = DerivedAttribute('ICRECCCG', str, Literal(None))  # type: DerivedAttribute

    ICRECCCGNAME = DerivedAttribute('ICRECCCGNAME', str, Literal(None))  # type: DerivedAttribute


class Referral(_Referral):
    """MHS101Referral"""

    # -- auto-generated --

    __table__ = "MHS101Referral"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    Patient = SubmittedAttribute('Patient', MasterPatientIndex)  # type: MasterPatientIndex
    ServiceTypesReferredTo = RepeatingSubmittedAttribute('ServiceTypesReferredTo', ServiceTypeReferredTo)  # type: List[ServiceTypeReferredTo]
    OtherReasonsForReferral = RepeatingSubmittedAttribute('OtherReasonsForReferral', OtherReasonForReferral)  # type: List[OtherReasonForReferral]
    ReferralsToTreatment = RepeatingSubmittedAttribute('ReferralsToTreatment', ReferralToTreatment)  # type: List[ReferralToTreatment]
    OnwardReferrals = RepeatingSubmittedAttribute('OnwardReferrals', OnwardReferral)  # type: List[OnwardReferral]
    DischargePlanAgreements = RepeatingSubmittedAttribute('DischargePlanAgreements', DischargePlanAgreement)  # type: List[DischargePlanAgreement]
    MedicationPrescriptions = RepeatingSubmittedAttribute('MedicationPrescriptions', MedicationPrescription)  # type: List[MedicationPrescription]
    CareContacts = RepeatingSubmittedAttribute('CareContacts', CareContact)  # type: List[CareContact]
    IndirectActivities = RepeatingSubmittedAttribute('IndirectActivities', IndirectActivity)  # type: List[IndirectActivity]
    HospitalProviderSpells = RepeatingSubmittedAttribute('HospitalProviderSpells', HospitalProviderSpell)  # type: List[HospitalProviderSpell]
    ProvisionalDiagnoses = RepeatingSubmittedAttribute('ProvisionalDiagnoses', ProvisionalDiagnosis)  # type: List[ProvisionalDiagnosis]
    PrimaryDiagnoses = RepeatingSubmittedAttribute('PrimaryDiagnoses', PrimaryDiagnosis)  # type: List[PrimaryDiagnosis]
    SecondaryDiagnoses = RepeatingSubmittedAttribute('SecondaryDiagnoses', SecondaryDiagnosis)  # type: List[SecondaryDiagnosis]
    CodedScoredAssessmentReferrals = RepeatingSubmittedAttribute('CodedScoredAssessmentReferrals', CodedScoredAssessmentReferral)  # type: List[CodedScoredAssessmentReferral]

    # ^^ auto-generated ^^

    AgeServReferRecDate = DerivedAttribute(
        'AgeServReferRecDate', int,
        AgeAtDate(
            date_of_birth_expr=Select(_Referral.Patient.PersonBirthDate),
            date_for_age_expr=Select(_Referral.ReferralRequestReceivedDate),
            ignore_time=True)
    )  # type: DerivedAttribute

    AgeServReferDischDate = DerivedAttribute(
        'AgeServReferDischDate', int,
        AgeAtDate(
            date_of_birth_expr=Select(_Referral.Patient.PersonBirthDate),
            date_for_age_expr=Select(_Referral.ServDischDate),
            ignore_time=True)
    )  # type: DerivedAttribute

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_Referral.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    UniqServReqID = DerivedAttribute('UniqServReqID', str, Concat([
        Select(_Referral.Header.OrgIDProvider),
        Select(_Referral.ServiceRequestId)
    ]))  # type: DerivedAttribute

    MHS101UniqID = DerivedAttribute(
        'MHS101UniqID', int,
        RecordNumberOrUniqueID(
            submission_id_expr=Select(_Referral.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_Referral.RowNumber))
    )  # type: DerivedAttribute

    InactTimeRef = DerivedAttribute('InactTimeRef', date, If(
        NotNull(Select(_Referral.ServDischDate)),
        then=Literal(None),
        otherwise=AddToDate(
            days_to_add_expr=Literal(1),
            input_date_expr=Select(_Referral.Header.ReportingPeriodEndDate)))
    )  # type: DerivedAttribute

    FirstAttendedContactInRPDate = DerivedAttribute(
        'FirstAttendedContactInRPDate', date,
        FirstAttendedContactInRPDate(
            Select(_Referral.CareContacts),
            Select(_Referral.Header.ReportingPeriodStartDate),
            Select(_Referral.Header.ReportingPeriodEndDate))
    ) # type: DerivedAttribute

    FirstContactEverDate = DerivedAttribute(
        'FirstContactEverDate', date, Literal(None)
    ) # type: DerivedAttribute

    CountOfAttendedCareContactsInFinancialYear = DerivedAttribute('CountOfAttendedCareContactsInFinancialYear',int,
                                                  Literal(None))  # type: DerivedAttribute

    CountAttendedCareContactsInFinancialYearPersonWas017AtTimeOfContact = DerivedAttribute(
                                                 'CountAttendedCareContactsInFinancialYearPersonWas017AtTimeOfContact',int,
                                                   Literal(None))  # type: DerivedAttribute

    CountOfAttendedCareContacts = DerivedAttribute('CountOfAttendedCareContacts', int, Literal(None))  # type: DerivedAttribute

    FirstAttendedContactInFinancialYearDate = DerivedAttribute(
        'FirstAttendedContactInFinancialYearDate',
        date, Literal(None)
    )

    SecondAttendedContactEverDate = DerivedAttribute(
        'SecondAttendedContactEverDate',
        date, Literal(None)
    )

    SecondAttendedContactInFinancialYearDate = DerivedAttribute(
        'SecondAttendedContactInFinancialYearDate',
        date, Literal(None)
    )

    FirstContactEverWhereAgeAtContactUnder18Date = DerivedAttribute('FirstContactEverWhereAgeAtContactUnder18Date',
                                                                    date, DefaultNone())  # type: DerivedAttribute

    SecondContactEverWhereAgeAtContactUnder18Date = DerivedAttribute('FirstContactEverWhereAgeAtContactUnder18Date',
                                                                    date, DefaultNone())  # type: DerivedAttribute


class GroupSession(_GroupSession):
    """MHS301GroupSession"""

    # -- auto-generated --

    __table__ = "MHS301GroupSession"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    MHS301UniqID = DerivedAttribute(
        'MHS301UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_GroupSession.Header.UniqSubmissionID),
            row_number_expr=Select(_GroupSession.META.RECORD_INDEX))
    )  # type: DerivedAttribute

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_GroupSession.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    UniqCareProfLocalID = \
        DerivedAttribute(
            'UniqCareProfLocalID',
            str,
            Concat([Select(_GroupSession.Header.OrgIDProvider),
                    Select(_GroupSession.CareProfLocalId)]))  # type: DerivedAttribute


class MentalHealthDropInContact(_MentalHealthDropInContact):
    """MHS302MHDropInContact"""

    # -- auto-generated --

    __table__ = "MHS302MHDropInContact"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    NHSDEthnicity = DerivedAttribute('NHSDEthnicity', str,
                                     DMSEthnicity(ethnic_category_expr=Select(_MentalHealthDropInContact.EthnicCategory))
                                     )  # type: DerivedAttribute

    MHS302UniqID = DerivedAttribute(
        'MHS302UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MentalHealthDropInContact.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MentalHealthDropInContact.RowNumber)))  # type: DerivedAttribute

    UniqCareProfLocalID = DerivedAttribute('UniqCareProfLocalID', str, Concat(
        [Select(_MentalHealthDropInContact.Root.Header.OrgIDProvider),
            Select(_MentalHealthDropInContact.CareProfLocalId)]))  # type: DerivedAttribute

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_MentalHealthDropInContact.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    AgeRepPeriodStart = DerivedAttribute(
        'AgeRepPeriodStart',
        int,
        AgeAtDate(
            date_of_birth_expr=Select(_MentalHealthDropInContact.PersonBirthDate),
            date_for_age_expr=Select(_MentalHealthDropInContact.Header.ReportingPeriodStartDate),
            ignore_time=True
        )
    )  # type: DerivedAttribute

    AgeRepPeriodEnd = DerivedAttribute(
        'AgeRepPeriodEnd',
        int,
        AgeAtDate(
            date_of_birth_expr=Select(_MentalHealthDropInContact.PersonBirthDate),
            date_for_age_expr=Select(_MentalHealthDropInContact.Header.ReportingPeriodEndDate),
            ignore_time=True
        )
    )  # type: DerivedAttribute

    UniqMHDropInContactId = DerivedAttribute(
        'UniqMHDropInContactId', str,
        Concat([Select(_MentalHealthDropInContact.Root.Header.OrgIDProvider),
                Select(_MentalHealthDropInContact.MHDropInContactId)]))  # type: DerivedAttribute


class AnonymousSelfAssessment(_AnonymousSelfAssessment):
    """MHS608AnonSelfAssess"""

    # -- auto-generated --

    __table__ = "MHS608AnonSelfAssess"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^


    MHS608UniqID = DerivedAttribute(
        'MHS608UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AnonymousSelfAssessment.Header.UniqSubmissionID),
            row_number_expr=Select(_AnonymousSelfAssessment.META.RECORD_INDEX))
    )  # type: DerivedAttribute

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

    MHS901UniqID = DerivedAttribute(
        'MHS901UniqID', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_StaffDetails.Header.UniqSubmissionID),
            row_number_expr=Select(_StaffDetails.META.RECORD_INDEX))
    )  # type: DerivedAttribute

    UniqMonthID = DerivedAttribute('UniqMonthID', int, UniqMonth(
        date_expr=Select(_StaffDetails.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    UniqCareProfLocalID = DerivedAttribute(
        'UniqCareProfLocalID', str,
        Concat([Select(_StaffDetails.Header.OrgIDProvider),
                Select(_StaffDetails.CareProfLocalId)])
    )  # type: DerivedAttribute


def get_all_models() -> Generator[Type[BaseMHSDS_V5], None, None]:
    clsmembers = [
        name for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
        if obj.__module__ is __name__
    ]

    for member in clsmembers:
        if not member.startswith('_') and not member.startswith('BaseMHSDS_V5'):
            cls_obj = getattr(sys.modules[__name__], member)
            if safe_issubclass(cls_obj, BaseMHSDS_V5):
                yield cls_obj


def get_anonymous_models():
    return [AnonymousSelfAssessment, GroupSession, MentalHealthDropInContact, StaffDetails]
