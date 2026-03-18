# pylint: disable=line-too-long,too-many-lines

import inspect
import sys
from typing import Generator, List, Type

from dsp.datasets.models.iapt_v2_1_base import *
from dsp.model.ons_record import ONSRecordPaths
from dsp.common.iapt_v2_1_expressions import *
from dsp.common.expressions import *
from dsp.common.structured_model import (
    META,
    SubmittedAttribute,
    RepeatingSubmittedAttribute,
    DerivedAttribute,
    AssignableAttribute, MPSConfidenceScores, Decimal_23_0)
from dsp.shared import safe_issubclass
from dsp.shared.constants import DS

from dsp.common.constants import CCG_ICB_SWITCHOVER_PERIOD_END


__all__ = [
    'AccommStatus',
    'CareActivity',
    'CareCluster',
    'CareContact',
    'CarePersonnelQualification',
    'CodedScoredAssessmentActivity',
    'CodedScoredAssessmentReferral',
    'DisabilityType',
    'EmploymentStatus',
    'GPPracticeRegistration',
    'Header',
    'InternetTherapyLog',
    'LongTermCondition',
    'MPI',
    'MedHistPrevDiag',
    'OnwardReferral',
    'OverseasVisitorChargingCategory',
    'PresentingComplaint',
    'Referral',
    'SocialAndPersonalCircumstances',
    'WaitingTimePause',
    'get_all_models',
    'get_anonymous_models'
]


class Header(_Header):
    """IDS000Header"""

    # -- auto-generated --

    __table__ = "IDS000Header"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueSubmissionID = DerivedAttribute(
        'UniqueSubmissionID', int, SubmissionId(meta_event_id_expr=Select(_Header.Root.META.EVENT_ID))
    )  # type: DerivedAttribute

    File_Type = DerivedAttribute(
        'File_Type', str,
        FileTypeExpression(
            Select(_Header.Root.META.EVENT_RECEIVED_TS), Select(_Header.ReportingPeriodStartDate), DS.IAPT_V2_1
        )
    )

    UniqueID_IDS000 = DerivedAttribute(
        'UniqueID_IDS000', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Header.UniqueSubmissionID),
            row_number_expr=Select(_Header.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    Unique_MonthID = DerivedAttribute('Unique_MonthID', int, UniqMonth(
        date_expr=Select(_Header.ReportingPeriodStartDate)))  # type: DerivedAttribute


class GPPracticeRegistration(_GPPracticeRegistration):
    """IDS002GP"""

    # -- auto-generated --

    __table__ = "IDS002GP"
    __concrete__ = True

    # ^^ auto-generated ^^

    OrgID_CCG_GP = DerivedAttribute('OrgID_CCG_GP', str, (
        If(
            DateBeforeOrEqualTo(
                eval_expr=(Select(_MPI.Root.Header.ReportingPeriodStartDate)), check_date_expr=Literal(CCG_ICB_SWITCHOVER_PERIOD_END)),
            then=(
                CCGFromGPPracticeCode(gp_practice_code=Select(_GPPracticeRegistration.GMPCodeReg),
                                      event_date=Select(_GPPracticeRegistration.Root.Header.ReportingPeriodStartDate),
                                      enforce_icb_switchover_period=Literal(True))),
            otherwise=Literal(None))))  # type: DerivedAttribute

    OrgIDSubICBLocGP = DerivedAttribute('OrgIDSubICBLocGP', str, SubICBFromGPPracticeCode(
        gp_practice_code=Select(_GPPracticeRegistration.GMPCodeReg),
        event_date=Select(_GPPracticeRegistration.Root.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    OrgIDICBGPPractice = DerivedAttribute('OrgIDICBGPPractice', str, ICBFromSubICB(
        sub_icb=Select(_GPPracticeRegistration.OrgIDSubICBLocGP),
        event_date=Select(_GPPracticeRegistration.Root.Header.ReportingPeriodStartDate)))  # type: DerivedAttribute

    UniqueID_IDS002 = DerivedAttribute(
        'UniqueID_IDS002', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_GPPracticeRegistration.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_GPPracticeRegistration.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    DistanceFromHome_GP = DerivedAttribute('DistanceFromHome_GP', int, (
        OrgDistance(org_code_expr=Select(_GPPracticeRegistration.GMPCodeReg),
                    from_postcode_expr=Select(_GPPracticeRegistration.Parent.Postcode),
                    point_in_time_expr=Select(_GPPracticeRegistration.Root.Header.ReportingPeriodStartDate)))
    )  # type: DerivedAttribute

    LADistrictAuthGPPractice = DerivedAttribute(
        'LADistrictAuthGPPractice',
        str,
        UnitaryAuthorityFromGPPracticeCode(
            gp_practice_code=Select(_GPPracticeRegistration.GMPCodeReg),
            event_date=Select(_GPPracticeRegistration.Root.Header.ReportingPeriodStartDate))
    )  # type: DerivedAttribute


class AccommStatus(_AccommStatus):
    """IDS003AccommStatus"""

    # -- auto-generated --

    __table__ = "IDS003AccommStatus"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS003 = DerivedAttribute(
        'UniqueID_IDS003', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AccommStatus.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_AccommStatus.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute


class EmploymentStatus(_EmploymentStatus):
    """IDS004EmpStatus"""

    # -- auto-generated --

    __table__ = "IDS004EmpStatus"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS004 = DerivedAttribute(
        'UniqueID_IDS004', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_EmploymentStatus.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_EmploymentStatus.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute


class DisabilityType(_DisabilityType):
    """IDS007DisabilityType"""

    # -- auto-generated --

    __table__ = "IDS007DisabilityType"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS007 = DerivedAttribute(
        'UniqueID_IDS007', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_DisabilityType.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_DisabilityType.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute


class SocialAndPersonalCircumstances(_SocialAndPersonalCircumstances):
    """IDS011SocPerCircumstances"""

    # -- auto-generated --

    __table__ = "IDS011SocPerCircumstances"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS011 = DerivedAttribute(
        'UniqueID_IDS011', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_SocialAndPersonalCircumstances.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_SocialAndPersonalCircumstances.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    SocPerCircumstance_FSN = DerivedAttribute('SocPerCircumstance_FSN', str, DefaultNone())


class OverseasVisitorChargingCategory(_OverseasVisitorChargingCategory):
    """IDS012OverseasVisitorChargCat"""

    # -- auto-generated --

    __table__ = "IDS012OverseasVisitorChargCat"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS012 = DerivedAttribute(
        'UniqueID_IDS012', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_OverseasVisitorChargingCategory.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_OverseasVisitorChargingCategory.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute


class OnwardReferral(_OnwardReferral):
    """IDS105OnwardReferral"""

    # -- auto-generated --

    __table__ = "IDS105OnwardReferral"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS105 = DerivedAttribute(
        'UniqueID_IDS105', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_OnwardReferral.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_OnwardReferral.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute


class WaitingTimePause(_WaitingTimePause):
    """IDS108WaitingTimePauses"""

    # -- auto-generated --

    __table__ = "IDS108WaitingTimePauses"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS108 = DerivedAttribute(
        'UniqueID_IDS108', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_WaitingTimePause.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_WaitingTimePause.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute


class InternetTherapyLog(_InternetTherapyLog):
    """IDS205InternetTherLog"""

    # -- auto-generated --

    __table__ = "IDS205InternetTherLog"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS205 = DerivedAttribute(
        'UniqueID_IDS205', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_InternetTherapyLog.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_InternetTherapyLog.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    Unique_CarePersonnelID_Local = DerivedAttribute('Unique_CarePersonnelID_Local', str,
                                                    If(
                                                        NotNull(Select(_InternetTherapyLog.CarePersLocalId)),
                                                        then=Concat([
                                                            Select(_InternetTherapyLog.Root.Header.OrgIDProv),
                                                            Select(_InternetTherapyLog.CarePersLocalId)
                                                        ]),
                                                        otherwise=Literal(None)
                                                    )
                                                    )  # type: DerivedAttribute


class MedHistPrevDiag(_MedHistPrevDiag):
    """IDS601MedHistPrevDiag"""

    # -- auto-generated --

    __table__ = "IDS601MedHistPrevDiag"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS601 = DerivedAttribute(
        'UniqueID_IDS601', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MedHistPrevDiag.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_MedHistPrevDiag.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    PrevDiag_FSN = DerivedAttribute('PrevDiag_FSN', str, DefaultNone())
    PrevDiag_ICD10_Mapped = DerivedAttribute('PrevDiag_ICD10_Mapped', str, DefaultNone())
    PrevDiag_ICD10_Master = DerivedAttribute('PrevDiag_ICD10_Master', str, DefaultNone())
    PrevDiagDescription_ICD10_Master = DerivedAttribute('PrevDiagDescription_ICD10_Master', str, DefaultNone())


class LongTermCondition(_LongTermCondition):
    """IDS602LongTermCondition"""

    # -- auto-generated --

    __table__ = "IDS602LongTermCondition"
    __concrete__ = True

    # ^^ auto-generated ^^

    Validated_LongTermConditionCode = DerivedAttribute(
        'Validated_LongTermConditionCode', str, MatchCleanICD10Code(
            icd10code=Select(_LongTermCondition.LongTermCondition),
            findschemeinuse=Select(_LongTermCondition.FindSchemeInUse))
    )  # type: DerivedAttribute

    UniqueID_IDS602 = DerivedAttribute(
        'UniqueID_IDS602', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_LongTermCondition.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_LongTermCondition.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    LongTermConditionCode_ICD10_Mapped = DerivedAttribute('LongTermConditionCode_ICD10_Mapped', str, DefaultNone())
    LongTermConditionCode_ICD10_Master = DerivedAttribute('LongTermConditionCode_ICD10_Master', str, DefaultNone())
    LongTermConditionDescription_ICD10_Master = DerivedAttribute(
        'LongTermConditionDescription_ICD10_Master', str, DefaultNone())
    LongTermCondition_FSN = DerivedAttribute('LongTermCondition_FSN', str, DefaultNone())


class PresentingComplaint(_PresentingComplaint):
    """IDS603PresentingComplaints"""

    # -- auto-generated --

    __table__ = "IDS603PresentingComplaints"
    __concrete__ = True

    # ^^ auto-generated ^^

    Validated_PresentingComplaint = DerivedAttribute(
        'Validated_PresentingComplaint', str, MatchCleanICD10Code(
            icd10code=Select(_PresentingComplaint.PresComp),
            findschemeinuse=Select(_PresentingComplaint.FindSchemeInUse))
    )  # type: DerivedAttribute

    UniqueID_IDS603 = DerivedAttribute(
        'UniqueID_IDS603', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_PresentingComplaint.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_PresentingComplaint.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    PresentingComplaintCode_ICD10_Mapped = DerivedAttribute('PresentingComplaintCode_ICD10_Mapped', str, DefaultNone())
    PresentingComplaintCode_ICD10_Master = DerivedAttribute('PresentingComplaintCode_ICD10_Master', str, DefaultNone())
    PresentingComplaintDescription_ICD10_Master = DerivedAttribute(
        'PresentingComplaintDescription_ICD10_Master', str, DefaultNone())
    PresComp_FSN = DerivedAttribute('PresComp_FSN', str, DefaultNone())


class CodedScoredAssessmentReferral(_CodedScoredAssessmentReferral):
    """IDS606CodedScoreAssessmentRefer"""

    # -- auto-generated --

    __table__ = "IDS606CodedScoreAssessmentRefer"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS606 = DerivedAttribute(
        'UniqueID_IDS606', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessmentReferral.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CodedScoredAssessmentReferral.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    Age_AssessmentCompletion_Date = DerivedAttribute('Age_AssessmentCompletion_Date', int,
                                                     AgeAtDate(
                                                         date_of_birth_expr=Select(_CodedScoredAssessmentReferral
                                                                                   .Root.Patient.PersonBirthDate),
                                                         date_for_age_expr=Select(
                                                             _CodedScoredAssessmentReferral.AssToolCompDate),
                                                         ignore_time=True,
                                                         time_unit=AgeAtDate.YEARS)
                                                     )  # type: DerivedAttribute

    CodedAssToolType_FSN = DerivedAttribute('CodedAssToolType_FSN', str, DefaultNone())


class CodedScoredAssessmentActivity(_CodedScoredAssessmentActivity):
    """IDS607CodedScoreAssessmentAct"""

    # -- auto-generated --

    __table__ = "IDS607CodedScoreAssessmentAct"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS607 = DerivedAttribute(
        'UniqueID_IDS607', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessmentActivity.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CodedScoredAssessmentActivity.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    Unique_CareContactID = DerivedAttribute('Unique_CareContactID', str,
                                            Concat([Select(_CodedScoredAssessmentActivity.Root.Header.OrgIDProv),
                                                    Select(
                                                _CodedScoredAssessmentActivity.Parent.CareContactId)]))  # type: DerivedAttribute

    Unique_CareActivityID = DerivedAttribute('Unique_CareActivityID', str,
                                             Concat([Select(_CodedScoredAssessmentActivity.Root.Header.OrgIDProv),
                                                     Select(
                                                 _CodedScoredAssessmentActivity.CareActId)]))  # type: DerivedAttribute

    Age_AssessmentCompletion_Date = DerivedAttribute('Age_AssessmentCompletion_Date', int,
                                                     AgeAtDate(
                                                         date_of_birth_expr=Select(_CodedScoredAssessmentActivity.
                                                                                   Root.Patient.PersonBirthDate),
                                                         date_for_age_expr=Select(_CodedScoredAssessmentActivity.
                                                                                  Parent.Parent.CareContDate),
                                                         ignore_time=True,
                                                         time_unit=AgeAtDate.YEARS)
                                                     )  # type: DerivedAttribute

    CodedAssToolType_FSN = DerivedAttribute('CodedAssToolType_FSN', str, DefaultNone())


class CareCluster(_CareCluster):
    """IDS803CareCluster"""

    # -- auto-generated --

    __table__ = "IDS803CareCluster"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_IDS803 = DerivedAttribute(
        'UniqueID_IDS803', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareCluster.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CareCluster.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    CareCluster_StartedInRP_Flag = DerivedAttribute('CareCluster_StartedInRP_Flag',
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

    CareCluster_EndedInRP_Flag = DerivedAttribute('CareCluster_EndedInRP_Flag',
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

    CareCluster_OpenAtRPEnd_Flag = DerivedAttribute('CareCluster_OpenAtRPEnd_Flag',
                                                    bool,
                                                    If(
                                                        NotNull(Select(_CareCluster.EndDateCareClust)),
                                                        then=Literal(False),
                                                        otherwise=Literal(True)
                                                    )
                                                    )  # type: DerivedAttribute

    CareCluster_DayCount = DerivedAttribute(
        'CareCluster_DayCount', int,
        DaysBetween(
            If(
                NotNull(Select(_CareCluster.EndDateCareClust)),
                then=Select(_CareCluster.EndDateCareClust),
                otherwise=Select(_CareCluster.Root.Header.ReportingPeriodEndDate)
            ),
            If(NotNull(Select(_CareCluster.StartDateCareClust)), then=If(
                Select(_CareCluster.Root.Header.ReportingPeriodStartDate) > Select(_CareCluster.StartDateCareClust),
                then=Select(_CareCluster.Root.Header.ReportingPeriodStartDate),
                otherwise=Select(_CareCluster.StartDateCareClust)),
               otherwise=Literal(None)
               ),
            days_to_add=1
        )
    )  # type: DerivedAttribute


class CareActivity(_CareActivity):
    """IDS202CareActivity"""

    # -- auto-generated --

    __table__ = "IDS202CareActivity"
    __concrete__ = True

    CodedScoredAssessments = RepeatingSubmittedAttribute(
        'CodedScoredAssessments', CodedScoredAssessmentActivity)  # type: List[CodedScoredAssessmentActivity]

    # ^^ auto-generated ^^

    UniqueID_IDS202 = DerivedAttribute(
        'UniqueID_IDS202', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareActivity.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CareActivity.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    Unique_CareContactID = DerivedAttribute('Unique_CareContactID', str,
                                            Concat([Select(_CareActivity.Root.Header.OrgIDProv),
                                                    Select(
                                                _CareActivity.CareContactId)]))  # type: DerivedAttribute

    Unique_CareActivityID = DerivedAttribute('Unique_CareActivityID', str,
                                             Concat([Select(_CareActivity.Root.Header.OrgIDProv),
                                                     Select(
                                                 _CareActivity.CareActId)]))  # type: DerivedAttribute

    Unique_CarePersonnelID_Local = DerivedAttribute('Unique_CarePersonnelID_Local', str,
                                                    If(
                                                        NotNull(Select(_CareActivity.CarePersLocalId)),
                                                        then=Concat([
                                                            Select(_CareActivity.Root.Header.OrgIDProv),
                                                            Select(_CareActivity.CarePersLocalId)
                                                        ]),
                                                        otherwise=Literal(None)
                                                    )
                                                    )  # type: DerivedAttribute

    Validated_FindingCode = DerivedAttribute('Validated_FindingCode', str, MatchCleanICD10Code(
        icd10code=Select(_CareActivity.CodeFind),
        findschemeinuse=Select(_CareActivity.FindSchemeInUse))
    )  # type: DerivedAttribute

    FindingCode_ICD10_Mapped = DerivedAttribute('FindingCode_ICD10_Mapped', str, DefaultNone())
    FindingCode_ICD10_Master = DerivedAttribute('FindingCode_ICD10_Master', str, DefaultNone())
    FindingDescription_ICD10_Master = DerivedAttribute('FindingDescription_ICD10_Master', str, DefaultNone())
    CodeProcAndProcStatus_FSN = DerivedAttribute('CodeProcAndProcStatus_FSN', str, DefaultNone())
    CodeFind_FSN = DerivedAttribute('CodeFind_FSN', str, DefaultNone())
    CodeObs_FSN = DerivedAttribute('CodeObs_FSN', str, DefaultNone())


class CareContact(_CareContact):
    """IDS201CareContact"""

    # -- auto-generated --

    __table__ = "IDS201CareContact"
    __concrete__ = True

    CareActivities = RepeatingSubmittedAttribute('CareActivities', CareActivity)  # type: List[CareActivity]

    # ^^ auto-generated ^^

    UniqueID_IDS201 = DerivedAttribute(
        'UniqueID_IDS201', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareContact.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CareContact.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    Unique_CareContactID = DerivedAttribute('Unique_CareContactID', str,
                                            Concat([Select(_CareContact.Root.Header.OrgIDProv),
                                                    Select(
                                                _CareContact.CareContactId)]))  # type: DerivedAttribute
    Time_Referral_to_CareContact = DerivedAttribute(
        'Time_Referral_to_CareContact', int, DaysBetween(Select(
            _CareContact.CareContDate), Select(_CareContact.Root.ReferralRequestReceivedDate))
    )  # type: DerivedAttribute

    Age_CareContact_Date = DerivedAttribute(
        'Age_CareContact_Date', int,
        AgeAtDate(date_of_birth_expr=Select(_CareContact.Root.Patient.PersonBirthDate),
                  date_for_age_expr=Select(_CareContact.CareContDate), ignore_time=True,
                  time_unit=AgeAtDate.YEARS)
    )  # type: DerivedAttribute

    DistanceFromHome_ContactLocation = DerivedAttribute(
        'DistanceFromHome_ContactLocation', int,
        (OrgDistance(org_code_expr=Select(_CareContact.SiteIDOfTreat),
                     from_postcode_expr=Select(_CareContact.Root.Patient.Postcode),
                     point_in_time_expr=Select(
                         _CareContact.Root.Header.ReportingPeriodStartDate
        )
        )
        ))  # type: DerivedAttribute


class MPI(_MPI):
    """IDS001MPI"""

    # -- auto-generated --

    __table__ = "IDS001MPI"
    __concrete__ = True

    GPPracticeRegistrations = RepeatingSubmittedAttribute(
        'GPPracticeRegistrations', GPPracticeRegistration)  # type: List[GPPracticeRegistration]
    EmploymentStatuses = RepeatingSubmittedAttribute(
        'EmploymentStatuses', EmploymentStatus)  # type: List[EmploymentStatus]
    DisabilityTypes = RepeatingSubmittedAttribute('DisabilityTypes', DisabilityType)  # type: List[DisabilityType]
    SocialAndPersonalCircumstances = RepeatingSubmittedAttribute(
        'SocialAndPersonalCircumstances', SocialAndPersonalCircumstances)  # type: List[SocialAndPersonalCircumstances]
    OverseasVisitorChargingCategories = RepeatingSubmittedAttribute(
        'OverseasVisitorChargingCategories', OverseasVisitorChargingCategory)  # type: List[OverseasVisitorChargingCategory]
    CareClusters = RepeatingSubmittedAttribute('CareClusters', CareCluster)  # type: List[CareCluster]
    AccommStatus = RepeatingSubmittedAttribute('AccommStatus', AccommStatus)  # type: List[AccommStatus]
    MedHistPrevDiag = RepeatingSubmittedAttribute('MedHistPrevDiag', MedHistPrevDiag)  # type: List[MedHistPrevDiag]

    # ^^ auto-generated ^^

    UniqueID_IDS001 = DerivedAttribute(
        'UniqueID_IDS001', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MPI.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_MPI.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    RecordNumber = DerivedAttribute(
        'RecordNumber', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MPI.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_MPI.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    ValidPostcode_Flag = DerivedAttribute(
        'ValidPostcode_Flag', str, If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode), point_in_time_expr=Select(
                _MPI.Root.Header.ReportingPeriodStartDate)), then=Literal('Y'), otherwise=Literal('N'))
    )  # type: DerivedAttribute

    OrgID_CCG_Residence = DerivedAttribute('OrgID_CCG_Residence', str, (
        If(
            DateBeforeOrEqualTo(
                eval_expr=(Select(_MPI.Root.Header.ReportingPeriodStartDate)), check_date_expr=Literal(CCG_ICB_SWITCHOVER_PERIOD_END)),
            then=(
                If(
                    ValidPostcode(postcode_expr=Select(_MPI.Postcode), point_in_time_expr=Select(
                        _MPI.Root.Header.ReportingPeriodStartDate)),
                    then=(CcgFromPostcode(
                        postcode_expr=Select(_MPI.Postcode),
                        event_date=Select(_MPI.Root.Header.ReportingPeriodStartDate),
                        enforce_icb_switchover_period=Literal(True))),
                    otherwise=Literal(None))),
            otherwise=Literal(None))))  # type: DerivedAttribute

    OrgIDSubICBLocResidence = DerivedAttribute('OrgIDSubICBLocResidence', str, (
        If(
            ValidPostcode(
                postcode_expr=Select(_MPI.Postcode), point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate)
            ),
            then=(
                SubICBFromPostcode(
                    postcode_expr=Select(_MPI.Postcode), event_date=Select(_MPI.Root.Header.ReportingPeriodStartDate)
                )
            ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    OrgIDICBRes = DerivedAttribute('OrgIDICBRes', str, (
        If(
            ValidPostcode(
                postcode_expr=Select(_MPI.Postcode), point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate)
            ),
            then=(
                ICBFromPostcode(
                    postcode_expr=Select(_MPI.Postcode),
                    event_date=Select(_MPI.Root.Header.ReportingPeriodStartDate)
                )
            ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)  # type: AssignableAttribute

    Age_RP_StartDate = DerivedAttribute('Age_RP_StartDate', int,
                                        AgeAtDate(date_of_birth_expr=Select(_MPI.PersonBirthDate),
                                                  date_for_age_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate),
                                                  ignore_time=True,
                                                  time_unit=AgeAtDate.YEARS)
                                        )  # type: DerivedAttribute

    Age_RP_EndDate = DerivedAttribute('Age_RP_EndDate', int,
                                      AgeAtDate(date_of_birth_expr=Select(_MPI.PersonBirthDate),
                                                date_for_age_expr=Select(_MPI.Root.Header.ReportingPeriodEndDate),
                                                ignore_time=True,
                                                time_unit=AgeAtDate.YEARS)
                                      )  # type: DerivedAttribute

    Postcode_District = DerivedAttribute('Postcode_District', str, (
        DerivePostcodeDistrict(postcode_expr=Select(_MPI.Postcode),
                               point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate))
    ))  # type: DerivedAttribute
    # Postcode_District doesn't need a ValidPostcode check as DerivePostcodeDistrict checks point_in__time against
    # postcode, which has start and end date. Derivations using DerivePostcode do need ValidPostcode check as
    # point_in__time is instead checked against LSOA etc., which only have start date

    Postcode_Default = DerivedAttribute('Postcode_Default', str, (
        GetDefaultPostcode(postcode_expr=Select(_MPI.Postcode))
    ))  # type: DerivedAttribute

    LSOA = DerivedAttribute('LSOA', str, (
        If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                          point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate)),
            then=(DerivePostcode(postcode_expr=Select(_MPI.Postcode),
                                 point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate),
                                 field_name=ONSRecordPaths.LOWER_LAYER_SOA)
                  ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    Census_Year = DerivedAttribute('Census_Year', int, (
        If(
            NotNull(Select(_MPI.LSOA)),
            then=Literal(2011),
            otherwise=Literal(None))
    ))  # type: DerivedAttribute

    LocalAuthority = DerivedAttribute('LocalAuthority', str, (
        If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                          point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate)),
            then=(DerivePostcode(postcode_expr=Select(_MPI.Postcode),
                                 point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate),
                                 field_name=ONSRecordPaths.UNITARY_AUTHORITY)
                  ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    County = DerivedAttribute('County', str, (
        If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                          point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate)),
            then=(DerivePostcode(postcode_expr=Select(_MPI.Postcode),
                                 point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate),
                                 field_name=ONSRecordPaths.RESIDENCE_COUNTY)
                  ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    ElectoralWard = DerivedAttribute('ElectoralWard', str, (
        If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                          point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate)),
            then=(DerivePostcode(postcode_expr=Select(_MPI.Postcode),
                                 point_in_time_expr=Select(_MPI.Root.Header.ReportingPeriodStartDate),
                                 field_name=ONSRecordPaths.OS_WARD_2011)
                  ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    IndicesOfDeprivationDecile = DerivedAttribute(
        'IndicesOfDeprivationDecile',
        int,
        (
            If(
                ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                              point_in_time_expr=Select(
                                  _MPI.Root.Header.ReportingPeriodStartDate)),
                then=(GetIMDDecile(
                    postcode_expr=Select(_MPI.Postcode),
                    point_in_time_expr=Select(
                        _MPI.Root.Header.ReportingPeriodStartDate),
                    imd_year=2019, should_sanitise=True)),
                otherwise=Literal(None)
            )
        )
    )  # type: DerivedAttribute

    IndicesOfDeprivationQuartile = DerivedAttribute(
        'IndicesOfDeprivationQuartile',
        int,
        (
            If(
                ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                              point_in_time_expr=Select(
                                  _MPI.Root.Header.ReportingPeriodStartDate)),
                then=(GetIMDQuartile(
                    postcode_expr=Select(_MPI.Postcode),
                    point_in_time_expr=Select(
                        _MPI.Root.Header.ReportingPeriodStartDate),
                    imd_year=2019)),
                otherwise=Literal(None)
            )
        )
    )  # type: DerivedAttribute

    IMD_YEAR = DerivedAttribute('LSOA_YEAR', int, (
        If(
            NotNull(Concat([Select(_MPI.IndicesOfDeprivationDecile), Select(_MPI.IndicesOfDeprivationQuartile)])),
            then=Literal(2019),
            otherwise=Literal(None))
    ))  # type: DerivedAttribute

    Validated_EthnicCategory = DerivedAttribute('Validated_EthnicCategory', str, CleanEthnicCategory(
        ethnic_category=Select(_MPI.EthnicCategory),
    ))  # type: DerivedAttribute


class Referral(_Referral):
    """IDS101Referral"""

    # -- auto-generated --

    __table__ = "IDS101Referral"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    Patient = SubmittedAttribute('Patient', MPI)  # type: MPI
    OnwardReferrals = RepeatingSubmittedAttribute('OnwardReferrals', OnwardReferral)  # type: List[OnwardReferral]
    WaitingTimePauses = RepeatingSubmittedAttribute(
        'WaitingTimePauses', WaitingTimePause)  # type: List[WaitingTimePause]
    CareContacts = RepeatingSubmittedAttribute('CareContacts', CareContact)  # type: List[CareContact]
    InternetTherapyLogs = RepeatingSubmittedAttribute(
        'InternetTherapyLogs', InternetTherapyLog)  # type: List[InternetTherapyLog]
    LongTermConditions = RepeatingSubmittedAttribute(
        'LongTermConditions', LongTermCondition)  # type: List[LongTermCondition]
    PresentingComplaints = RepeatingSubmittedAttribute(
        'PresentingComplaints', PresentingComplaint)  # type: List[PresentingComplaint]
    CodedScoredAssessments = RepeatingSubmittedAttribute(
        'CodedScoredAssessments', CodedScoredAssessmentReferral)  # type: List[CodedScoredAssessmentReferral]

    # ^^ auto-generated ^^

    RFAs = DerivedAttribute(
        'RFAs', List[str], ReasonsForAccessExpression(Select(_Referral.Root))
    )  # type: DerivedAttribute

    UniqueID_IDS101 = DerivedAttribute(
        'UniqueID_IDS101', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Referral.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_Referral.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    Unique_MonthID = DerivedAttribute('Unique_MonthID', int,
                                      Select(_Referral.Header.Unique_MonthID))  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute('Unique_ServiceRequestID', str,
                                               Concat([Select(_Referral.Header.OrgIDProv),
                                                       Select(
                                                   _Referral.ServiceRequestId)]))  # type: DerivedAttribute

    Age_ReferralRequest_ReceivedDate = DerivedAttribute(
        'Age_ReferralRequest_ReceivedDate', int, AgeAtDate(
            date_of_birth_expr=Select(_Referral.Patient.PersonBirthDate),
            date_for_age_expr=Select(_Referral.ReferralRequestReceivedDate), ignore_time=True,
            time_unit=AgeAtDate.YEARS)
    )  # type: DerivedAttribute

    Age_ServiceDischarge_Date = DerivedAttribute('Age_ServiceDischarge_Date', int,
                                                 AgeAtDate(date_of_birth_expr=Select(_Referral.Patient.PersonBirthDate),
                                                           date_for_age_expr=Select(_Referral.ServDischDate),
                                                           ignore_time=True,
                                                           time_unit=AgeAtDate.YEARS)
                                                 )  # type: DerivedAttribute

    CommissioningRegion = DerivedAttribute('CommissioningRegion', str,
                                           DeriveCommissioningRegion(
                                               org_code_expr=Select(_Referral.OrgIDComm),
                                               point_in_time_expr=Select(_Referral.Header.ReportingPeriodEndDate)
                                           ))  # type: DerivedAttribute


class CarePersonnelQualification(_CarePersonnelQualification):
    """IDS902CarePersQual"""

    # -- auto-generated --

    __table__ = "IDS902CarePersQual"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    RFAs = DerivedAttribute(
        'RFAs', List[str], CarePersonnelQualificationReasonsForAccessExpression()
    )  # type: DerivedAttribute

    UniqueID_IDS902 = DerivedAttribute(
        'UniqueID_IDS902', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CarePersonnelQualification.Header.UniqueSubmissionID),
            row_number_expr=Select(_CarePersonnelQualification.RowNumber),
            return_decimal=True)
    )  # type: DerivedAttribute

    Unique_MonthID = DerivedAttribute('Unique_MonthID', int, Select(
        _CarePersonnelQualification.Header.Unique_MonthID))  # type: DerivedAttribute

    Unique_CarePersonnelID_Local = DerivedAttribute('Unique_CarePersonnelID_Local', str,
                                                    If(
                                                        NotNull(Select(_CarePersonnelQualification.CarePersLocalId)),
                                                        then=Concat([
                                                            Select(_CarePersonnelQualification.Header.OrgIDProv),
                                                            Select(_CarePersonnelQualification.CarePersLocalId)
                                                        ]),
                                                        otherwise=Literal(None)
                                                    )
                                                    )  # type: DerivedAttribute


def get_all_models() -> Generator[Type[BaseIAPT_V2_1], None, None]:
    clsmembers = [
        name for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
        if obj.__module__ is __name__
    ]

    for member in clsmembers:
        if not member.startswith('_') and not member.startswith('BaseIAPT_V2_1'):
            cls_obj = getattr(sys.modules[__name__], member)
            if safe_issubclass(cls_obj, BaseIAPT_V2_1):
                yield cls_obj


def get_anonymous_models():
    return [CarePersonnelQualification]
