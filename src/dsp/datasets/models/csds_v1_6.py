# pylint: disable=line-too-long,too-many-lines

import inspect
import sys
from datetime import datetime, date
from typing import Generator, List, Type

from dsp.datasets.models.csds_v1_6_base import *
from dsp.model.ons_record import ONSRecordPaths
from dsp.common.expressions import *
from dsp.common.structured_model import (
    META,
    SubmittedAttribute,
    RepeatingSubmittedAttribute,
    DerivedAttribute,
    AssignableAttribute,
    MPSConfidenceScores,
    Decimal_23_0
)
from dsp.shared import safe_issubclass
from dsp.shared.constants import DS

from dsp.common.constants import CCG_ICB_SWITCHOVER_PERIOD_END


__all__ = [
    'AccommodationType',
    'AnonSelfAssessment',
    'AssTechToSupportDisabilityType',
    'BloodSpotResult',
    'BreastfeedingStatus',
    'CareActivity',
    'CareContact',
    'CarePlanAgreement',
    'CarePlanType',
    'ChildProtectionPlan',
    'CodedImmunisation',
    'CodedScoredAssessmentContact',
    'CodedScoredAssessmentReferral',
    'DisabilityType',
    'EmploymentStatus',
    'GPPracticeRegistration',
    'GroupSession',
    'Header',
    'Immunisation',
    'InfantPhysicalExamination',
    'MPI',
    'MedicalHistory',
    'NewbornHearingScreening',
    'Observation',
    'OnwardReferral',
    'OtherReasonForReferral',
    'OverseasVisitorChargeCat',
    'PrimaryDiagnosis',
    'ProvisionalDiagnosis',
    'Referral',
    'ReferralToTreatment',
    'SafeguardingVulnerabilityFactor',
    'SecondaryDiagnosis',
    'ServiceTypeReferredTo',
    'SocialAndPersonalCircumstances',
    'SpecialEducationalNeed',
    'StaffDetails',
    'get_all_models',
    'get_anonymous_models'
]


class Header(_Header):
    """CYP000Header"""

    # -- auto-generated --

    __table__ = "CYP000Header"
    __concrete__ = True

    # ^^ auto-generated ^^

    Upload_DateTime = DerivedAttribute(
        'Upload_DateTime', datetime, Select(_Header.Root.META.EVENT_RECEIVED_TS)
    )  # type: datetime

    UniqueID_CYP000 = DerivedAttribute(
        'UniqueID_CYP000', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Header.UniqueSubmissionID),
            row_number_expr=Select(_Header.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    UniqueSubmissionID = DerivedAttribute(
        'UniqueSubmissionID', int, SubmissionId(meta_event_id_expr=Select(_Header.Root.META.EVENT_ID))
    )  # type: DerivedAttribute

    File_Type = DerivedAttribute(
        'File_Type', str,
        FileTypeExpression(
            Select(_Header.Root.META.EVENT_RECEIVED_TS), Select(_Header.RP_StartDate), DS.CSDS_V1_6
        )
    )  # type: str
    Unique_MonthID = DerivedAttribute(
        'Unique_MonthID', int, UniqMonth(date_expr=Select(_Header.RP_StartDate))
    )  # type: int


class GPPracticeRegistration(_GPPracticeRegistration):
    """CYP002GP"""

    # -- auto-generated --

    __table__ = "CYP002GP"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP002 = DerivedAttribute(
        'UniqueID_CYP002', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_GPPracticeRegistration.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_GPPracticeRegistration.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    DistanceFromHome_GP = DerivedAttribute('DistanceFromHome_GP', int, (
        If(
            ValidPostcode(postcode_expr=Select(_GPPracticeRegistration.Parent.Postcode), point_in_time_expr=Select(
                _GPPracticeRegistration.Root.Header.RP_StartDate)),
            then=(
                OrgDistance(org_code_expr=Select(_GPPracticeRegistration.OrgID_GP),
                            from_postcode_expr=Select(_GPPracticeRegistration.Parent.Postcode),
                            point_in_time_expr=Select(_GPPracticeRegistration.Root.Header.RP_StartDate))
            ),
            otherwise=Literal(None)
        )
    ))   # type: DerivedAttribute

    OrgID_CCG_GP = DerivedAttribute('OrgID_CCG_GP', str, (
        If(
            DateBeforeOrEqualTo(
                eval_expr=(Select(_MPI.Root.Header.RP_StartDate)), check_date_expr=Literal(CCG_ICB_SWITCHOVER_PERIOD_END)),
            then=(
                CCGFromGPPracticeCode(gp_practice_code=Select(_GPPracticeRegistration.OrgID_GP),
                                      event_date=Select(
                    _GPPracticeRegistration.Root.Header.RP_StartDate),
                    enforce_icb_switchover_period=Literal(True))),
            otherwise=Literal(None))))

    OrgIDSubICBLocGP = DerivedAttribute('OrgIDSubICBLocGP', str, SubICBFromGPPracticeCode(
        gp_practice_code=Select(_GPPracticeRegistration.OrgID_GP),
        event_date=Select(_GPPracticeRegistration.Root.Header.RP_StartDate)))  # type: DerivedAttribute

    OrgIDICBGPPractice = DerivedAttribute('OrgIDICBGPPractice', str, ICBFromSubICB(
        sub_icb=Select(_GPPracticeRegistration.OrgIDSubICBLocGP),
        event_date=Select(_GPPracticeRegistration.Root.Header.RP_StartDate)))  # type: DerivedAttribute


class AccommodationType(_AccommodationType):
    """CYP003AccommType"""

    # -- auto-generated --

    __table__ = "CYP003AccommType"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP003 = DerivedAttribute(
        'UniqueID_CYP003', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AccommodationType.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_AccommodationType.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Age_AccommodationStatusDate = DerivedAttribute('Age_AccommodationStatusDate', int,
                                                   AgeAtDate(date_of_birth_expr=Select(_AccommodationType.Root.DateOfBirth),
                                                             date_for_age_expr=Select(
                                                       _AccommodationType.AccommStatus_Date),
                                                       ignore_time=True,
                                                       time_unit=AgeAtDate.DAYS)
                                                   )
    # type: int

    AgeYr_AccommodationStatusDate = DerivedAttribute('AgeYr_AccommodationStatusDate', int,
                                                     AgeAtDate(date_of_birth_expr=Select(_AccommodationType.Root.DateOfBirth),
                                                               date_for_age_expr=Select(
                                                         _AccommodationType.AccommStatus_Date),
                                                         ignore_time=True,
                                                         time_unit=AgeAtDate.YEARS)
                                                     )
    # type: int

    AgeGroup_AccommodationStatusDate = DerivedAttribute(
        'AgeGroup_AccommodationStatusDate', str,
        AgeGroup(age_yrs_expr=Select(_AccommodationType.AgeYr_AccommodationStatusDate))
    )
    # type: str

    AgeBand_AccommodationStatusDate = DerivedAttribute(
        'AgeBand_AccommodationStatusDate', str,
        AgeBand(age_yr_expr=Select(_AccommodationType.AgeYr_AccommodationStatusDate))
    )
    # type: str


class CarePlanAgreement(_CarePlanAgreement):
    """CYP005CarePlanAgreement"""

    # -- auto-generated --

    __table__ = "CYP005CarePlanAgreement"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP005 = DerivedAttribute(
        'UniqueID_CYP005', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CarePlanAgreement.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CarePlanAgreement.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_PlanID = DerivedAttribute(
        'Unique_PlanID', str, Concat([
            Select(_CarePlanAgreement.Root.Header.OrgID_Provider),
            Select(_CarePlanAgreement.PlanID)
        ])
    )  # type: str


class SocialAndPersonalCircumstances(_SocialAndPersonalCircumstances):
    """CYP006SocPerCircumstances"""

    # -- auto-generated --

    __table__ = "CYP006SocPerCircumstances"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP006 = DerivedAttribute(
        'UniqueID_CYP006', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_SocialAndPersonalCircumstances.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_SocialAndPersonalCircumstances.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute


class EmploymentStatus(_EmploymentStatus):
    """CYP007EmpStatus"""

    # -- auto-generated --

    __table__ = "CYP007EmpStatus"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP007 = DerivedAttribute(
        'UniqueID_CYP007', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_EmploymentStatus.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_EmploymentStatus.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute


class OverseasVisitorChargeCat(_OverseasVisitorChargeCat):
    """CYP008OverseasVisitorChargCat"""

    # -- auto-generated --

    __table__ = "CYP008OverseasVisitorChargCat"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP008 = DerivedAttribute(
        'UniqueID_CYP008', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_OverseasVisitorChargeCat.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_OverseasVisitorChargeCat.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute


class ServiceTypeReferredTo(_ServiceTypeReferredTo):
    """CYP102ServiceTypeReferredTo"""

    # -- auto-generated --

    __table__ = "CYP102ServiceTypeReferredTo"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP102 = DerivedAttribute(
        'UniqueID_CYP102', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_ServiceTypeReferredTo.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_ServiceTypeReferredTo.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_ServiceTypeReferredTo.Root.Header.OrgID_Provider),
            Select(_ServiceTypeReferredTo.ServiceRequestID)
        ])
    )  # type: str

    Unique_TeamID_Local = DerivedAttribute(
        'Unique_TeamID_Local', str,
        If(NotNull(Select(_ServiceTypeReferredTo.TeamID_Local)),
           then=Concat([
               Select(_ServiceTypeReferredTo.Root.Header.OrgID_Provider),
               Select(_ServiceTypeReferredTo.TeamID_Local)]),
           otherwise=Literal(None)))  # type: str

    Age_Referral_ClosureDate = DerivedAttribute('Age_Referral_ClosureDate', int,
                                                AgeAtDate(date_of_birth_expr=Select(
                                                    _ServiceTypeReferredTo.Root.DateOfBirth),
                                                    date_for_age_expr=Select(
                                                    _ServiceTypeReferredTo.Referral_ClosureDate),
                                                    ignore_time=True,
                                                    time_unit=AgeAtDate.DAYS)
                                                )
    # type: int

    Age_Referral_RejectionDate = DerivedAttribute('Age_Referral_RejectionDate', int,
                                                  AgeAtDate(date_of_birth_expr=Select(
                                                      _ServiceTypeReferredTo.Root.DateOfBirth),
                                                      date_for_age_expr=Select(
                                                      _ServiceTypeReferredTo.Referral_RejectionDate),
                                                      ignore_time=True,
                                                      time_unit=AgeAtDate.DAYS)
                                                  )
    # type: int

    AgeYr_Referral_ClosureDate = DerivedAttribute('AgeYr_Referral_ClosureDate', int,
                                                  AgeAtDate(date_of_birth_expr=Select(
                                                      _ServiceTypeReferredTo.Root.DateOfBirth),
                                                      date_for_age_expr=Select(
                                                      _ServiceTypeReferredTo.Referral_ClosureDate),
                                                      ignore_time=True,
                                                      time_unit=AgeAtDate.YEARS)
                                                  )
    # type: int

    AgeYr_Referral_RejectionDate = DerivedAttribute('AgeYr_Referral_RejectionDate', int,
                                                    AgeAtDate(date_of_birth_expr=Select(
                                                        _ServiceTypeReferredTo.Root.DateOfBirth),
                                                        date_for_age_expr=Select(
                                                        _ServiceTypeReferredTo.Referral_RejectionDate),
                                                        ignore_time=True,
                                                        time_unit=AgeAtDate.YEARS)
                                                    )
    # type: int

    AgeGroup_Referral_ClosureDate = DerivedAttribute(
        'AgeGroup_Referral_ClosureDate', str, AgeGroup(age_yrs_expr=Select(
            _ServiceTypeReferredTo.AgeYr_Referral_ClosureDate))
    )
    # type: str

    AgeGroup_Referral_RejectionDate = DerivedAttribute(
        'AgeGroup_Referral_RejectionDate', str, AgeGroup(age_yrs_expr=Select(
            _ServiceTypeReferredTo.AgeYr_Referral_RejectionDate))
    )
    # type: str

    AgeBand_Referral_ClosureDate = DerivedAttribute(
        'AgeBand_Referral_ClosureDate', str, AgeBand(age_yr_expr=Select(
            _ServiceTypeReferredTo.AgeYr_Referral_ClosureDate))
    )
    # type: str

    AgeBand_Referral_RejectionDate = DerivedAttribute(
        'AgeBand_Referral_RejectionDate', str, AgeBand(age_yr_expr=Select(
            _ServiceTypeReferredTo.AgeYr_Referral_RejectionDate))
    )
    # type: str


class OtherReasonForReferral(_OtherReasonForReferral):
    """CYP103OtherReasonReferral"""

    # -- auto-generated --

    __table__ = "CYP103OtherReasonReferral"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP103 = DerivedAttribute(
        'UniqueID_CYP103', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_OtherReasonForReferral.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_OtherReasonForReferral.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_OtherReasonForReferral.Root.Header.OrgID_Provider),
            Select(_OtherReasonForReferral.ServiceRequestID)
        ])
    )  # type: str


class ReferralToTreatment(_ReferralToTreatment):
    """CYP104RTT"""

    # -- auto-generated --

    __table__ = "CYP104RTT"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP104 = DerivedAttribute(
        'UniqueID_CYP104', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_ReferralToTreatment.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_ReferralToTreatment.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_ReferralToTreatment.Root.Header.OrgID_Provider),
            Select(_ReferralToTreatment.ServiceRequestID)
        ])
    )  # type: str

    Days_RTTStart_to_End = DerivedAttribute('Days_RTTStart_to_End', int,
                                            AgeAtDate(date_of_birth_expr=Select(_ReferralToTreatment.RTT_StartDate),
                                                      date_for_age_expr=Select(_ReferralToTreatment.RTT_EndDate),
                                                      ignore_time=True,
                                                      time_unit=AgeAtDate.DAYS)
                                            )
    # type: int

    Age_RTT_StartDate = DerivedAttribute('Age_RTT_StartDate', int,
                                         AgeAtDate(date_of_birth_expr=Select(_ReferralToTreatment.Root.DateOfBirth),
                                                   date_for_age_expr=Select(_ReferralToTreatment.RTT_StartDate),
                                                   ignore_time=True,
                                                   time_unit=AgeAtDate.DAYS)
                                         )
    # type: int

    Age_RTT_EndDate = DerivedAttribute('Age_RTT_EndDate ', int,
                                       AgeAtDate(date_of_birth_expr=Select(_ReferralToTreatment.Root.DateOfBirth),
                                                 date_for_age_expr=Select(_ReferralToTreatment.RTT_EndDate),
                                                 ignore_time=True,
                                                 time_unit=AgeAtDate.DAYS)
                                       )
    # type: int

    AgeYr_RTT_StartDate = DerivedAttribute('AgeYr_RTT_StartDate ', int,
                                           AgeAtDate(date_of_birth_expr=Select(_ReferralToTreatment.Root.DateOfBirth),
                                                     date_for_age_expr=Select(_ReferralToTreatment.RTT_StartDate),
                                                     ignore_time=True,
                                                     time_unit=AgeAtDate.YEARS)
                                           )
    # type: int

    AgeYr_RTT_EndDate = DerivedAttribute('AgeYr_RTT_EndDate ', int,
                                         AgeAtDate(date_of_birth_expr=Select(_ReferralToTreatment.Root.DateOfBirth),
                                                   date_for_age_expr=Select(_ReferralToTreatment.RTT_EndDate),
                                                   ignore_time=True,
                                                   time_unit=AgeAtDate.YEARS)
                                         )
    # type: int

    AgeGroup_RTT_StartDate = DerivedAttribute(
        'AgeGroup_RTT_StartDate', str, AgeGroup(age_yrs_expr=Select(_ReferralToTreatment.AgeYr_RTT_StartDate))
    )
    # type: str

    AgeGroup_RTT_EndDate = DerivedAttribute(
        'AgeGroup_RTT_EndDate', str, AgeGroup(age_yrs_expr=Select(_ReferralToTreatment.AgeYr_RTT_EndDate))
    )
    # type: str

    AgeBand_RTT_StartDate = DerivedAttribute(
        'AgeBand_RTT_StartDate', str, AgeBand(age_yr_expr=Select(_ReferralToTreatment.AgeYr_RTT_StartDate))
    )
    # type: str

    AgeBand_RTT_EndDate = DerivedAttribute(
        'AgeBand_RTT_EndDate	', str, AgeBand(age_yr_expr=Select(_ReferralToTreatment.AgeYr_RTT_EndDate))
    )
    # type: str


class OnwardReferral(_OnwardReferral):
    """CYP105OnwardReferral"""

    # -- auto-generated --

    __table__ = "CYP105OnwardReferral"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP105 = DerivedAttribute(
        'UniqueID_CYP105', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_OnwardReferral.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_OnwardReferral.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_OnwardReferral.Root.Header.OrgID_Provider),
            Select(_OnwardReferral.ServiceRequestID)
        ])
    )  # type: str


class SpecialEducationalNeed(_SpecialEducationalNeed):
    """CYP401SpEdNeedId"""

    # -- auto-generated --

    __table__ = "CYP401SpEdNeedId"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP401 = DerivedAttribute(
        'UniqueID_CYP401', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_SpecialEducationalNeed.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_SpecialEducationalNeed.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute


class SafeguardingVulnerabilityFactor(_SafeguardingVulnerabilityFactor):
    """CYP402SVF"""

    # -- auto-generated --

    __table__ = "CYP402SVF"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP402 = DerivedAttribute(
        'UniqueID_CYP402', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_SafeguardingVulnerabilityFactor.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_SafeguardingVulnerabilityFactor.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute


class ChildProtectionPlan(_ChildProtectionPlan):
    """CYP403CPP"""

    # -- auto-generated --

    __table__ = "CYP403CPP"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP403 = DerivedAttribute(
        'UniqueID_CYP403', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_ChildProtectionPlan.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_ChildProtectionPlan.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Duration_SpentOnCPP = DerivedAttribute('Duration_SpentOnCPP', int,
                                           DaysBetween(Select(_ChildProtectionPlan.CPP_EndDate),
                                                       Select(_ChildProtectionPlan.CPP_StartDate),
                                                       days_to_add=1)
                                           )  # type: int

    Age_CPP_StartDate = DerivedAttribute('Age_CPP_StartDate', int,
                                         AgeAtDate(date_of_birth_expr=Select(_ChildProtectionPlan.Root.DateOfBirth),
                                                   date_for_age_expr=Select(_ChildProtectionPlan.CPP_StartDate),
                                                   ignore_time=True,
                                                   time_unit=AgeAtDate.DAYS)
                                         )
    # type: int

    Age_CPP_EndDate = DerivedAttribute('Age_CPP_EndDate ', int,
                                       AgeAtDate(date_of_birth_expr=Select(_ChildProtectionPlan.Root.DateOfBirth),
                                                 date_for_age_expr=Select(_ChildProtectionPlan.CPP_EndDate),
                                                 ignore_time=True,
                                                 time_unit=AgeAtDate.DAYS)
                                       )
    # type: int

    AgeYr_CPP_StartDate = DerivedAttribute('AgeYr_CPP_StartDate ', int,
                                           AgeAtDate(date_of_birth_expr=Select(_ChildProtectionPlan.Root.DateOfBirth),
                                                     date_for_age_expr=Select(_ChildProtectionPlan.CPP_StartDate),
                                                     ignore_time=True,
                                                     time_unit=AgeAtDate.YEARS)
                                           )
    # type: int

    AgeYr_CPP_EndDate = DerivedAttribute('AgeYr_CPP_EndDate ', int,
                                         AgeAtDate(date_of_birth_expr=Select(_ChildProtectionPlan.Root.DateOfBirth),
                                                   date_for_age_expr=Select(_ChildProtectionPlan.CPP_EndDate),
                                                   ignore_time=True,
                                                   time_unit=AgeAtDate.YEARS)
                                         )
    # type: int

    AgeBand_CPP_StartDate = DerivedAttribute(
        'AgeBand_CPP_StartDate', str, AgeBand(age_yr_expr=Select(_ChildProtectionPlan.AgeYr_CPP_StartDate))
    )
    # type: str

    AgeBand_CPP_EndDate = DerivedAttribute(
        'AgeBand_CPP_EndDate	', str, AgeBand(age_yr_expr=Select(_ChildProtectionPlan.AgeYr_CPP_EndDate))
    )
    # type: str


class AssTechToSupportDisabilityType(_AssTechToSupportDisabilityType):
    """CYP404AssTechToSupportDisTyp"""

    # -- auto-generated --

    __table__ = "CYP404AssTechToSupportDisTyp"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP404 = DerivedAttribute(
        'UniqueID_CYP404', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AssTechToSupportDisabilityType.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_AssTechToSupportDisabilityType.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute


class CodedImmunisation(_CodedImmunisation):
    """CYP501CodedImm"""

    # -- auto-generated --

    __table__ = "CYP501CodedImm"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP501 = DerivedAttribute(
        'UniqueID_CYP501', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedImmunisation.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CodedImmunisation.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Age_Immunisation_Date = DerivedAttribute('Age_Immunisation_Date', int,
                                             AgeAtDate(
                                                 date_of_birth_expr=Select(_CodedImmunisation.Root.DateOfBirth),
                                                 date_for_age_expr=Select(_CodedImmunisation.Immunisation_Date),
                                                 ignore_time=True,
                                                 time_unit=AgeAtDate.DAYS)
                                             )
    # type: int

    SchoolYear_Immunisation_Date = DerivedAttribute("SchoolYear_Immunisation_Date", str,
                                                    SchoolYear(date_of_birth_expr=Select(
                                                        _CodedImmunisation.Root.DateOfBirth),
                                                        date_of_activity_expr=Select(
                                                        _CodedImmunisation.Immunisation_Date),
                                                        age_at_activity_expr=Select(
                                                        _CodedImmunisation.AgeYr_Immunisation_Date)
                                                    )
                                                    )
    # type: int

    AgeYr_Immunisation_Date = DerivedAttribute('AgeYr_Immunisation_Date', int,
                                               AgeAtDate(
                                                   date_of_birth_expr=Select(_CodedImmunisation.Root.DateOfBirth),
                                                   date_for_age_expr=Select(_CodedImmunisation.Immunisation_Date),
                                                   ignore_time=True,
                                                   time_unit=AgeAtDate.YEARS)
                                               )
    # type: int

    AgeGroup_Immunisation_Date = DerivedAttribute(
        'AgeGroup_Immunisation_Date', str,
        AgeGroup(age_yrs_expr=Select(_CodedImmunisation.AgeYr_Immunisation_Date))
    )
    # type: str

    AgeBand_Immunisation_Date = DerivedAttribute(
        'AgeBand_Immunisation_Date', str,
        AgeBand(age_yr_expr=Select(_CodedImmunisation.AgeYr_Immunisation_Date))
    )
    # type: str


class Immunisation(_Immunisation):
    """CYP502Imm"""

    # -- auto-generated --

    __table__ = "CYP502Imm"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP502 = DerivedAttribute(
        'UniqueID_CYP502', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Immunisation.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_Immunisation.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Age_Immunisation_Date = DerivedAttribute('Age_Immunisation_Date', int,
                                             AgeAtDate(
                                                 date_of_birth_expr=Select(_Immunisation.Root.DateOfBirth),
                                                 date_for_age_expr=Select(_Immunisation.Immunisation_Date),
                                                 ignore_time=True,
                                                 time_unit=AgeAtDate.DAYS)
                                             )
    # type: int

    SchoolYear_Immunisation_Date = DerivedAttribute("SchoolYear_Immunisation_Date", str,
                                                    SchoolYear(date_of_birth_expr=Select(
                                                        _Immunisation.Root.DateOfBirth),
                                                        date_of_activity_expr=Select(
                                                        _Immunisation.Immunisation_Date),
                                                        age_at_activity_expr=Select(
                                                        _Immunisation.AgeYr_Immunisation_Date)
                                                    )
                                                    )
    # type: int

    AgeYr_Immunisation_Date = DerivedAttribute('AgeYr_Immunisation_Date', int,
                                               AgeAtDate(
                                                   date_of_birth_expr=Select(_Immunisation.Root.DateOfBirth),
                                                   date_for_age_expr=Select(_Immunisation.Immunisation_Date),
                                                   ignore_time=True,
                                                   time_unit=AgeAtDate.YEARS)
                                               )
    # type: int

    AgeBand_Immunisation_Date = DerivedAttribute(
        'AgeBand_Immunisation_Date', str,
        AgeBand(age_yr_expr=Select(_Immunisation.AgeYr_Immunisation_Date))
    )
    # type: str


class MedicalHistory(_MedicalHistory):
    """CYP601MedicalHistory"""

    # -- auto-generated --

    __table__ = "CYP601MedicalHistory"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP601 = DerivedAttribute(
        'UniqueID_CYP601', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MedicalHistory.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_MedicalHistory.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute


class DisabilityType(_DisabilityType):
    """CYP602DisabilityType"""

    # -- auto-generated --

    __table__ = "CYP602DisabilityType"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP602 = DerivedAttribute(
        'UniqueID_CYP602', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_DisabilityType.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_DisabilityType.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute


class NewbornHearingScreening(_NewbornHearingScreening):
    """CYP603NewbornHearingScreening"""

    # -- auto-generated --

    __table__ = "CYP603NewbornHearingScreening"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP603 = DerivedAttribute(
        'UniqueID_CYP603', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_NewbornHearingScreening.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_NewbornHearingScreening.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Age_ServiceRequest_Date = DerivedAttribute('Age_ServiceRequest_Date', int,
                                               AgeAtDate(
                                                   date_of_birth_expr=Select(_NewbornHearingScreening.Root.DateOfBirth),
                                                   date_for_age_expr=Select(
                                                       _NewbornHearingScreening.ServiceRequest_Date),
                                                   ignore_time=True,
                                                   time_unit=AgeAtDate.DAYS)
                                               )
    # type: int

    Age_Procedure_Date = DerivedAttribute('Age_Procedure_Date', int,
                                          AgeAtDate(date_of_birth_expr=Select(_NewbornHearingScreening.Root.DateOfBirth),
                                                    date_for_age_expr=Select(
                                              _NewbornHearingScreening.Procedure_Date),
                                              ignore_time=True,
                                              time_unit=AgeAtDate.DAYS)
                                          )
    # type: int


class BloodSpotResult(_BloodSpotResult):
    """CYP604BloodSpotResult"""

    # -- auto-generated --

    __table__ = "CYP604BloodSpotResult"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP604 = DerivedAttribute(
        'UniqueID_CYP604', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_BloodSpotResult.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_BloodSpotResult.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Days_ReceiveResults = DerivedAttribute('Days_ReceiveResults', int,
                                           AgeAtDate(date_of_birth_expr=Select(_BloodSpotResult.CardCompletion_Date),
                                                     date_for_age_expr=Select(_BloodSpotResult.TestResult_ReceivedDate),
                                                     ignore_time=True,
                                                     time_unit=AgeAtDate.DAYS)
                                           )
    # type: int

    Age_CardCompletion_Date = DerivedAttribute('Age_CardCompletion_Date', int,
                                               AgeAtDate(
                                                   date_of_birth_expr=Select(_BloodSpotResult.Root.DateOfBirth),
                                                   date_for_age_expr=Select(_BloodSpotResult.CardCompletion_Date),
                                                   ignore_time=True,
                                                   time_unit=AgeAtDate.DAYS)
                                               )
    # type: int


class InfantPhysicalExamination(_InfantPhysicalExamination):
    """CYP605IPE"""

    # -- auto-generated --

    __table__ = "CYP605IPE"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP605 = DerivedAttribute(
        'UniqueID_CYP605', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_InfantPhysicalExamination.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_InfantPhysicalExamination.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Age_Examination_Date = DerivedAttribute('Age_Examination_Date', int,
                                            AgeAtDate(
                                                date_of_birth_expr=Select(
                                                    _InfantPhysicalExamination.Root.DateOfBirth),
                                                date_for_age_expr=Select(
                                                    _InfantPhysicalExamination.Examination_Date),
                                                ignore_time=True,
                                                time_unit=AgeAtDate.DAYS)
                                            )
    # type: int


class ProvisionalDiagnosis(_ProvisionalDiagnosis):
    """CYP606ProvDiag"""

    # -- auto-generated --

    __table__ = "CYP606ProvDiag"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP606 = DerivedAttribute(
        'UniqueID_CYP606', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_ProvisionalDiagnosis.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_ProvisionalDiagnosis.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_ProvisionalDiagnosis.Root.Header.OrgID_Provider),
            Select(_ProvisionalDiagnosis.ServiceRequestID)
        ])
    )  # type: str


class PrimaryDiagnosis(_PrimaryDiagnosis):
    """CYP607PrimDiag"""

    # -- auto-generated --

    __table__ = "CYP607PrimDiag"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP607 = DerivedAttribute(
        'UniqueID_CYP607', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_PrimaryDiagnosis.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_PrimaryDiagnosis.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_PrimaryDiagnosis.Root.Header.OrgID_Provider),
            Select(_PrimaryDiagnosis.ServiceRequestID)
        ])
    )  # type: str


class SecondaryDiagnosis(_SecondaryDiagnosis):
    """CYP608SecDiag"""

    # -- auto-generated --

    __table__ = "CYP608SecDiag"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP608 = DerivedAttribute(
        'UniqueID_CYP608', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_SecondaryDiagnosis.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_SecondaryDiagnosis.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_ProvisionalDiagnosis.Root.Header.OrgID_Provider),
            Select(_ProvisionalDiagnosis.ServiceRequestID)
        ])
    )  # type: str


class CodedScoredAssessmentReferral(_CodedScoredAssessmentReferral):
    """CYP609CodedAssessmentReferral"""

    # -- auto-generated --

    __table__ = "CYP609CodedAssessmentReferral"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP609 = DerivedAttribute(
        'UniqueID_CYP609', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessmentReferral.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CodedScoredAssessmentReferral.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_CodedScoredAssessmentReferral.Root.Header.OrgID_Provider),
            Select(_CodedScoredAssessmentReferral.ServiceRequestID)
        ])
    )  # type: str

    Age_AssessmentCompletion_Date = DerivedAttribute('Age_AssessmentCompletion_Date', int,
                                                     AgeAtDate(date_of_birth_expr=Select(
                                                         _CodedScoredAssessmentReferral.Root.DateOfBirth),
                                                         date_for_age_expr=Select(
                                                         _CodedScoredAssessmentReferral.AssessmentCompletion_Date),
                                                         ignore_time=True,
                                                         time_unit=AgeAtDate.DAYS)
                                                     )
    # type: int

    AgeYr_AssessmentCompletion_Date = DerivedAttribute('AgeYr_AssessmentCompletion_Date', int,
                                                       AgeAtDate(date_of_birth_expr=Select(
                                                           _CodedScoredAssessmentReferral.Root.DateOfBirth),
                                                           date_for_age_expr=Select(
                                                           _CodedScoredAssessmentReferral.AssessmentCompletion_Date),
                                                           ignore_time=True,
                                                           time_unit=AgeAtDate.YEARS)
                                                       )
    # type: int

    AgeGroup_AssessmentCompletion_Date = DerivedAttribute(
        'AgeGroup_AssessmentCompletion_Date', str, AgeGroup(age_yrs_expr=Select(
            _CodedScoredAssessmentReferral.AgeYr_AssessmentCompletion_Date))
    )
    # type: str

    AgeBand_AssessmentCompletion_Date = DerivedAttribute(
        'AgeBand_AssessmentCompletion_Date', str, AgeBand(age_yr_expr=Select(
            _CodedScoredAssessmentReferral.AgeYr_AssessmentCompletion_Date))
    )
    # type: str


class BreastfeedingStatus(_BreastfeedingStatus):
    """CYP610BreastfeedingStatus"""

    # -- auto-generated --

    __table__ = "CYP610BreastfeedingStatus"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP610 = DerivedAttribute(
        'UniqueID_CYP610', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_BreastfeedingStatus.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_BreastfeedingStatus.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_CareActivityID = DerivedAttribute(
        'Unique_CareActivityID', str, Concat([
            Select(_BreastfeedingStatus.Root.Header.OrgID_Provider),
            Select(_BreastfeedingStatus.CareActivityID)
        ])
    )  # type: str

    Age_BreastFeedingStatus = DerivedAttribute('Age_BreastFeedingStatus', int,
                                               AgeAtDate(date_of_birth_expr=Select(_BreastfeedingStatus.Root.DateOfBirth),
                                                         date_for_age_expr=Select(_BreastfeedingStatus.Parent.Parent.
                                                                                  Contact_Date),
                                                         ignore_time=True,
                                                         time_unit=AgeAtDate.DAYS)
                                               )

    AgeYr_BreastFeedingStatus = DerivedAttribute('AgeYr_BreastFeedingStatus', int,
                                                 AgeAtDate(date_of_birth_expr=Select(
                                                     _BreastfeedingStatus.Root.DateOfBirth),
                                                     date_for_age_expr=Select(_BreastfeedingStatus.Parent.Parent.
                                                                              Contact_Date),
                                                     ignore_time=True,
                                                     time_unit=AgeAtDate.YEARS)
                                                 )
    # type: int

    AgeBand_BreastFeedingStatus = DerivedAttribute(
        'AgeBand_BreastFeedingStatus', str, AgeBand(age_yr_expr=Select(_BreastfeedingStatus.AgeYr_BreastFeedingStatus))
    )
    # type: str


class Observation(_Observation):
    """CYP611Obs"""

    # -- auto-generated --

    __table__ = "CYP611Obs"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP611 = DerivedAttribute(
        'UniqueID_CYP611', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Observation.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_Observation.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_CareActivityID = DerivedAttribute(
        'Unique_CareActivityID', str, Concat([
            Select(_Observation.Root.Header.OrgID_Provider),
            Select(_Observation.CareActivityID)
        ])
    )  # type: str

    Age_BMI_Observation = DerivedAttribute('Age_BMI_Observation', int,
                                           AgeAtDate(date_of_birth_expr=Select(_Observation.Root.DateOfBirth),
                                                     date_for_age_expr=Select(
                                               _Observation.Parent.Parent.Contact_Date),
                                               ignore_time=True,
                                               time_unit=AgeAtDate.DAYS)
                                           )  # type: int

    SchoolYear_BMI_Observation = DerivedAttribute("SchoolYear_BMI_Observation", str,
                                                  SchoolYear(date_of_birth_expr=Select(
                                                      _Observation.Root.DateOfBirth),
                                                      date_of_activity_expr=Select(
                                                      _Observation.Parent.Parent.Contact_Date),
                                                      age_at_activity_expr=Select(
                                                      _Observation.AgeYr_BMI_Observation)
                                                  )
                                                  )
    # type: int

    AgeYr_BMI_Observation = DerivedAttribute('AgeYr_BMI_Observation', int,
                                             AgeAtDate(date_of_birth_expr=Select(_Observation.Root.DateOfBirth),
                                                       date_for_age_expr=Select(
                                                 _Observation.Parent.Parent.Contact_Date),
                                                 ignore_time=True,
                                                 time_unit=AgeAtDate.YEARS)
                                             )
    # type: int

    AgeGroup_BMI_Observation = DerivedAttribute(
        'AgeGroup_BMI_Observation', str, AgeGroup(age_yrs_expr=Select(_Observation.AgeYr_BMI_Observation))
    )
    # type: str

    AgeBand_BMI_Observation = DerivedAttribute(
        'AgeBand_BMI_Observation', str, AgeBand(age_yr_expr=Select(_Observation.AgeYr_BMI_Observation))
    )
    # type: str


class CodedScoredAssessmentContact(_CodedScoredAssessmentContact):
    """CYP612CodedAssessmentContact"""

    # -- auto-generated --

    __table__ = "CYP612CodedAssessmentContact"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqueID_CYP612 = DerivedAttribute(
        'UniqueID_CYP612', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessmentContact.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CodedScoredAssessmentContact.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_CareActivityID = DerivedAttribute(
        'Unique_CareActivityID', str, Concat([
            Select(_CodedScoredAssessmentContact.Root.Header.OrgID_Provider),
            Select(_CodedScoredAssessmentContact.CareActivityID)
        ])
    )  # type: str

    Age_AssessmentTool_Contact_Date = DerivedAttribute('Age_AssessmentTool_Contact_Date', int,
                                                       AgeAtDate(date_of_birth_expr=Select(
                                                           _CodedScoredAssessmentContact.Root.DateOfBirth),
                                                           date_for_age_expr=Select(_CodedScoredAssessmentContact.
                                                                                    Parent.Parent.Contact_Date),
                                                           ignore_time=True,
                                                           time_unit=AgeAtDate.DAYS)
                                                       )
    # type: int

    ASQ_ScoreBand = DerivedAttribute(
        'ASQ_ScoreBand', str, ASQScoreBand(
            score=Select(_CodedScoredAssessmentContact.Score),
            snomed_id=Select(_CodedScoredAssessmentContact.SNOMED_ID)
        )
    )  # type: str

    SNOMEDCTAssTerm = DerivedAttribute(
        'SNOMEDCTAssTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CodedScoredAssessmentContact.SNOMED_ID),
                             point_in_time_expr=Select(_CodedScoredAssessmentContact.Parent.Parent.Contact_Date))
        )
    )  # type: DerivedAttribute


class CareActivity(_CareActivity):
    """CYP202CareActivity"""

    # -- auto-generated --

    __table__ = "CYP202CareActivity"
    __concrete__ = True

    BreastfeedingStatuses = RepeatingSubmittedAttribute(
        'BreastfeedingStatuses', BreastfeedingStatus)  # type: List[BreastfeedingStatus]
    Observations = RepeatingSubmittedAttribute('Observations', Observation)  # type: List[Observation]
    CodedScoredAssessmentContacts = RepeatingSubmittedAttribute(
        'CodedScoredAssessmentContacts', CodedScoredAssessmentContact)  # type: List[CodedScoredAssessmentContact]

    # ^^ auto-generated ^^

    UniqueID_CYP202 = DerivedAttribute(
        'UniqueID_CYP202', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareActivity.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CareActivity.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_CareContactID = DerivedAttribute(
        'Unique_CareContactID', str, Concat([
            Select(_CareActivity.Root.Header.OrgID_Provider),
            Select(_CareActivity.CareContactID)
        ])
    )  # type: str

    Unique_CareActivityID = DerivedAttribute(
        'Unique_CareActivityID', str, Concat([
            Select(_CareActivity.Root.Header.OrgID_Provider),
            Select(_CareActivity.CareActivityID)
        ])
    )  # type: str

    BreastFeedingStatus_Master = DerivedAttribute(
        'BreastFeedingStatus_Master', str, BreastFeedingStatusMaster(
            coded_finding=Select(_CareActivity.CodedFinding),
            breast_feeding_status=Select(_CareActivity.BreastfeedingStatuses)
        ))  # type: str


class CareContact(_CareContact):
    """CYP201CareContact"""

    # -- auto-generated --

    __table__ = "CYP201CareContact"
    __concrete__ = True

    CareActivities = RepeatingSubmittedAttribute('CareActivities', CareActivity)  # type: List[CareActivity]

    # ^^ auto-generated ^^

    UniqueID_CYP201 = DerivedAttribute(
        'UniqueID_CYP201', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareContact.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CareContact.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_CareContact.Root.Header.OrgID_Provider),
            Select(_CareContact.ServiceRequestID)
        ])
    )  # type: str

    Unique_TeamID_Local = DerivedAttribute(
        'Unique_TeamID_Local', str,
        If(NotNull(Select(_CareContact.TeamID_Local)),
           then=Concat([
               Select(_CareContact.Root.Header.OrgID_Provider),
               Select(_CareContact.TeamID_Local)]),
           otherwise=Literal(None)))  # type: str

    Unique_CareContactID = DerivedAttribute(
        'Unique_CareContactID', str, Concat([
            Select(_CareContact.Root.Header.OrgID_Provider),
            Select(_CareContact.CareContactID)
        ])
    )  # type: str

    Days_Referral_to_CareContact = DerivedAttribute('Days_Referral_to_CareContact', int,
                                                    AgeAtDate(date_of_birth_expr=Select(
                                                        _CareContact.Parent.ReferralRequest_ReceivedDate),
                                                        date_for_age_expr=Select(_CareContact.Contact_Date),
                                                        ignore_time=True,
                                                        time_unit=AgeAtDate.DAYS)
                                                    )
    # type: int

    Age_Contact_Date = DerivedAttribute('Age_Contact_Date', int,
                                        AgeAtDate(date_of_birth_expr=Select(_CareContact.Root.DateOfBirth),
                                                  date_for_age_expr=Select(_CareContact.Contact_Date),
                                                  ignore_time=True,
                                                  time_unit=AgeAtDate.DAYS)
                                        )
    # type: int

    AgeYr_Contact_Date = DerivedAttribute('AgeYr_Contact_Date', int,
                                          AgeAtDate(date_of_birth_expr=Select(_CareContact.Root.DateOfBirth),
                                                    date_for_age_expr=Select(_CareContact.Contact_Date),
                                                    ignore_time=True,
                                                    time_unit=AgeAtDate.YEARS)
                                          )
    # type: int

    AgeGroup_Contact_Date = DerivedAttribute(
        'AgeGroup_Contact_Date', str, AgeGroup(age_yrs_expr=Select(_CareContact.AgeYr_Contact_Date))
    )
    # type: str

    AgeBand_Contact_Date = DerivedAttribute(
        'AgeBand_Contact_Date', str, AgeBand(age_yr_expr=Select(_CareContact.AgeYr_Contact_Date))
    )
    # type: str

    DistanceFromHome_ContactLocation = DerivedAttribute('DistanceFromHome_ContactLocation', int, (
        If(
            ValidPostcode(postcode_expr=Select(_CareContact.Root.Postcode), point_in_time_expr=Select(
                _CareContact.Root.Header.RP_StartDate)),
            then=(
                OrgDistance(org_code_expr=Select(_CareContact.Treatment_OrgSiteID),
                            from_postcode_expr=Select(_CareContact.Root.Postcode),
                            point_in_time_expr=Select(_CareContact.Root.Header.RP_StartDate))
            ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute


class Referral(_Referral):
    """CYP101Referral"""

    # -- auto-generated --

    __table__ = "CYP101Referral"
    __concrete__ = True

    ServiceTypesReferredTo = RepeatingSubmittedAttribute(
        'ServiceTypesReferredTo', ServiceTypeReferredTo)  # type: List[ServiceTypeReferredTo]
    OtherReasonsForReferral = RepeatingSubmittedAttribute(
        'OtherReasonsForReferral', OtherReasonForReferral)  # type: List[OtherReasonForReferral]
    ReferralsToTreatment = RepeatingSubmittedAttribute(
        'ReferralsToTreatment', ReferralToTreatment)  # type: List[ReferralToTreatment]
    OnwardReferrals = RepeatingSubmittedAttribute('OnwardReferrals', OnwardReferral)  # type: List[OnwardReferral]
    CareContacts = RepeatingSubmittedAttribute('CareContacts', CareContact)  # type: List[CareContact]
    ProvisionalDiagnoses = RepeatingSubmittedAttribute(
        'ProvisionalDiagnoses', ProvisionalDiagnosis)  # type: List[ProvisionalDiagnosis]
    PrimaryDiagnoses = RepeatingSubmittedAttribute('PrimaryDiagnoses', PrimaryDiagnosis)  # type: List[PrimaryDiagnosis]
    SecondaryDiagnoses = RepeatingSubmittedAttribute(
        'SecondaryDiagnoses', SecondaryDiagnosis)  # type: List[SecondaryDiagnosis]
    CodedScoredAssessmentReferrals = RepeatingSubmittedAttribute(
        'CodedScoredAssessmentReferrals', CodedScoredAssessmentReferral)  # type: List[CodedScoredAssessmentReferral]

    # ^^ auto-generated ^^

    UniqueID_CYP101 = DerivedAttribute(
        'UniqueID_CYP101', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Referral.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_Referral.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_ServiceRequestID = DerivedAttribute(
        'Unique_ServiceRequestID', str, Concat([
            Select(_Referral.Root.Header.OrgID_Provider),
            Select(_Referral.ServiceRequestID)
        ])
    )  # type: str

    Age_Referral_ReceivedDate = DerivedAttribute('Age_Referral_ReceivedDate', int,
                                                 AgeAtDate(date_of_birth_expr=Select(_Referral.Root.DateOfBirth),
                                                           date_for_age_expr=Select(
                                                     _Referral.ReferralRequest_ReceivedDate),
                                                     ignore_time=True,
                                                     time_unit=AgeAtDate.DAYS)
                                                 )
    # type: int

    Age_Service_Discharge_Date = DerivedAttribute('Age_Service_Discharge_Date', int,
                                                  AgeAtDate(date_of_birth_expr=Select(_Referral.Root.DateOfBirth),
                                                            date_for_age_expr=Select(_Referral.Discharge_Date),
                                                            ignore_time=True,
                                                            time_unit=AgeAtDate.DAYS)
                                                  )
    # type: int

    AgeYr_Referral_ReceivedDate = DerivedAttribute('AgeYr_Referral_ReceivedDate', int,
                                                   AgeAtDate(date_of_birth_expr=Select(_Referral.Root.DateOfBirth),
                                                             date_for_age_expr=Select(
                                                       _Referral.ReferralRequest_ReceivedDate),
                                                       ignore_time=True,
                                                       time_unit=AgeAtDate.YEARS)
                                                   )
    # type: int

    AgeYr_Service_DischargeDate = DerivedAttribute('AgeYr_Service_DischargeDate', int,
                                                   AgeAtDate(date_of_birth_expr=Select(_Referral.Root.DateOfBirth),
                                                             date_for_age_expr=Select(_Referral.Discharge_Date),
                                                             ignore_time=True,
                                                             time_unit=AgeAtDate.YEARS)
                                                   )
    # type: int

    AgeGroup_Referral_ReceivedDate = DerivedAttribute(
        'AgeGroup_Referral_ReceivedDate', str, AgeGroup(age_yrs_expr=Select(_Referral.AgeYr_Referral_ReceivedDate))
    )
    # type: str

    AgeGroup_Service_DischargeDate = DerivedAttribute(
        'AgeGroup_Service_DischargeDate', str, AgeGroup(age_yrs_expr=Select(_Referral.AgeYr_Service_DischargeDate))
    )
    # type: str

    AgeBand_Referral_ReceivedDate = DerivedAttribute(
        'AgeBand_Referral_ReceivedDate', str, AgeBand(age_yr_expr=Select(_Referral.AgeYr_Referral_ReceivedDate))
    )
    # type: str

    AgeBand_Service_DischargeDate = DerivedAttribute(
        'AgeBand_Service_DischargeDate', str, AgeBand(age_yr_expr=Select(_Referral.AgeYr_Service_DischargeDate))
    )
    # type: str


class CarePlanType(_CarePlanType):
    """CYP004CarePlanType"""

    # -- auto-generated --

    __table__ = "CYP004CarePlanType"
    __concrete__ = True

    CarePlanAgreements = RepeatingSubmittedAttribute(
        'CarePlanAgreements', CarePlanAgreement)  # type: List[CarePlanAgreement]

    # ^^ auto-generated ^^

    UniqueID_CYP004 = DerivedAttribute(
        'UniqueID_CYP004', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CarePlanType.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_CarePlanType.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_PlanID = DerivedAttribute(
        'Unique_PlanID', str, Concat([
            Select(_CarePlanType.Root.Header.OrgID_Provider),
            Select(_CarePlanType.PlanID)
        ])
    )  # type: str


class MPI(_MPI):
    """CYP001MPI"""

    # -- auto-generated --

    __table__ = "CYP001MPI"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    GPPracticeRegistrations = RepeatingSubmittedAttribute(
        'GPPracticeRegistrations', GPPracticeRegistration)  # type: List[GPPracticeRegistration]
    AccommodationTypes = RepeatingSubmittedAttribute(
        'AccommodationTypes', AccommodationType)  # type: List[AccommodationType]
    CarePlanTypes = RepeatingSubmittedAttribute('CarePlanTypes', CarePlanType)  # type: List[CarePlanType]
    SocialAndPersonalCircumstances = RepeatingSubmittedAttribute(
        'SocialAndPersonalCircumstances', SocialAndPersonalCircumstances)  # type: List[SocialAndPersonalCircumstances]
    EmploymentStatuses = RepeatingSubmittedAttribute(
        'EmploymentStatuses', EmploymentStatus)  # type: List[EmploymentStatus]
    OverseasVisitorChargeCats = RepeatingSubmittedAttribute(
        'OverseasVisitorChargeCats', OverseasVisitorChargeCat)  # type: List[OverseasVisitorChargeCat]
    Referrals = RepeatingSubmittedAttribute('Referrals', Referral)  # type: List[Referral]
    SpecialEducationalNeeds = RepeatingSubmittedAttribute(
        'SpecialEducationalNeeds', SpecialEducationalNeed)  # type: List[SpecialEducationalNeed]
    SafeguardingVulnerabilityFactors = RepeatingSubmittedAttribute(
        'SafeguardingVulnerabilityFactors', SafeguardingVulnerabilityFactor)  # type: List[SafeguardingVulnerabilityFactor]
    ChildProtectionPlans = RepeatingSubmittedAttribute(
        'ChildProtectionPlans', ChildProtectionPlan)  # type: List[ChildProtectionPlan]
    AssTechToSupportDisabilityTypes = RepeatingSubmittedAttribute(
        'AssTechToSupportDisabilityTypes', AssTechToSupportDisabilityType)  # type: List[AssTechToSupportDisabilityType]
    CodedImmunisations = RepeatingSubmittedAttribute(
        'CodedImmunisations', CodedImmunisation)  # type: List[CodedImmunisation]
    Immunisations = RepeatingSubmittedAttribute('Immunisations', Immunisation)  # type: List[Immunisation]
    MedicalHistories = RepeatingSubmittedAttribute('MedicalHistories', MedicalHistory)  # type: List[MedicalHistory]
    DisabilityTypes = RepeatingSubmittedAttribute('DisabilityTypes', DisabilityType)  # type: List[DisabilityType]
    NewbornHearingScreenings = RepeatingSubmittedAttribute(
        'NewbornHearingScreenings', NewbornHearingScreening)  # type: List[NewbornHearingScreening]
    BloodSpotResults = RepeatingSubmittedAttribute('BloodSpotResults', BloodSpotResult)  # type: List[BloodSpotResult]
    InfantPhysicalExaminations = RepeatingSubmittedAttribute(
        'InfantPhysicalExaminations', InfantPhysicalExamination)  # type: List[InfantPhysicalExamination]

    # ^^ auto-generated ^^

    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)

    RecordNumber = DerivedAttribute(
        'RecordNumber', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MPI.Header.UniqueSubmissionID),
            row_number_expr=Select(_MPI.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    UniqueID_CYP001 = DerivedAttribute(
        'UniqueID_CYP001', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MPI.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_MPI.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_MonthID = DerivedAttribute('Unique_MonthID', int,
                                      UniqMonth(date_expr=Select(_MPI.Header.RP_StartDate))
                                      )
    # type: DerivedAttribute

    Age_RP_StartDate = DerivedAttribute('Age_RP_StartDate', int,
                                        AgeAtDate(date_of_birth_expr=Select(_MPI.DateOfBirth),
                                                  date_for_age_expr=Select(_MPI.Root.Header.RP_StartDate),
                                                  ignore_time=True,
                                                  time_unit=AgeAtDate.DAYS)
                                        )
    # type: int

    Age_RP_EndDate = DerivedAttribute('Age_RP_EndDate', int,
                                      AgeAtDate(date_of_birth_expr=Select(_MPI.DateOfBirth),
                                                date_for_age_expr=Select(_MPI.Root.Header.RP_EndDate),
                                                ignore_time=True,
                                                time_unit=AgeAtDate.DAYS)
                                      )
    # type: int

    Age_Death = DerivedAttribute('Age_Death', int,
                                 AgeAtDate(date_of_birth_expr=Select(_MPI.DateOfBirth),
                                           date_for_age_expr=Select(_MPI.DateOfDeath),
                                           ignore_time=True,
                                           time_unit=AgeAtDate.DAYS)
                                 )
    # type: int

    AgeYr_RP_StartDate = DerivedAttribute('AgeYr_RP_StartDate', int,
                                          AgeAtDate(date_of_birth_expr=Select(_MPI.DateOfBirth),
                                                    date_for_age_expr=Select(_MPI.Root.Header.RP_StartDate),
                                                    ignore_time=True,
                                                    time_unit=AgeAtDate.YEARS)
                                          )
    # type: int

    AgeYr_RP_EndDate = DerivedAttribute('AgeYr_RP_EndDate', int,
                                        AgeAtDate(date_of_birth_expr=Select(_MPI.DateOfBirth),
                                                  date_for_age_expr=Select(_MPI.Root.Header.RP_EndDate),
                                                  ignore_time=True,
                                                  time_unit=AgeAtDate.YEARS)
                                        )
    # type: int

    AgeYr_Death = DerivedAttribute('AgeYr_Death', int,
                                   AgeAtDate(date_of_birth_expr=Select(_MPI.DateOfBirth),
                                             date_for_age_expr=Select(_MPI.DateOfDeath),
                                             ignore_time=True,
                                             time_unit=AgeAtDate.YEARS)
                                   )
    # type: int

    AgeGroup_RP_StartDate = DerivedAttribute(
        'AgeGroup_RP_StartDate', str, AgeGroup(age_yrs_expr=Select(_MPI.AgeYr_RP_StartDate))
    )
    # type: str

    AgeGroup_RP_EndDate = DerivedAttribute(
        'AgeGroup_RP_EndDate', str, AgeGroup(age_yrs_expr=Select(_MPI.AgeYr_RP_EndDate))
    )
    # type: str

    AgeGroup_Death = DerivedAttribute(
        'AgeGroup_Death', str, AgeGroup(age_yrs_expr=Select(_MPI.AgeYr_Death))
    )
    # type: str

    AgeBand_RP_StartDate = DerivedAttribute(
        'AgeBand_RP_StartDate', str, AgeBand(age_yr_expr=Select(_MPI.AgeYr_RP_StartDate))
    )
    # type: str

    AgeBand_RP_EndDate = DerivedAttribute(
        'AgeBand_RP_EndDate', str, AgeBand(age_yr_expr=Select(_MPI.AgeYr_RP_EndDate))
    )
    # type: str

    AgeBand_Death = DerivedAttribute(
        'AgeBand_Death', str, AgeBand(age_yr_expr=Select(_MPI.AgeYr_Death))
    )
    # type: str

    ValidPostcode_Flag = DerivedAttribute(
        'ValidPostcode_Flag', str, If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode), point_in_time_expr=Select(
                _MPI.Root.Header.RP_StartDate)), then=Literal('Y'), otherwise=Literal('N'))
    )  # type: DerivedAttribute

    Postcode_District = DerivedAttribute('Postcode_District', str, (
        DerivePostcodeDistrict(postcode_expr=Select(_MPI.Postcode),
                               point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate))
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
                          point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate)),
            then=(DerivePostcode(postcode_expr=Select(_MPI.Postcode),
                                 point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate),
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

    County = DerivedAttribute('County', str, (
        If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                          point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate)),
            then=(DerivePostcode(postcode_expr=Select(_MPI.Postcode),
                                 point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate),
                                 field_name=ONSRecordPaths.RESIDENCE_COUNTY)
                  ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    LocalAuthority = DerivedAttribute('LocalAuthority', str, (
        If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                          point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate)),
            then=(DerivePostcode(postcode_expr=Select(_MPI.Postcode),
                                 point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate),
                                 field_name=ONSRecordPaths.UNITARY_AUTHORITY)
                  ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    ElectoralWard = DerivedAttribute('ElectoralWard', str, (
        If(
            ValidPostcode(postcode_expr=Select(_MPI.Postcode),
                          point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate)),
            then=(DerivePostcode(postcode_expr=Select(_MPI.Postcode),
                                 point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate),
                                 field_name=ONSRecordPaths.OS_WARD_2011)
                  ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    OrgID_CCG_Residence = DerivedAttribute('OrgID_CCG_Residence', str, (
        If(
            DateBeforeOrEqualTo(
                eval_expr=(Select(_MPI.Root.Header.RP_StartDate)), check_date_expr=Literal(CCG_ICB_SWITCHOVER_PERIOD_END)),
            then=(
                If(
                    ValidPostcode(
                        postcode_expr=Select(_MPI.Postcode),
                        point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate)),
                    then=(CcgFromPostcode(
                        postcode_expr=Select(_MPI.Postcode),
                        event_date=Select(_MPI.Root.Header.RP_StartDate),
                        enforce_icb_switchover_period=Literal(True))),
                    otherwise=Literal(None))),
            otherwise=Literal(None))))  # type: DerivedAttribute

    OrgIDSubICBLocResidence = DerivedAttribute('OrgIDSubICBLocResidence', str, (
        If(
            ValidPostcode(
                postcode_expr=Select(_MPI.Postcode), point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate)
            ),
            then=(
                SubICBFromPostcode(
                    postcode_expr=Select(_MPI.Postcode), event_date=Select(_MPI.Root.Header.RP_StartDate)
                )
            ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    OrgIDICBRes = DerivedAttribute('OrgIDICBRes', str, (
        If(
            ValidPostcode(
                postcode_expr=Select(_MPI.Postcode), point_in_time_expr=Select(_MPI.Root.Header.RP_StartDate)
            ),
            then=(
                ICBFromPostcode(
                    postcode_expr=Select(_MPI.Postcode),
                    event_date=Select(_MPI.Root.Header.RP_StartDate)
                )
            ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    ValidNHSNumber_Flag = DerivedAttribute(
        'ValidNHSNumber_Flag', str,
        If(NotNull(Select(_MPI.NHSNumber)),
           then=Literal('Y'), otherwise=Literal('N')))  # type: DerivedAttribute


class GroupSession(_GroupSession):
    """CYP301GroupSession"""

    # -- auto-generated --

    __table__ = "CYP301GroupSession"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqueID_CYP301 = DerivedAttribute(
        'UniqueID_CYP301', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_GroupSession.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_GroupSession.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_MonthID = DerivedAttribute('Unique_MonthID', int, UniqMonth(
        date_expr=Select(_GroupSession.Header.RP_StartDate)))  # type: DerivedAttribute


class AnonSelfAssessment(_AnonSelfAssessment):
    """CYP613AnonSelfAssessment"""

    # -- auto-generated --

    __table__ = "CYP613AnonSelfAssessment"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqueID_CYP613 = DerivedAttribute(
        'UniqueID_CYP613', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AnonSelfAssessment.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_AnonSelfAssessment.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_MonthID = DerivedAttribute('Unique_MonthID', int, UniqMonth(
        date_expr=Select(_AnonSelfAssessment.Header.RP_StartDate)))  # type: DerivedAttribute


class StaffDetails(_StaffDetails):
    """CYP901StaffDetails"""

    # -- auto-generated --

    __table__ = "CYP901StaffDetails"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    UniqueID_CYP901 = DerivedAttribute(
        'UniqueID_CYP901', Decimal_23_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_StaffDetails.Root.Header.UniqueSubmissionID),
            row_number_expr=Select(_StaffDetails.RowNumber),
            return_decimal=True
        )
    )  # type: DerivedAttribute

    Unique_MonthID = DerivedAttribute('Unique_MonthID', int, UniqMonth(
        date_expr=Select(_StaffDetails.Header.RP_StartDate)))  # type: DerivedAttribute


def get_all_models() -> Generator[Type[BaseCSDS_V1_6], None, None]:
    clsmembers = [
        name for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
        if obj.__module__ is __name__
    ]

    for member in clsmembers:
        if not member.startswith('_') and not member.startswith('BaseCSDS_V1_6'):
            cls_obj = getattr(sys.modules[__name__], member)
            if safe_issubclass(cls_obj, BaseCSDS_V1_6):
                yield cls_obj


def get_anonymous_models():
    return [AnonSelfAssessment, GroupSession, StaffDetails]
