# pylint: disable=line-too-long,too-many-lines
import inspect
import sys
from datetime import datetime, date
from typing import Generator, List, Type
from dsp.common.constants import CCG_ICB_SWITCHOVER_PERIOD_END

from dsp.datasets.models.msds_base import *
from dsp.datasets.models.msds_constants import (
    ObservationSchemes, FindingSchemes, ProcedureSchemes,
    SnomedCodeSystemConcept
)
from dsp.model.ons_record import ONSRecordPaths
from dsp.common.expressions import *
from dsp.common.msds_expressions import *
from dsp.common.structured_model import (
    DerivedAttribute,
    META,
    RepeatingSubmittedAttribute,
    SubmittedAttribute,
    MPSConfidenceScores,
    AssignableAttribute,
    BaseTableModel,
    Decimal_20_0, Decimal_20_2)
from dsp.shared import safe_issubclass

__all__ = [
    'AnonFindings',
    'AnonSelfAssessment',
    'AssignedCareProfessional',
    'BabyDemographics',
    'CareActivity',
    'CareActivityBaby',
    'CareActivityLabourAndDelivery',
    'CareContact',
    'CodedScoredAssessment',
    'CodedScoredAssessmentBaby',
    'CodedScoredAssessmentPregnancy',
    'DatingScanProcedure',
    'DiagnosisNeonatal',
    'DiagnosisPregnancy',
    'FamilyHistoryAtBooking',
    'FindingAndObservationMother',
    'GPPracticeRegistration',
    'Header',
    'HospitalProviderSpell',
    'HospitalSpellCommissioner',
    'LabourAndDelivery',
    'MaternityCarePlan',
    'MedicalHistoryPreviousDiagnosis',
    'MotherDemog',
    'NeonatalAdmission',
    'OverseasVisitorChargingCategory',
    'PregnancyAndBookingDetails',
    'ProvisionalDiagnosisNeonatal',
    'ProvisionalDiagnosisPregnancy',
    'SocialAndPersonalCircumstances',
    'StaffDetails',
    'WardStay',
    'get_all_models',
]


class BaseMSD(BaseTableModel):

    RowNumber = SubmittedAttribute('RowNumber', int)  # type: SubmittedAttribute


class Header(_Header):
    """MSD000Header"""

    # -- auto-generated --

    __table__ = "MSD000Header"
    __concrete__ = True

    # ^^ auto-generated ^^

    UniqSubmissionID = DerivedAttribute(
        'UniqSubmissionID', int, SubmissionId(meta_event_id_expr=Select(_Header.Root.META.EVENT_ID))
    )  # type: DerivedAttribute

    MSD000_ID = DerivedAttribute(
        'MSD000_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_Header.UniqSubmissionID),
            row_number_expr=Select(_Header.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    OrgCodeProvider = DerivedAttribute(
        'OrgCodeProvider', str, Select(_Header.OrgIDProvider))  # type: DerivedAttribute


class GPPracticeRegistration(_GPPracticeRegistration):
    """MSD002GP"""

    # -- auto-generated --

    __table__ = "MSD002GP"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD002_ID = DerivedAttribute(
        'MSD002_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_GPPracticeRegistration.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_GPPracticeRegistration.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    CCGResponsibilityMother = DerivedAttribute('CCGResponsibilityMother', str,(
                                            If(
                                                DateBeforeOrEqualTo(
                                                eval_expr=(Select(_GPPracticeRegistration.Root.Header.RPStartDate)), 
                                                check_date_expr=Literal(CCG_ICB_SWITCHOVER_PERIOD_END)),
                                                then=(
                                                CCGFromGPPracticeCode(
                                                    gp_practice_code=Select(_GPPracticeRegistration.OrgCodeGMPMother),
                                                    event_date=Select(_GPPracticeRegistration.Root.Header.RPStartDate),
                                                    enforce_icb_switchover_period=Literal(True))
                                                ),
                                                otherwise=Literal(None)))
                                            )  # type: DerivedAttribute
                                                                               

    OrgIDSubICBLocGP = DerivedAttribute('OrgIDSubICBLocGP', str,
                                        SubICBFromGPPracticeCode(
                                            gp_practice_code=Select(_GPPracticeRegistration.OrgCodeGMPMother),
                                            event_date=Select(_GPPracticeRegistration.Root.Header.RPStartDate))
                                        )  # type: DerivedAttribute

    OrgIDICBGPPractice = DerivedAttribute('OrgIDICBGPPractice', str,
                                          ICBFromSubICB(
                                              sub_icb=Select(_GPPracticeRegistration.OrgIDSubICBLocGP),
                                              event_date=Select(_GPPracticeRegistration.Root.Header.RPStartDate))
                                          )  # type: DerivedAttribute


class SocialAndPersonalCircumstances(_SocialAndPersonalCircumstances):
    """MSD003SocPersCircumstances"""

    # -- auto-generated --

    __table__ = "MSD003SocPersCircumstances"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD003_ID = DerivedAttribute(
        'MSD003_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_SocialAndPersonalCircumstances.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_SocialAndPersonalCircumstances.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class OverseasVisitorChargingCategory(_OverseasVisitorChargingCategory):
    """MSD004OverseasVisChargCat"""

    # -- auto-generated --

    __table__ = "MSD004OverseasVisChargCat"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD004_ID = DerivedAttribute(
        'MSD004_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_OverseasVisitorChargingCategory.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_OverseasVisitorChargingCategory.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class MaternityCarePlan(_MaternityCarePlan):
    """MSD102MatCarePlan"""

    # -- auto-generated --

    __table__ = "MSD102MatCarePlan"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD102_ID = DerivedAttribute(
        'MSD102_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MaternityCarePlan.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MaternityCarePlan.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class DatingScanProcedure(_DatingScanProcedure):
    """MSD103DatingScan"""

    # -- auto-generated --

    __table__ = "MSD103DatingScan"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD103_ID = DerivedAttribute(
        'MSD103_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_DatingScanProcedure.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_DatingScanProcedure.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    GestAgeDatUltraDate = DerivedAttribute(
        'GestAgeDatUltraDate', int,
        GestationalAge(_DatingScanProcedure.ProcedureDateDatingUltrasound, _DatingScanProcedure.Parent),
    )  # type: DerivedAttribute


class CodedScoredAssessmentPregnancy(_CodedScoredAssessmentPregnancy):
    """MSD104CodedScoreAssPreg"""

    # -- auto-generated --

    __table__ = "MSD104CodedScoreAssPreg"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD104_ID = DerivedAttribute(
        'MSD104_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessmentPregnancy.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CodedScoredAssessmentPregnancy.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    GestAgeAssToolDate = DerivedAttribute(
        'GestAgeAssToolDate', int,
        GestationalAge(_CodedScoredAssessmentPregnancy.CompDate, _CodedScoredAssessmentPregnancy.Parent)
    )  # type: DerivedAttribute


class ProvisionalDiagnosisPregnancy(_ProvisionalDiagnosisPregnancy):
    """MSD105ProvDiagnosisPreg"""

    # -- auto-generated --

    __table__ = "MSD105ProvDiagnosisPreg"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD105_ID = DerivedAttribute(
        'MSD105_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_ProvisionalDiagnosisPregnancy.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_ProvisionalDiagnosisPregnancy.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    GestAgeProvDiagDate = DerivedAttribute(
        'GestAgeProvDiagDate', int,
        GestationalAge(_ProvisionalDiagnosisPregnancy.ProvDiagDate, _ProvisionalDiagnosisPregnancy.Parent)
    )  # type: DerivedAttribute

    MapSnomedCTProvDiagCode = DerivedAttribute('MapSnomedCTProvDiagCode', int, DefaultNone())
    MapSnomedCTProvDiagTerm = DerivedAttribute('MapSnomedCTProvDiagTerm', str, DefaultNone())
    MapICD10ProvCode = DerivedAttribute('MapICD10ProvCode', str, DefaultNone())
    MapICD10ProvDesc = DerivedAttribute('MapICD10ProvDesc', str, DefaultNone())
    MasterICD10ProvCode = DerivedAttribute('MasterICD10ProvCode', str, DefaultNone())
    MasterICD10ProvDesc = DerivedAttribute('MasterICD10ProvDesc', str, DefaultNone())


class DiagnosisPregnancy(_DiagnosisPregnancy):
    """MSD106DiagnosisPreg"""

    # -- auto-generated --

    __table__ = "MSD106DiagnosisPreg"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD106_ID = DerivedAttribute(
        'MSD106_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_DiagnosisPregnancy.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_DiagnosisPregnancy.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    GestAgeDiagDate = DerivedAttribute(
        'GestAgeDiagDate', int,
        GestationalAge(_DiagnosisPregnancy.DiagDate, _DiagnosisPregnancy.Parent)
    )  # type: DerivedAttribute

    MapSnomedCTDiagCode = DerivedAttribute('MapSnomedCTDiagCode', int, DefaultNone())
    MapSnomedCTDiagTerm = DerivedAttribute('MapSnomedCTDiagTerm', str, DefaultNone())
    MapICD10Code = DerivedAttribute('MapICD10Code', str, DefaultNone())
    MapICD10Desc = DerivedAttribute('MapICD10Desc', str, DefaultNone())
    MasterICD10Code = DerivedAttribute('MasterICD10Code', str, DefaultNone())
    MasterICD10Desc = DerivedAttribute('MasterICD10Desc', str, DefaultNone())


class MedicalHistoryPreviousDiagnosis(_MedicalHistoryPreviousDiagnosis):
    """MSD107MedHistory"""

    # -- auto-generated --

    __table__ = "MSD107MedHistory"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD107_ID = DerivedAttribute(
        'MSD107_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MedicalHistoryPreviousDiagnosis.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MedicalHistoryPreviousDiagnosis.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    MapSnomedCTPrevDiagCode = DerivedAttribute('MapSnomedCTPrevDiagCode', int, DefaultNone())
    MapSnomedCTPrevDiagTerm = DerivedAttribute('MapSnomedCTPrevDiagTerm', str, DefaultNone())
    MapICD10PrevCode = DerivedAttribute('MapICD10PrevCode', str, DefaultNone())
    MapICD10PrevDesc = DerivedAttribute('MapICD10PrevDesc', str, DefaultNone())
    MasterICD10PrevCode = DerivedAttribute('MasterICD10PrevCode', str, DefaultNone())
    MasterICD10PrevDesc = DerivedAttribute('MasterICD10PrevDesc', str, DefaultNone())


class FamilyHistoryAtBooking(_FamilyHistoryAtBooking):
    """MSD108FamHistBooking"""

    # -- auto-generated --

    __table__ = "MSD108FamHistBooking"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD108_ID = DerivedAttribute(
        'MSD108_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_FamilyHistoryAtBooking.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_FamilyHistoryAtBooking.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    MapSnomedCTSituationCode = DerivedAttribute('MapSnomedCTSituationCode', int, DefaultNone())
    MapSnomedCTSituationTerm = DerivedAttribute('MapSnomedCTSituationTerm', str, DefaultNone())
    MapICD10SituationCode = DerivedAttribute('MapICD10SituationCode', str, DefaultNone())
    MapICD10SituationDesc = DerivedAttribute('MapICD10SituationDesc', str, DefaultNone())
    MasterICD10SituationCode = DerivedAttribute('MasterICD10SituationCode', str, DefaultNone())
    MasterICD10SituationDesc = DerivedAttribute('MasterICD10SituationDesc', str, DefaultNone())


class FindingAndObservationMother(_FindingAndObservationMother):
    """MSD109FindingObsMother"""

    # -- auto-generated --

    __table__ = "MSD109FindingObsMother"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD109_ID = DerivedAttribute(
        'MSD109_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_FindingAndObservationMother.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_FindingAndObservationMother.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


    MasterSnomedCTFindingCode = DerivedAttribute(
        'MasterSnomedCTFindingCode', int,
        If(
            condition=Equals(Select(_FindingAndObservationMother.FindingScheme), Literal(FindingSchemes.SNOMED_CT)),
            then=Cast(Select(_FindingAndObservationMother.FindingCode), int),
            otherwise=Select(_FindingAndObservationMother.MapSnomedCTFindingCode)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTObsCode = DerivedAttribute(
        'MasterSnomedCTObsCode', int,
        If(
            condition=Equals(Select(_FindingAndObservationMother.ObsScheme), Literal(ObservationSchemes.SNOMED_CT)),
            then=Cast(Select(_FindingAndObservationMother.ObsCode), int),
            otherwise=Select(_FindingAndObservationMother.MapSnomedCTObsCode)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTFindingTerm = DerivedAttribute(
        'MasterSnomedCTFindingTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_FindingAndObservationMother.MasterSnomedCTFindingCode),
                                point_in_time_expr=Select(_FindingAndObservationMother.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MasterSnomedCTObsTerm = DerivedAttribute(
        'MasterSnomedCTObsTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_FindingAndObservationMother.MasterSnomedCTObsCode),
                                point_in_time_expr=Select(_FindingAndObservationMother.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MapSnomedCTFindingCode = DerivedAttribute('MapSnomedCTFindingCode', int, DefaultNone())
    MapSnomedCTFindingTerm = DerivedAttribute('MapSnomedCTFindingTerm', str, DefaultNone())
    MapICD10FindingCode = DerivedAttribute('MapICD10FindingCode', str, DefaultNone())
    MapICD10FindingDesc = DerivedAttribute('MapICD10FindingDesc', str, DefaultNone())
    MasterICD10FindingCode = DerivedAttribute('MasterICD10FindingCode', str, DefaultNone())
    MasterICD10FindingDesc = DerivedAttribute('MasterICD10FindingDesc', str, DefaultNone())
    MapSnomedCTObsCode = DerivedAttribute('MapSnomedCTObsCode', int, DefaultNone())
    MapSnomedCTObsTerm = DerivedAttribute('MapSnomedCTObsTerm', str, DefaultNone())


class CodedScoredAssessment(_CodedScoredAssessment):
    """MSD203CodedScoreAssContact"""

    # -- auto-generated --

    __table__ = "MSD203CodedScoreAssContact"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD203_ID = DerivedAttribute(
        'MSD203_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessment.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CodedScoredAssessment.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class CareActivityLabourAndDelivery(_CareActivityLabourAndDelivery):
    """MSD302CareActivityLabDel"""

    # -- auto-generated --

    __table__ = "MSD302CareActivityLabDel"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD302_ID = DerivedAttribute(
        'MSD302_ImsdsD', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareActivityLabourAndDelivery.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareActivityLabourAndDelivery.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    MasterSnomedCTProcedureCode = DerivedAttribute(
        'MasterSnomedCTProcedureCode', str,
        If(
            condition=Equals(Select(_CareActivityLabourAndDelivery.ProcedureScheme), Literal(ProcedureSchemes.SNOMED_CT)),
            then=Select(_CareActivityLabourAndDelivery.ProcedureCode),
            otherwise=Cast(Select(_CareActivityLabourAndDelivery.MapSnomedCTProcedureCode), str)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTFindingCode = DerivedAttribute(
        'MasterSnomedCTFindingCode', int,
        If(
            condition=Equals(Select(_CareActivityLabourAndDelivery.FindingScheme), Literal(FindingSchemes.SNOMED_CT)),
            then=Cast(Select(_CareActivityLabourAndDelivery.FindingCode), int),
            otherwise=Select(_CareActivityLabourAndDelivery.MapSnomedCTFindingCode)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTObsCode = DerivedAttribute(
        'MasterSnomedCTObsCode', int,
        If(
            condition=Equals(Select(_CareActivityLabourAndDelivery.ObsScheme), Literal(ObservationSchemes.SNOMED_CT)),
            then=Cast(Select(_CareActivityLabourAndDelivery.ObsCode), int),
            otherwise=Select(_CareActivityLabourAndDelivery.MapSnomedCTObsCode)
        )
    )  # type: DerivedAttribute

    GenitalTractTraumaticLesion = DerivedAttribute(
        'GenitalTractTraumaticLesion', str,
        If(
            condition=SnomedProcedureCodeMatches(
                Select(_CareActivityLabourAndDelivery.MasterSnomedCTProcedureCode),
                SnomedCodeSystemConcept.GENITAL_TRACT_TRAUMATIC_PROCEDURE_CODES),
            then=Select(_CareActivityLabourAndDelivery.MasterSnomedCTProcedureCode),
            otherwise=(
                If(
                    condition=Includes(Select(_CareActivityLabourAndDelivery.MasterSnomedCTFindingCode),
                                       Literal(SnomedCodeSystemConcept.GENITAL_TRACT_TRAUMATIC_FINDING_CODES)),
                    then=Cast(Select(_CareActivityLabourAndDelivery.MasterSnomedCTFindingCode), str),
                    otherwise=Literal(None)
                )
            )
        )
    )  # type: DerivedAttribute

    PostpartumBloodLoss = DerivedAttribute(
        'PostpartumBloodLoss', str,
        DerivePostpartumBloodLoss(
            Select(_CareActivityLabourAndDelivery.MasterSnomedCTObsCode),
            Select(_CareActivityLabourAndDelivery.ObsValue),
            Select(_CareActivityLabourAndDelivery.UCUMUnit)),
    )  # type: DerivedAttribute

    MasterSnomedCTProcedureTerm = DerivedAttribute(
        'MasterSnomedCTProcedureTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivityLabourAndDelivery.MasterSnomedCTProcedureCode),
                                point_in_time_expr=Select(_CareActivityLabourAndDelivery.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MasterSnomedCTFindingTerm = DerivedAttribute(
        'MasterSnomedCTFindingTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivityLabourAndDelivery.MasterSnomedCTFindingCode),
                                point_in_time_expr=Select(_CareActivityLabourAndDelivery.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MasterSnomedCTObsTerm = DerivedAttribute(
        'MasterSnomedCTObsTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivityLabourAndDelivery.MasterSnomedCTObsCode),
                                point_in_time_expr=Select(_CareActivityLabourAndDelivery.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MapSnomedCTProcedureCode = DerivedAttribute('MapSnomedCTProcedureCode', int, DefaultNone())
    MapSnomedCTProcedureTerm = DerivedAttribute('MapSnomedCTProcedureTerm', str, DefaultNone())
    MapOPCS4ProcedureCode = DerivedAttribute('MapOPCS4ProcedureCode', str, DefaultNone())
    MapOPCS4ProcedureDesc = DerivedAttribute('MapOPCS4ProcedureDesc', str, DefaultNone())
    MasterOPCS4ProcedureCode = DerivedAttribute('MasterOPCS4ProcedureCode', str, DefaultNone())
    MasterOPCS4ProcedureDesc = DerivedAttribute('MasterOPCS4ProcedureDesc', str, DefaultNone())
    MapSnomedCTFindingCode = DerivedAttribute('MapSnomedCTFindingCode', int, DefaultNone())
    MapSnomedCTFindingTerm = DerivedAttribute('MapSnomedCTFindingTerm', str, DefaultNone())
    MapICD10FindingCode = DerivedAttribute('MapICD10FindingCode', str, DefaultNone())
    MapICD10FindingDesc = DerivedAttribute('MapICD10FindingDesc', str, DefaultNone())
    MasterICD10FindingCode = DerivedAttribute('MasterICD10FindingCode', str, DefaultNone())
    MasterICD10FindingDesc = DerivedAttribute('MasterICD10FindingDesc', str, DefaultNone())
    MapSnomedCTObsCode = DerivedAttribute('MapSnomedCTObsCode', int, DefaultNone())
    MapSnomedCTObsTerm = DerivedAttribute('MapSnomedCTObsTerm', str, DefaultNone())


class NeonatalAdmission(_NeonatalAdmission):
    """MSD402NeonatalAdmission"""

    # -- auto-generated --

    __table__ = "MSD402NeonatalAdmission"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD402_ID = DerivedAttribute(
        'MSD402_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_NeonatalAdmission.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_NeonatalAdmission.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class ProvisionalDiagnosisNeonatal(_ProvisionalDiagnosisNeonatal):
    """MSD403ProvDiagNeonatal"""

    # -- auto-generated --

    __table__ = "MSD403ProvDiagNeonatal"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD403_ID = DerivedAttribute(
        'MSD403_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_ProvisionalDiagnosisNeonatal.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_ProvisionalDiagnosisNeonatal.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    MapSnomedCTProvDiagCode = DerivedAttribute('MapSnomedCTProvDiagCode', int, DefaultNone())
    MapSnomedCTProvDiagTerm = DerivedAttribute('MapSnomedCTProvDiagTerm', str, DefaultNone())
    MapICD10ProvCode = DerivedAttribute('MapICD10ProvCode', str, DefaultNone())
    MapICD10ProvDesc = DerivedAttribute('MapICD10ProvDesc', str, DefaultNone())
    MasterICD10ProvCode = DerivedAttribute('MasterICD10ProvCode', str, DefaultNone())
    MasterICD10ProvDesc = DerivedAttribute('MasterICD10ProvDesc', str, DefaultNone())


class DiagnosisNeonatal(_DiagnosisNeonatal):
    """MSD404DiagnosisNeonatal"""

    # -- auto-generated --

    __table__ = "MSD404DiagnosisNeonatal"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD404_ID = DerivedAttribute(
        'MSD404_ID', Decimal_20_0, RecordNumberOrUniqueID(
             submission_id_expr=Select(_DiagnosisNeonatal.Root.Header.UniqSubmissionID),
             row_number_expr=Select(
                 _DiagnosisNeonatal.RowNumber),
             return_decimal = True)
    )  # type: DerivedAttribute

    MapSnomedCTDiagCode = DerivedAttribute('MapSnomedCTDiagCode', int, DefaultNone())
    MapSnomedCTDiagTerm = DerivedAttribute('MapSnomedCTDiagTerm', str, DefaultNone())
    MapICD10Code = DerivedAttribute('MapICD10Code', str, DefaultNone())
    MasterICD10Code = DerivedAttribute('MasterICD10Code', str, DefaultNone())
    MasterICD10Desc = DerivedAttribute('MasterICD10Desc', str, DefaultNone())


class CodedScoredAssessmentBaby(_CodedScoredAssessmentBaby):
    """MSD406CodedScoreAssBaby"""

    # -- auto-generated --

    __table__ = "MSD406CodedScoreAssBaby"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD406_ID = DerivedAttribute(
        'MSD406_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CodedScoredAssessmentBaby.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CodedScoredAssessmentBaby.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class HospitalSpellCommissioner(_HospitalSpellCommissioner):
    """MSD502HospSpellComm"""

    # -- auto-generated --

    __table__ = "MSD502HospSpellComm"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD502_ID = DerivedAttribute(
        'MSD502_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_HospitalSpellCommissioner.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_HospitalSpellCommissioner.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class WardStay(_WardStay):
    """MSD503WardStay"""

    # -- auto-generated --

    __table__ = "MSD503WardStay"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD503_ID = DerivedAttribute(
        'MSD503_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_WardStay.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_WardStay.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class AssignedCareProfessional(_AssignedCareProfessional):
    """MSD504AssignedCareProf"""

    # -- auto-generated --

    __table__ = "MSD504AssignedCareProf"
    __concrete__ = True

    # ^^ auto-generated ^^

    MSD504_ID = DerivedAttribute(
        'MSD504_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AssignedCareProfessional.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_AssignedCareProfessional.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute


class HospitalProviderSpell(_HospitalProviderSpell):
    """MSD501HospProvSpell"""

    # -- auto-generated --

    __table__ = "MSD501HospProvSpell"
    __concrete__ = True

    HospitalSpellCommissioners = RepeatingSubmittedAttribute('HospitalSpellCommissioners', HospitalSpellCommissioner)  # type: List[HospitalSpellCommissioner]
    WardStays = RepeatingSubmittedAttribute('WardStays', WardStay)  # type: List[WardStay]
    AssignedCareProfessionals = RepeatingSubmittedAttribute('AssignedCareProfessionals', AssignedCareProfessional)  # type: List[AssignedCareProfessional]

    # ^^ auto-generated ^^

    MSD501_ID = DerivedAttribute(
        'MSD501_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_HospitalProviderSpell.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_HospitalProviderSpell.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    AgeAtDischargeMother = DerivedAttribute('AgeAtDischargeMother', int, AgeAtDate(
        date_of_birth_expr=Select(_HospitalProviderSpell.Root.Mother.PersonBirthDateMother),
        date_for_age_expr=Select(_HospitalProviderSpell.DischDateHospProvSpell),
        ignore_time=True))  # type: DerivedAttribute


class CareActivityBaby(_CareActivityBaby):
    """MSD405CareActivityBaby"""

    # -- auto-generated --

    __table__ = "MSD405CareActivityBaby"
    __concrete__ = True

    CodedScoredAssessmentsBaby = RepeatingSubmittedAttribute('CodedScoredAssessmentsBaby', CodedScoredAssessmentBaby)  # type: List[CodedScoredAssessmentBaby]

    # ^^ auto-generated ^^

    MSD405_ID = DerivedAttribute(
        'MSD405_ID', Decimal_20_0, RecordNumberOrUniqueID(
             submission_id_expr=Select(_CareActivityBaby.Root.Header.UniqSubmissionID),
             row_number_expr=Select(_CareActivityBaby.RowNumber),
             return_decimal = True)
    )  # type: DerivedAttribute

    MasterSnomedCTProcedureCode = DerivedAttribute(
        'MasterSnomedCTProcedureCode', str,
        If(
            condition=Equals(Select(_CareActivityBaby.ProcedureScheme), Literal(ProcedureSchemes.SNOMED_CT)),
            then=Select(_CareActivityBaby.ProcedureCode),
            otherwise=Cast(Select(_CareActivityBaby.MapSnomedCTProcedureCode), str)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTFindingCode = DerivedAttribute(
        'MasterSnomedCTFindingCode', int,
        If(
            condition=Equals(Select(_CareActivityBaby.FindingScheme), Literal(FindingSchemes.SNOMED_CT)),
            then=Cast(Select(_CareActivityBaby.FindingCode), int),
            otherwise=Select(_CareActivityBaby.MapSnomedCTFindingCode)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTObsCode = DerivedAttribute(
        'MasterSnomedCTObsCode', int,
        If(
            condition=Equals(Select(_CareActivityBaby.ObsScheme), Literal(ObservationSchemes.SNOMED_CT)),
            then=Cast(Select(_CareActivityBaby.ObsCode), int),
            otherwise=Select(_CareActivityBaby.MapSnomedCTObsCode)
        )
    )  # type: DerivedAttribute

    ApgarScore = DerivedAttribute(
        'ApgarScore', str,
        If(
            condition=Equals(Select(_CareActivityBaby.MasterSnomedCTObsCode),
                             Literal(SnomedCodeSystemConcept.APGAR_SCORE_AT_5_MINUTES)),
            then=Select(_CareActivityBaby.ObsValue),
            otherwise=Literal(None)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTProcedureTerm = DerivedAttribute(
        'MasterSnomedCTProcedureTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivityBaby.MasterSnomedCTProcedureCode),
                                point_in_time_expr=Select(_CareActivityBaby.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MasterSnomedCTFindingTerm = DerivedAttribute(
        'MasterSnomedCTFindingTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivityBaby.MasterSnomedCTFindingCode),
                                point_in_time_expr=Select(_CareActivityBaby.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MasterSnomedCTObsTerm = DerivedAttribute(
        'MasterSnomedCTObsTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivityBaby.MasterSnomedCTObsCode),
                                point_in_time_expr=Select(_CareActivityBaby.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MapSnomedCTProcedureCode = DerivedAttribute('MapSnomedCTProcedureCode', int, DefaultNone())
    MapSnomedCTProcedureTerm = DerivedAttribute('MapSnomedCTProcedureTerm', str, DefaultNone())
    MapOPCS4ProcedureCode = DerivedAttribute('MapOPCS4ProcedureCode', str, DefaultNone())
    MapOPCS4ProcedureDesc = DerivedAttribute('MapOPCS4ProcedureDesc', str, DefaultNone())
    MasterOPCS4ProcedureCode = DerivedAttribute('MasterOPCS4ProcedureCode', str, DefaultNone())
    MasterOPCS4ProcedureDesc = DerivedAttribute('MasterOPCS4ProcedureDesc', str, DefaultNone())
    MapSnomedCTFindingCode = DerivedAttribute('MapSnomedCTFindingCode', int, DefaultNone())
    MapSnomedCTFindingTerm = DerivedAttribute('MapSnomedCTFindingTerm', str, DefaultNone())
    MapICD10FindingCode = DerivedAttribute('MapICD10FindingCode', str, DefaultNone())
    MapICD10FindingDesc = DerivedAttribute('MapICD10FindingDesc', str, DefaultNone())
    MasterICD10FindingCode = DerivedAttribute('MasterICD10FindingCode', str, DefaultNone())
    MasterICD10FindingDesc = DerivedAttribute('MasterICD10FindingDesc', str, DefaultNone())
    MapSnomedCTObsCode = DerivedAttribute('MapSnomedCTObsCode', int, DefaultNone())
    MapSnomedCTObsTerm = DerivedAttribute('MapSnomedCTObsTerm', str, DefaultNone())


class BabyDemographics(_BabyDemographics):
    """MSD401BabyDemographics"""

    # -- auto-generated --

    __table__ = "MSD401BabyDemographics"
    __concrete__ = True

    NeonatalAdmissions = RepeatingSubmittedAttribute('NeonatalAdmissions', NeonatalAdmission)  # type: List[NeonatalAdmission]
    ProvisionalDiagnosisNeonatals = RepeatingSubmittedAttribute('ProvisionalDiagnosisNeonatals', ProvisionalDiagnosisNeonatal)  # type: List[ProvisionalDiagnosisNeonatal]
    DiagnosisNeonatals = RepeatingSubmittedAttribute('DiagnosisNeonatals', DiagnosisNeonatal)  # type: List[DiagnosisNeonatal]
    CareActivitiesBaby = RepeatingSubmittedAttribute('CareActivitiesBaby', CareActivityBaby)  # type: List[CareActivityBaby]

    # ^^ auto-generated ^^

    MSD401_ID = DerivedAttribute(
        'MSD401_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareActivityLabourAndDelivery.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareActivityLabourAndDelivery.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    ValidNHSNoFlagBaby = DerivedAttribute(
        'ValidNHSNoFlagBaby', str,
        If(NotNull(Select(_BabyDemographics.NHSNumberBaby)),
           then=Literal('Y'), otherwise=Literal('N')))  # type: DerivedAttribute
    YearOfBirthBaby = DerivedAttribute('YearOfBirthBaby', int,
                                       ExtractFromDate(Select(_BabyDemographics.PersonBirthDateBaby),
                                                       ExtractFromDate.YEAR))  # type: DerivedAttribute
    MonthOfBirthBaby = DerivedAttribute('MonthOfBirthBaby', str,
                                        ExtractFromDate(Select(_BabyDemographics.PersonBirthDateBaby),
                                                        ExtractFromDate.MONTH))  # type: DerivedAttribute
    DayOfBirthBaby = DerivedAttribute('DayOfWeekOfBirthBaby', str,
                                      ExtractFromDate(Select(_BabyDemographics.PersonBirthDateBaby),
                                                      ExtractFromDate.DAYOFWEEK))  # type: DerivedAttribute
    MerOfBirthBaby = DerivedAttribute('MerOfBirthBaby', str,
                                      ExtractFromTime(Select(_BabyDemographics.PersonBirthTimeBaby),
                                                      ExtractFromTime.MERIDIAN))  # type: DerivedAttribute
    YearOfDeathBaby = DerivedAttribute('YearOfDeathBaby', int,
                                       ExtractFromDate(Select(_BabyDemographics.PersonDeathDateBaby),
                                                       ExtractFromDate.YEAR))  # type: DerivedAttribute
    MonthOfDeathBaby = DerivedAttribute('MonthOfDeathBaby', str,
                                        ExtractFromDate(Select(_BabyDemographics.PersonDeathDateBaby),
                                                        ExtractFromDate.MONTH))  # type: DerivedAttribute
    DayOfDeathBaby = DerivedAttribute('DayOfWeekOfDeathBaby', str,
                                      ExtractFromDate(Select(_BabyDemographics.PersonDeathDateBaby),
                                                      ExtractFromDate.DAYOFWEEK))  # type: DerivedAttribute
    MeridianOfDeathBaby = DerivedAttribute('MeridianOfDeathBaby', str,
                                           ExtractFromTime(Select(_BabyDemographics.PersonDeathTimeBaby),
                                                           ExtractFromTime.MERIDIAN))  # type: DerivedAttribute
    AgeAtBirthMother = DerivedAttribute('AgeAtBirthMother', int, AgeAtDate(
        date_of_birth_expr=Select(_BabyDemographics.Root.Mother.PersonBirthDateMother),
        date_for_age_expr=Select(_BabyDemographics.PersonBirthDateBaby),
        ignore_time=True))  # type: DerivedAttribute
    AgeAtDeathBaby = DerivedAttribute('AgeAtDeathBaby', int, AgeAtTime(
        date_of_birth_expr=Select(_BabyDemographics.PersonBirthDateBaby),
        time_of_birth_expr=Select(_BabyDemographics.PersonBirthTimeBaby),
        date_for_age_expr=Select(_BabyDemographics.PersonDeathDateBaby),
        time_for_age_expr=Select(_BabyDemographics.PersonDeathTimeBaby),
        time_unit=AgeAtTime.HOURS))  # type: DerivedAttribute

    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)  # type: AssignableAttribute


class LabourAndDelivery(_LabourAndDelivery):
    """MSD301LabourDelivery"""

    # -- auto-generated --

    __table__ = "MSD301LabourDelivery"
    __concrete__ = True

    CareActivityLaboursAndDeliveries = RepeatingSubmittedAttribute('CareActivityLaboursAndDeliveries', CareActivityLabourAndDelivery)  # type: List[CareActivityLabourAndDelivery]
    BabyDemographics = RepeatingSubmittedAttribute('BabyDemographics', BabyDemographics)  # type: List[BabyDemographics]

    # ^^ auto-generated ^^

    MSD301_ID = DerivedAttribute(
        'MSD301_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_LabourAndDelivery.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_LabourAndDelivery.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    AgeAtLabourMother = DerivedAttribute('AgeAtLabourMother', int, AgeAtDate(
        date_of_birth_expr=Select(_LabourAndDelivery.Root.Mother.PersonBirthDateMother),
        date_for_age_expr=Select(_LabourAndDelivery.LabourOnsetDate),
        ignore_time=True))  # type: DerivedAttribute

    BirthsPerLabandDel = DerivedAttribute('BirthsPerLabandDel', int,
                                          Count(Select(_LabourAndDelivery.BabyDemographics)))  # type: DerivedAttribute


class CareActivity(_CareActivity):
    """MSD202CareActivityPreg"""

    # -- auto-generated --

    __table__ = "MSD202CareActivityPreg"
    __concrete__ = True

    CodedScoredAssessments = RepeatingSubmittedAttribute('CodedScoredAssessments', CodedScoredAssessment)  # type: List[CodedScoredAssessment]

    # ^^ auto-generated ^^

    MSD202_ID = DerivedAttribute(
        'MSD202_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareActivity.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareActivity.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    MasterSnomedCTProcedureCode = DerivedAttribute(
        'MasterSnomedCTProcedureCode', str,
        If(
            condition=Equals(Select(_CareActivity.ProcedureScheme), Literal(ProcedureSchemes.SNOMED_CT)),
            then=Select(_CareActivity.ProcedureCode),
            otherwise=Cast(Select(_CareActivity.MapSnomedCTProcedureCode), str)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTFindingCode = DerivedAttribute(
        'MasterSnomedCTFindingCode', int,
        If(
            condition=Equals(Select(_CareActivity.FindingScheme), Literal(FindingSchemes.SNOMED_CT)),
            then=Cast(Select(_CareActivity.FindingCode), int),
            otherwise=Select(_CareActivity.MapSnomedCTFindingCode)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTObsCode = DerivedAttribute(
        'MasterSnomedCTObsCode', int,
        If(
            condition=Equals(Select(_CareActivity.ObsScheme), Literal(ObservationSchemes.SNOMED_CT)),
            then=Cast(Select(_CareActivity.ObsCode), int),
            otherwise=Select(_CareActivity.MapSnomedCTObsCode)
        )
    )  # type: DerivedAttribute

    AlcoholUnitsPerWeek = DerivedAttribute(
        'AlcoholUnitsPerWeek', str,
        DeriveAlcoholUnitsPerWeek(
            observation_code=Select(_CareActivity.MasterSnomedCTObsCode),
            observation_value=Select(_CareActivity.ObsValue)
        )
    )  # type: DerivedAttribute

    PersonHeight = DerivedAttribute(
        'PersonHeight', str,
        DerivePersonHeight(
            observation_code=Select(_CareActivity.MasterSnomedCTObsCode),
            observation_units=Select(_CareActivity.UCUMUnit),
            observation_value=Select(_CareActivity.ObsValue)
        )
    )  # type: DerivedAttribute

    PersonWeight = DerivedAttribute(
        'PersonWeight', str,
        DerivePersonWeight(
            observation_code=Select(_CareActivity.MasterSnomedCTObsCode),
            observation_units=Select(_CareActivity.UCUMUnit),
            observation_value=Select(_CareActivity.ObsValue)
        )
    )  # type: DerivedAttribute

    CigarettesPerDay = DerivedAttribute(
        'CigarettesPerDay', str,
        DeriveCigarettesPerDay(
            observation_code=Select(_CareActivity.MasterSnomedCTObsCode),
            observation_value=Select(_CareActivity.ObsValue)
        )
    )  # type: DerivedAttribute

    COMonReading = DerivedAttribute(
        'COMonReading', str, DeriveCOMonReading(
            observation_code=Select(_CareActivity.MasterSnomedCTObsCode),
            observation_units=Select(_CareActivity.UCUMUnit),
            observation_value=Select(_CareActivity.ObsValue),
            finding_code=Select(_CareActivity.MasterSnomedCTFindingCode)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTProcedureTerm = DerivedAttribute(
        'MasterSnomedCTProcedureTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivity.MasterSnomedCTProcedureCode),
                                point_in_time_expr=Select(_CareActivity.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MasterSnomedCTFindingTerm = DerivedAttribute(
        'MasterSnomedCTFindingTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivity.MasterSnomedCTFindingCode),
                                point_in_time_expr=Select(_CareActivity.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MasterSnomedCTObsTerm = DerivedAttribute(
        'MasterSnomedCTObsTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_CareActivity.MasterSnomedCTObsCode),
                                point_in_time_expr=Select(_CareActivity.Root.AntenatalAppDate))
        )
    )  # type: DerivedAttribute

    MapSnomedCTProcedureCode = DerivedAttribute('MapSnomedCTProcedureCode', int, DefaultNone())
    MapSnomedCTProcedureTerm = DerivedAttribute('MapSnomedCTProcedureTerm', str, DefaultNone())
    MapOPCS4ProcedureCode = DerivedAttribute('MapOPCS4ProcedureCode', str, DefaultNone())
    MapOPCS4ProcedureDesc = DerivedAttribute('MapOPCS4ProcedureDesc', str, DefaultNone())
    MasterOPCS4ProcedureCode = DerivedAttribute('MasterOPCS4ProcedureCode', str, DefaultNone())
    MasterOPCS4ProcedureDesc = DerivedAttribute('MasterOPCS4ProcedureDesc', str, DefaultNone())
    MapSnomedCTFindingCode = DerivedAttribute('MapSnomedCTFindingCode', int, DefaultNone())
    MapSnomedCTFindingTerm = DerivedAttribute('MapSnomedCTFindingTerm', str, DefaultNone())
    MapICD10FindingCode = DerivedAttribute('MapICD10FindingCode', str, DefaultNone())
    MapICD10FindingDesc = DerivedAttribute('MapICD10FindingDesc', str, DefaultNone())
    MasterICD10FindingCode = DerivedAttribute('MasterICD10FindingCode', str, DefaultNone())
    MasterICD10FindingDesc = DerivedAttribute('MasterICD10FindingDesc', str, DefaultNone())
    MapSnomedCTObsCode = DerivedAttribute('MapSnomedCTObsCode', int, DefaultNone())
    MapSnomedCTObsTerm = DerivedAttribute('MapSnomedCTObsTerm', str, DefaultNone())


class CareContact(_CareContact):
    """MSD201CareContactPreg"""

    # -- auto-generated --

    __table__ = "MSD201CareContactPreg"
    __concrete__ = True

    CareActivities = RepeatingSubmittedAttribute('CareActivities', CareActivity)  # type: List[CareActivity]

    # ^^ auto-generated ^^

    MSD201_ID = DerivedAttribute(
        'MSD201_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_CareContact.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_CareContact.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    GestAgeCContactDate = DerivedAttribute(
        'GestCContactDate', int,
        GestationalAge(_CareContact.CContactDate, _CareContact.Parent)
    )  # type: DerivedAttribute
    DerivedBMI = DerivedAttribute(
        'DerivedBMI', str,
        DerivePersonBMI(
            care_activities_expr=Select(_CareContact.CareActivities),
            uniq_id_expr=Select(_CareContact.CareActivities.MSD202_ID),
            obs_code_expr=Select(_CareContact.CareActivities.MasterSnomedCTObsCode),
            obs_value_expr=Select(_CareContact.CareActivities.ObsValue),
            measurement_unit_expr=Select(_CareContact.CareActivities.UCUMUnit),
            height_expr=Select(_CareContact.CareActivities.PersonHeight),
            weight_expr=Select(_CareContact.CareActivities.PersonWeight),
        )
    )  # type: DerivedAttribute


class MotherDemog(_MotherDemog):
    """MSD001MotherDemog"""

    # -- auto-generated --

    __table__ = "MSD001MotherDemog"
    __concrete__ = True

    GPPracticeRegistrations = RepeatingSubmittedAttribute('GPPracticeRegistrations', GPPracticeRegistration)  # type: List[GPPracticeRegistration]
    SocialAndPersonalCircumstances = RepeatingSubmittedAttribute('SocialAndPersonalCircumstances', SocialAndPersonalCircumstances)  # type: List[SocialAndPersonalCircumstances]
    OverseasVisitorChargingCategories = RepeatingSubmittedAttribute('OverseasVisitorChargingCategories', OverseasVisitorChargingCategory)  # type: List[OverseasVisitorChargingCategory]

    # ^^ auto-generated ^^

    MSD001_ID = DerivedAttribute(
        'MSD001_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_MotherDemog.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_MotherDemog.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    AgeRPEndDate = DerivedAttribute('AgeRPEndDate', int, AgeAtDate(
        date_of_birth_expr=Select(_MotherDemog.PersonBirthDateMother),
        date_for_age_expr=Select(_MotherDemog.Root.Header.RPEndDate),
        ignore_time=True))  # type: DerivedAttribute

    YearOfDeathMother = DerivedAttribute('YearOfDeathMother', int,
                                         ExtractFromDate(Select(_MotherDemog.PersonDeathDateMother),
                                                         ExtractFromDate.YEAR))  # type: DerivedAttribute
    MonthOfDeathMother = DerivedAttribute('MonthOfDeathMother', str,
                                          ExtractFromDate(Select(_MotherDemog.PersonDeathDateMother),
                                                          ExtractFromDate.MONTH))  # type: DerivedAttribute
    DayOfWeekOfDeathMother = DerivedAttribute('DayOfWeekOfDeathMother', str,
                                              ExtractFromDate(Select(_MotherDemog.PersonDeathDateMother),
                                                              ExtractFromDate.DAYOFWEEK))  # type: DerivedAttribute
    MeridianOfDeathMother = DerivedAttribute('MeridianOfDeathMother', str,
                                             ExtractFromTime(Select(_MotherDemog.PersonDeathTimeMother),
                                                             ExtractFromTime.MERIDIAN))  # type: DerivedAttribute
    AgeAtDeathMother = DerivedAttribute('AgeAtDeathMother', int, AgeAtDate(
        date_of_birth_expr=Select(_MotherDemog.PersonBirthDateMother),
        date_for_age_expr=Select(_MotherDemog.PersonDeathDateMother),
        ignore_time=True))  # type: DerivedAttribute

    PostcodeDistrictMother = DerivedAttribute('PostcodeDistrictMother', str, (
        DerivePostcodeDistrict(postcode_expr=Select(_MotherDemog.Postcode),
                               point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate))
    ))  # type: DerivedAttribute

    LSOAMother2011 = DerivedAttribute(
        'LSOAMother2011',
        str,
        (
            If(
                ValidPostcode(postcode_expr=Select(_MotherDemog.Postcode), point_in_time_expr=Select(
                    _MotherDemog.Root.Header.RPStartDate)),
                then=(DerivePostcode(postcode_expr=Select(_MotherDemog.Postcode),
                       point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate),
                       field_name=ONSRecordPaths.LOWER_LAYER_SOA)
                      ),
                otherwise=Literal(None)
            )
        )
    )  # type: DerivedAttribute

    LAD_UAMother = DerivedAttribute(
        'LAD_UAMother',
        str,
        (
            If(
                ValidPostcode(postcode_expr=Select(_MotherDemog.Postcode), point_in_time_expr=Select(
                    _MotherDemog.Root.Header.RPStartDate)),
                then=(DerivePostcode(postcode_expr=Select(_MotherDemog.Postcode),
                       point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate),
                       field_name=ONSRecordPaths.UNITARY_AUTHORITY)
                      ),
                otherwise=Literal(None)
            )
        )
    )# type: DerivedAttribute

    CCGResidenceMother = DerivedAttribute(
            'CCGResidenceMother',
            str,
            (
            If(
                DateBeforeOrEqualTo(
                eval_expr=(Select(_MotherDemog.Root.Header.RPStartDate)), 
                check_date_expr=Literal(CCG_ICB_SWITCHOVER_PERIOD_END)),
                then=(
                If(
                    ValidPostcode(
                        postcode_expr=Select(_MotherDemog.Postcode),
                        point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate)
                    ),
                    then=(
                        CcgFromPostcode(
                            postcode_expr=Select(_MotherDemog.Postcode),
                            event_date=Select(_MotherDemog.Root.Header.RPStartDate),
                            enforce_icb_switchover_period=Literal(True)
                        )
                    ),
                    otherwise=Literal(None)
                )),
                otherwise=Literal(None))
            )
        )  # type: DerivedAttribute



    OrgIDSubICBLocResidence = DerivedAttribute('OrgIDSubICBLocResidence', str, (
        If(
            ValidPostcode(
                postcode_expr=Select(_MotherDemog.Postcode),
                point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate)
            ),
            then=(
                SubICBFromPostcode(
                    postcode_expr=Select(_MotherDemog.Postcode),
                    event_date=Select(_MotherDemog.Root.Header.RPStartDate)
                )
            ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    OrgIDICBRes = DerivedAttribute('OrgIDICBRes', str, (
        If(
            ValidPostcode(
                postcode_expr=Select(_MotherDemog.Postcode),
                point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate)
            ),
            then=(
                ICBFromPostcode(
                    postcode_expr=Select(_MotherDemog.Postcode),
                    event_date=Select(_MotherDemog.Root.Header.RPStartDate)
                )
            ),
            otherwise=Literal(None)
        )
    ))  # type: DerivedAttribute

    CountyMother = DerivedAttribute(
        'CountyMother',
        str,
        (
            If(
                ValidPostcode(postcode_expr=Select(_MotherDemog.Postcode), point_in_time_expr=Select(
                    _MotherDemog.Root.Header.RPStartDate)),
                then=(DerivePostcode(
                        postcode_expr=Select(_MotherDemog.Postcode),
                        point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate),
                        field_name=ONSRecordPaths.RESIDENCE_COUNTY)
                ),
                otherwise=Literal(None)
            )
        )
    )  # type: DerivedAttribute

    ElectoralWardMother = DerivedAttribute(
        'ElectoralWardMother', str,
        (
            If(
                ValidPostcode(postcode_expr=Select(_MotherDemog.Postcode), point_in_time_expr=Select(
                    _MotherDemog.Root.Header.RPStartDate)),
                then=(DerivePostcode(postcode_expr=Select(_MotherDemog.Postcode),
                                     point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate),
                                    field_name=ONSRecordPaths.OS_WARD_2011)
                      ),
                otherwise=Literal(None)
            )
        )
    )  # type: DerivedAttribute

    ValidNHSNoFlagMother = DerivedAttribute('ValidNHSNoFlagMother', str, If(
        NotNull(Select(_MotherDemog.NHSNumberMother)),
        then=Literal('Y'), otherwise=Literal('N')))  # type: DerivedAttribute

    ValidPostcodeFlag = DerivedAttribute(
        'ValidPostcodeFlag',
        str,
        If(
            ValidPostcode(postcode_expr=Select(_MotherDemog.Postcode), point_in_time_expr=Select(
                                                             _MotherDemog.Root.Header.RPStartDate)),
            then=Literal('Y'), otherwise=Literal('N')
        )
    )  # type: DerivedAttribute

    Rank_IMD_Decile_2015 = DerivedAttribute(
        'Rank_IMD_Decile_2015',
        str,
        (
            If(
                ValidPostcode(postcode_expr=Select(_MotherDemog.Postcode), point_in_time_expr=Select(
                    _MotherDemog.Root.Header.RPStartDate)),
                then=(GetIMDDecile(
                        postcode_expr=Select(_MotherDemog.Postcode),
                        point_in_time_expr=Select(_MotherDemog.Root.Header.RPStartDate))
                    ),
                otherwise=Literal(None)
            )
        )
    )# type: DerivedAttribute

    DefaultPostcode = DerivedAttribute('DefaultPostcode', str,
                                       GetDefaultPostcode(postcode_expr=Select(_MotherDemog.Postcode))
                                       )  # type: DerivedAttribute

    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)  # type: AssignableAttribute


class PregnancyAndBookingDetails(_PregnancyAndBookingDetails):
    """MSD101PregnancyBooking"""

    # -- auto-generated --

    __table__ = "MSD101PregnancyBooking"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    Mother = SubmittedAttribute('Mother', MotherDemog)  # type: MotherDemog
    MaternityCarePlans = RepeatingSubmittedAttribute('MaternityCarePlans', MaternityCarePlan)  # type: List[MaternityCarePlan]
    DatingScanProcedures = RepeatingSubmittedAttribute('DatingScanProcedures', DatingScanProcedure)  # type: List[DatingScanProcedure]
    CodedScoredAssessmentsPregnancy = RepeatingSubmittedAttribute('CodedScoredAssessmentsPregnancy', CodedScoredAssessmentPregnancy)  # type: List[CodedScoredAssessmentPregnancy]
    ProvisionalDiagnosisPregnancies = RepeatingSubmittedAttribute('ProvisionalDiagnosisPregnancies', ProvisionalDiagnosisPregnancy)  # type: List[ProvisionalDiagnosisPregnancy]
    DiagnosisPregnancies = RepeatingSubmittedAttribute('DiagnosisPregnancies', DiagnosisPregnancy)  # type: List[DiagnosisPregnancy]
    MedicalHistoryPreviousDiagnoses = RepeatingSubmittedAttribute('MedicalHistoryPreviousDiagnoses', MedicalHistoryPreviousDiagnosis)  # type: List[MedicalHistoryPreviousDiagnosis]
    FamilyHistoryAtBooking = RepeatingSubmittedAttribute('FamilyHistoryAtBooking', FamilyHistoryAtBooking)  # type: List[FamilyHistoryAtBooking]
    FindingsAndObservationsMother = RepeatingSubmittedAttribute('FindingsAndObservationsMother', FindingAndObservationMother)  # type: List[FindingAndObservationMother]
    CareContacts = RepeatingSubmittedAttribute('CareContacts', CareContact)  # type: List[CareContact]
    LaboursAndDeliveries = RepeatingSubmittedAttribute('LaboursAndDeliveries', LabourAndDelivery)  # type: List[LabourAndDelivery]
    HospitalProviderSpells = RepeatingSubmittedAttribute('HospitalProviderSpells', HospitalProviderSpell)  # type: List[HospitalProviderSpell]

    # ^^ auto-generated ^^

    RFAs = DerivedAttribute(
        'RFAs', List[str], ReasonsForAccessExpression(Select(_PregnancyAndBookingDetails.Root))
    )  # type: DerivedAttribute

    MSD101_ID = DerivedAttribute(
        'MSD101_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_PregnancyAndBookingDetails.Root.Header.UniqSubmissionID),
            row_number_expr=Select(_PregnancyAndBookingDetails.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    RecordNumber = DerivedAttribute(
        'RecordNumber', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_PregnancyAndBookingDetails.Header.UniqSubmissionID),
            row_number_expr=Select(_PregnancyAndBookingDetails.Mother.RowNumber))
    )  # type: DerivedAttribute

    RPStartDate = DerivedAttribute('RPStartDate', date,
                                   Select(_PregnancyAndBookingDetails.Header.RPStartDate))  # type: DerivedAttribute
    RPEndDate = DerivedAttribute('RPEndDate', date,
                                 Select(_PregnancyAndBookingDetails.Header.RPEndDate))  # type: DerivedAttribute
    AgeAtBookingMother = DerivedAttribute('AgeAtBookingMother', int, AgeAtDate(
        date_of_birth_expr=Select(_PregnancyAndBookingDetails.Mother.PersonBirthDateMother),
        date_for_age_expr=Select(_PregnancyAndBookingDetails.AntenatalAppDate),
        ignore_time=True))  # type: DerivedAttribute
    GestAgeBooking = DerivedAttribute(
        'GestAgeBooking', int,
        GestationalAge(_PregnancyAndBookingDetails.AntenatalAppDate, _PregnancyAndBookingDetails)
    )  # type: DerivedAttribute
    CigarettesPerDayBand = DerivedAttribute(
        'CigarettesPerDayBand', str,
        Case(
            variable=Cast(
                CareActivityAttributeAtAppointmentDate(_PregnancyAndBookingDetails, 'CigarettesPerDay'),
                float),
            branches=[
                (lambda cigarettes: cigarettes is None, Literal(None)),
                (lambda cigarettes: cigarettes == 0, Literal("0")),
                (lambda cigarettes: cigarettes < 10, Literal("1-9")),
                (lambda cigarettes: cigarettes < 20, Literal("10-19")),
                (lambda cigarettes: cigarettes >= 20, Literal("20 or more")),
            ],
        )
    )  # type: DerivedAttribute
    AlcoholUnitsPerWeekBand = DerivedAttribute(
        'AlcoholUnitsPerWeekBand', str,
        Case(
            variable=Cast(
                CareActivityAttributeAtAppointmentDate(_PregnancyAndBookingDetails, 'AlcoholUnitsPerWeek'),
                float),
            branches=[
                (lambda units: units is None, Literal(None)),
                (lambda units: units < 1, Literal("Less than 1 unit")),
                (lambda units: units < 7, Literal("1 - less than 7 units")),
                (lambda units: units < 14, Literal("7 - less than 14 units")),
                (lambda units: units >= 14, Literal("14 units and over")),
            ],
        )
    )  # type: DerivedAttribute
    AlcoholUnitsBooking = DerivedAttribute(
        'AlcoholUnitsBooking', str,
        CareActivityAttributeAtAppointmentDate(_PregnancyAndBookingDetails, 'AlcoholUnitsPerWeek')
    )  # type: DerivedAttribute


class AnonSelfAssessment(_AnonSelfAssessment):
    """MSD601AnonSelfAssessment"""

    # -- auto-generated --

    __table__ = "MSD601AnonSelfAssessment"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    RFAs = DerivedAttribute(
        'RFAs', List[str], AnonymousTableReasonsForAccessExpression(
            Select(_AnonSelfAssessment.OrgIDComm),
            Select(_AnonSelfAssessment.CompDate)
        )
    )  # type: DerivedAttribute

    RPStartDate = DerivedAttribute('RPStartDate', date,
                                   Select(_AnonSelfAssessment.Header.RPStartDate))  # type: DerivedAttribute
    RPEndDate = DerivedAttribute('RPEndDate', date,
                                 Select(_AnonSelfAssessment.Root.Header.RPEndDate))  # type: DerivedAttribute

    MSD601_ID = DerivedAttribute(
        'MSD601_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AnonSelfAssessment.Header.UniqSubmissionID),
            row_number_expr=Select(_AnonSelfAssessment.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    AnonRecordNumber = DerivedAttribute(
        'AnonRecordNumber', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AnonSelfAssessment.Header.UniqSubmissionID),
            row_number_expr=Select(_AnonSelfAssessment.RowNumber))

    )  # type: DerivedAttribute


class AnonFindings(_AnonFindings):
    """MSD602AnonFindings"""

    # -- auto-generated --

    __table__ = "MSD602AnonFindings"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    RFAs = DerivedAttribute(
        'RFAs', List[str], AnonymousTableReasonsForAccessExpression(
            Select(_AnonFindings.OrgIDComm),
            Select(_AnonFindings.ClinInterDate)
        )
    )  # type: DerivedAttribute

    RPStartDate = DerivedAttribute('RPStartDate', date,
                                   Select(_AnonFindings.Header.RPStartDate))  # type: DerivedAttribute
    RPEndDate = DerivedAttribute('RPEndDate', date,
                                 Select(_AnonFindings.Root.Header.RPEndDate))  # type: DerivedAttribute

    MSD602_ID = DerivedAttribute(
        'MSD602_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AnonFindings.Header.UniqSubmissionID),
            row_number_expr=Select(_AnonFindings.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    AnonRecordNumber = DerivedAttribute(
        'AnonRecordNumber', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_AnonFindings.Header.UniqSubmissionID),
            row_number_expr=Select(_AnonFindings.RowNumber))
    )  # type: DerivedAttribute

    MasterSnomedCTFindingCode = DerivedAttribute(
        'MasterSnomedCTFindingCode', int,
        If(
            condition=Equals(Select(_AnonFindings.FindingScheme), Literal(FindingSchemes.SNOMED_CT)),
            then=Cast(Select(_AnonFindings.FindingCode), int),
            otherwise=Select(_AnonFindings.MapSnomedCTFindingCode)
        )
    )  # type: DerivedAttribute

    MasterSnomedCTFindingTerm = DerivedAttribute(
        'MasterSnomedCTFindingTerm', str, (
            DeriveSnomedTerm(snomed_code_expr=Select(_AnonFindings.MasterSnomedCTFindingCode),
                             point_in_time_expr=Select(_AnonFindings.ClinInterDate))
        )
    )  # type: DerivedAttribute

    MapSnomedCTFindingCode = DerivedAttribute('MapSnomedCTFindingCode', int, DefaultNone())
    MapSnomedCTFindingTerm = DerivedAttribute('MapSnomedCTFindingTerm', str, DefaultNone())
    MapICD10FindingCode = DerivedAttribute('MapICD10FindingCode', str, DefaultNone())
    MapICD10FindingDesc = DerivedAttribute('MapICD10FindingDesc', str, DefaultNone())
    MasterICD10FindingCode = DerivedAttribute('MasterICD10FindingCode', str, DefaultNone())
    MasterICD10FindingDesc = DerivedAttribute('MasterICD10FindingDesc', str, DefaultNone())


class StaffDetails(_StaffDetails):
    """MSD901StaffDetails"""

    # -- auto-generated --

    __table__ = "MSD901StaffDetails"
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: META
    Header = SubmittedAttribute('Header', Header)  # type: Header

    # ^^ auto-generated ^^

    RFAs = DerivedAttribute(
        'RFAs', List[str], AnonymousStaffDetailsTableReasonsForAccessExpression()
    )  # type: DerivedAttribute

    RPStartDate = DerivedAttribute('RPStartDate', date,
                                   Select(_StaffDetails.Header.RPStartDate))  # type: DerivedAttribute
    RPEndDate = DerivedAttribute('RPEndDate', date,
                                 Select(_StaffDetails.Header.RPEndDate))  # type: DerivedAttribute
    MSD901_ID = DerivedAttribute(
        'MSD901_ID', Decimal_20_0, RecordNumberOrUniqueID(
            submission_id_expr=Select(_StaffDetails.Header.UniqSubmissionID),
            row_number_expr=Select(_StaffDetails.RowNumber),
            return_decimal = True)
    )  # type: DerivedAttribute

    RecordNumber = DerivedAttribute(
        'RecordNumber', int, RecordNumberOrUniqueID(
            submission_id_expr=Select(_StaffDetails.Header.UniqSubmissionID),
            row_number_expr=Select(_StaffDetails.RowNumber))
    )  # type: DerivedAttribute


def get_all_models() -> Generator[Type[BaseMSDS], None, None]:
    clsmembers = [
        name for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
        if obj.__module__ is __name__
    ]

    for member in clsmembers:
        if not member.startswith('_') and not member.startswith('BaseMSDS'):
            cls_obj = getattr(sys.modules[__name__], member)
            if safe_issubclass(cls_obj, BaseMSDS):
                yield cls_obj


def get_anonymous_models():
    return [AnonFindings, AnonSelfAssessment, StaffDetails]
