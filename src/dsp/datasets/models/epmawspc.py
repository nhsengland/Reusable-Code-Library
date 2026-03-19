from datetime import date, datetime
from dsp.datasets.common import Fields as CommonFields
from dsp.common.structured_model import _META, Decimal_12_6, DerivedAttribute, DerivedAttributePlaceholder, \
    DSPStructuredModel, META, SubmittedAttribute, AssignableAttribute
from dsp.common.expressions import Literal, Select, ValToDateTime, ToDate
from dsp.common.epma_expressions import CompareTwoColumnsToTwoLists
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes


class _EPMAWellSkyPrescriptionModel(DSPStructuredModel):
    META = SubmittedAttribute(CommonFields.META, _META)

    ODS = SubmittedAttribute('ODS', str)
    SPR = SubmittedAttribute('SPR', str)
    NHS = SubmittedAttribute('NHS', str)
    Type = SubmittedAttribute('Type', str)
    Drug = SubmittedAttribute('Drug', str)
    MDDF = SubmittedAttribute('MDDF', str)
    DmdName = SubmittedAttribute('DmdName', str)
    DmdId = SubmittedAttribute('DmdId', str)
    PSCName = SubmittedAttribute('PSCName', str)
    PSCStrength = SubmittedAttribute('PSCStrength', str)
    PSCForm = SubmittedAttribute('PSCForm', str)
    OrderChangeDate = SubmittedAttribute('OrderChangeDate', datetime)  # Do we need to supply date format somewhere?
    DoseStatus = SubmittedAttribute('DoseStatus', str)
    PriDose = SubmittedAttribute('PriDose', Decimal_12_6)
    PriDoseUnit = SubmittedAttribute('PriDoseUnit', str)
    SecDose = SubmittedAttribute('SecDose', Decimal_12_6)
    SecDoseUnit = SubmittedAttribute('SecDoseUnit', str)
    Route = SubmittedAttribute('Route', str)
    Frequency = SubmittedAttribute('Frequency', str)
    PRN = SubmittedAttribute('PRN', str)
    PRNReason = SubmittedAttribute('PRNReason', str)
    OriginalStartDate = SubmittedAttribute('OriginalStartDate', date)  # Do we need to supply date format somewhere?
    StopDate = SubmittedAttribute('StopDate', date)  # Do we need to supply date format somewhere?
    LinkType = SubmittedAttribute('LinkType', str)
    LinkFrom = SubmittedAttribute('LinkFrom', int)
    LinkTo = SubmittedAttribute('LinkTo', int)
    OrderID = SubmittedAttribute('OrderID', int)
    Version = SubmittedAttribute('Version', str)
    ADMLink = SubmittedAttribute('ADMLink', str)
    ReportedDateTime = AssignableAttribute('ReportedDateTime', datetime)
    ReportedDate = AssignableAttribute('ReportedDate', date)
    ReportingPeriodStartDate = AssignableAttribute('ReportingPeriodStartDate', date)
    ReportingPeriodEndDate = AssignableAttribute('ReportingPeriodEndDate', date)
    SourceSystem = DerivedAttributePlaceholder('SourceSystem', str)
    NHSLegallyExcluded = DerivedAttributePlaceholder('NHSLegallyExcluded', str)


class EPMAWellSkyPrescriptionModel(_EPMAWellSkyPrescriptionModel):
    __concrete__ = True

    META = SubmittedAttribute('META', META)

    SourceSystem = DerivedAttribute(
        'SourceSystem', str,
        Literal('WellSky'))

    NHSLegallyExcluded = DerivedAttribute(
        'NHSLegallyExcluded', str,
        CompareTwoColumnsToTwoLists(input1=Select(_EPMAWellSkyPrescriptionModel.MDDF),
                                    lookup1=Literal(MDDFLegallyExcludedCodes.MDDF_Legally_Sensitive_Lookup),
                                    input2=Select(_EPMAWellSkyPrescriptionModel.DmdId),
                                    lookup2=Literal(DmdLegallyExcludedCodes.Dmd_Legally_Sensitive_Lookup),
                                    val_if_true=Literal('Removed'),
                                    val_if_false=Select(_EPMAWellSkyPrescriptionModel.NHS)))
