from datetime import date, datetime
from dsp.datasets.common import Fields as CommonFields
from dsp.common.structured_model import _META, Decimal_16_6, DerivedAttribute, DerivedAttributePlaceholder, \
    DSPStructuredModel, META, SubmittedAttribute, AssignableAttribute
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes, EPMAPDSEnrichmentFields
from dsp.common.expressions import Literal, Select, ValToDateTime, Concat, \
    Equals, If, Includes
from dsp.common.epma_expressions import CompareTwoColumnsToTwoLists, FieldBeginsWith, \
    ConvertLegacyMddf
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes, \
    OdsOldMddfUsers

from dsp.shared.constants import DS


class _EPMAWellSkyPrescriptionModel2(DSPStructuredModel):
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
    OrderChangeDate = SubmittedAttribute('OrderChangeDate', datetime)
    DoseStatus = SubmittedAttribute('DoseStatus', str)
    PriDose = SubmittedAttribute('PriDose', Decimal_16_6)
    PriDoseUnit = SubmittedAttribute('PriDoseUnit', str)
    SecDose = SubmittedAttribute('SecDose', Decimal_16_6)
    SecDoseUnit = SubmittedAttribute('SecDoseUnit', str)
    Route = SubmittedAttribute('Route', str)
    Frequency = SubmittedAttribute('Frequency', str)
    PRN = SubmittedAttribute('PRN', str)
    PRNReason = SubmittedAttribute('PRNReason', str)
    OriginalStartDate = SubmittedAttribute('OriginalStartDate', date)
    StopDate = SubmittedAttribute('StopDate', date)
    LinkType = SubmittedAttribute('LinkType', str)
    LinkFrom = SubmittedAttribute('LinkFrom', int)
    LinkTo = SubmittedAttribute('LinkTo', int)
    OrderID = SubmittedAttribute('OrderID', int)
    Version = SubmittedAttribute('Version', str)
    ADMLink = SubmittedAttribute('ADMLink', str)
    RunDate = SubmittedAttribute('RunDate', str)
    RunTime = SubmittedAttribute('RunTime', str)
    OrderDoseInactiveDate = SubmittedAttribute('OrderDoseInactiveDate', datetime)
    PrescriptionPrimaryKey = DerivedAttributePlaceholder('PrescriptionPrimaryKey', str)
    ReportedDateTime = DerivedAttributePlaceholder('ReportedDateTime', datetime)
    SourceSystemType = DerivedAttributePlaceholder('SourceSystemType', str)
    TempMddf = DerivedAttributePlaceholder('TempMddf', str)
    NHSlrRemoved = DerivedAttributePlaceholder('NHSlrRemoved', str)
    LocalPatientIdentifier = DerivedAttributePlaceholder('LocalPatientIdentifier', str)
    TypeAbbr = DerivedAttributePlaceholder('TypeAbbr', str)
    SourceDMD = DerivedAttributePlaceholder('SourceDMD', str)
    MappedDmdCode = DerivedAttributePlaceholder('MappedDmdCode', str)
    SourceMDDF = DerivedAttributePlaceholder('SourceMDDF', str)
    DosageSequence = DerivedAttributePlaceholder('DosageSequence', str)
    DosageAsNeededBoolean = DerivedAttributePlaceholder('DosageAsNeededBoolean', str)
    NHSorCHINumber = DerivedAttributePlaceholder('NHSorCHINumber', str)
    GroupIdentifier = DerivedAttributePlaceholder('GroupIdentifier', str)  # Assignable attr when grouping re-enabled
    GroupOrder = DerivedAttributePlaceholder('GroupOrder', int)  # Assignable attr when grouping re-enable

    # PDS derivations

    PDSPostcode = AssignableAttribute(EPMAPDSEnrichmentFields.PDSPostcode, str)
    PDSDateOfBirth = AssignableAttribute(EPMAPDSEnrichmentFields.PDSDateOfBirth, date)
    PDSGender = AssignableAttribute(EPMAPDSEnrichmentFields.PDSGender, str)
    PDSGPCode = AssignableAttribute(EPMAPDSEnrichmentFields.PDSGPCode, str)
    PatientAge = AssignableAttribute(EPMAPDSEnrichmentFields.PatientAge, int)
    PatientLSOA = AssignableAttribute(EPMAPDSEnrichmentFields.PatientLSOA, str)
    PatientCCG = AssignableAttribute(EPMAPDSEnrichmentFields.PatientCCG, str)
    PatientGPLA = AssignableAttribute(EPMAPDSEnrichmentFields.PatientGPLA, str)
    PatientGPCCG = AssignableAttribute(EPMAPDSEnrichmentFields.PatientGPCCG, str)


class EPMAWellSkyPrescriptionModel2(_EPMAWellSkyPrescriptionModel2):
    __concrete__ = True
    __table__ = DS.EPMAWSPC2

    META = SubmittedAttribute('META', META)

    PrescriptionPrimaryKey = DerivedAttribute(
        'PrescriptionPrimaryKey', str,
        Concat([Select(_EPMAWellSkyPrescriptionModel2.ODS),
                Select(_EPMAWellSkyPrescriptionModel2.ADMLink),
                Select(_EPMAWellSkyPrescriptionModel2.Version)]
               ))
    # Not Tested
    ReportedDateTime = DerivedAttribute(
        'ReportedDateTime', datetime,
        ValToDateTime(Concat([Select(_EPMAWellSkyPrescriptionModel2.RunDate), Literal(" "),
                              Select(_EPMAWellSkyPrescriptionModel2.RunTime)]), '%d/%m/%Y %H:%M:%S')
    )

    SourceSystemType = DerivedAttribute(
        'SourceSystemType', str,
        Literal('WellSky')
    )

    TempMddf = DerivedAttribute(
        'TempMddf', str,
        If(Includes(Select(_EPMAWellSkyPrescriptionModel2.ODS), Literal(OdsOldMddfUsers.Ods_legacy_Mddf_Lookup)),
           ConvertLegacyMddf(Select(_EPMAWellSkyPrescriptionModel2.MDDF)),
           Select(_EPMAWellSkyPrescriptionModel2.MDDF))
    )

    NHSlrRemoved = DerivedAttribute(
        'NHSlrRemoved', str,
        CompareTwoColumnsToTwoLists(input1=Select(_EPMAWellSkyPrescriptionModel2.TempMddf),
                                    lookup1=Literal(MDDFLegallyExcludedCodes.MDDF_Legally_Sensitive_Lookup),
                                    input2=Select(_EPMAWellSkyPrescriptionModel2.DmdId),
                                    lookup2=Literal(DmdLegallyExcludedCodes.Dmd_Legally_Sensitive_Lookup),
                                    val_if_true=Literal('Removed'),
                                    val_if_false=Select(_EPMAWellSkyPrescriptionModel2.NHS)),
    )

    NHSorCHINumber = DerivedAttribute(
        'NHSorCHINumber', str,
        If(Equals(Select(_EPMAWellSkyPrescriptionModel2.NHSlrRemoved), Literal('Removed')),
           Literal('Removed'),
           If(Equals(FieldBeginsWith(Select(_EPMAWellSkyPrescriptionModel2.NHS), Literal('P')), Literal(False)),
              Select(_EPMAWellSkyPrescriptionModel2.NHS),
              Literal(None)))
    )

    LocalPatientIdentifier = DerivedAttribute(
        'LocalPatientIdentifier', str,
        If(Equals(Select(_EPMAWellSkyPrescriptionModel2.NHSlrRemoved), Literal('Removed')),
           Literal('Removed'),
           If(Equals(FieldBeginsWith(Select(_EPMAWellSkyPrescriptionModel2.NHS), Literal('P')), Literal(True)),
              Select(_EPMAWellSkyPrescriptionModel2.NHS),
              Literal(None)))
    )

    TypeAbbr = DerivedAttribute(
        'TypeAbbr', str,
        If(Equals(Select(_EPMAWellSkyPrescriptionModel2.Type), Literal('Inpatient')),
           Literal('I'),
           If(Equals(Select(_EPMAWellSkyPrescriptionModel2.Type), Literal('Outpatient')),
              Literal('O'),
              If(Equals(Select(_EPMAWellSkyPrescriptionModel2.Type), Literal('TTA')),
                 Literal('D'),
                 Select(_EPMAWellSkyPrescriptionModel2.Type))))
    )

    SourceDMD = DerivedAttribute(
        'SourceDMD', str,
        Literal('Recorded by Trust')
    )
    # Mapped DMD code to go here once ref data arrangements in place
    MappedDmdCode = DerivedAttribute(
        'MappedDmdCode', str,
        Literal(None)
    )

    SourceMDDF = DerivedAttribute(
        'SourceMDDF', str,
        Literal('Mapped from Multilex')
    )

    DosageSequence = DerivedAttribute(
        'DosageSequence', str,
        Literal('1')
    )

    DosageAsNeededBoolean = DerivedAttribute(
        'DosageAsNeededBoolean', str,
        If(Equals(Select(_EPMAWellSkyPrescriptionModel2.PRN), Literal('Y')),
           Literal('TRUE'),
           If(Equals(Select(_EPMAWellSkyPrescriptionModel2.PRN), Literal('N')),
              Literal('FALSE'),
              Select(_EPMAWellSkyPrescriptionModel2.PRN)))
    )

    # Temporary additions while grouping logic reviewed and pipeline stage disabled

    GroupIdentifier = DerivedAttribute(
        'GroupIdentifier', str,
        Literal(None)
    )

    GroupOrder = DerivedAttribute(
        'GroupOrder', int,
        Literal(None)
    )
