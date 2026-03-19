from datetime import date, datetime
from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes, OdsOldMddfUsers
from dsp.common.structured_model import _META, DerivedAttribute, DerivedAttributePlaceholder, \
    DSPStructuredModel, META, SubmittedAttribute, AssignableAttribute
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes, EPMAPDSEnrichmentFields
from dsp.common.expressions import Literal, Select, ValToDateTime, ToDate, Concat, StringToDateTime, Equals, \
    If, Includes
from dsp.common.epma_expressions import CompareTwoColumnsToTwoLists, FieldBeginsWith, ConvertLegacyMddf
from dsp.shared.constants import DS


class _EPMAWellSkyAdministrationModel2(DSPStructuredModel):
    META = SubmittedAttribute(CommonFields.META, _META)

    ODS = SubmittedAttribute('ODS', str)
    SPR = SubmittedAttribute('SPR', str)
    ADMLink = SubmittedAttribute('ADMLink', str)
    NHS = SubmittedAttribute('NHS', str)
    Drug = SubmittedAttribute('Drug', str)
    Route = SubmittedAttribute('Route', str)
    PRN = SubmittedAttribute('PRN', str)
    OrderID = SubmittedAttribute('OrderID', int)
    ScheduledAdmin = SubmittedAttribute('ScheduledAdmin', datetime)
    GivenAdminDateTime = SubmittedAttribute('GivenAdminDateTime', datetime)
    AdminReasonNotGiven = SubmittedAttribute('AdminReasonNotGiven', str)
    TransactionDateTime = SubmittedAttribute('TransactionDateTime', datetime)
    MDDF = SubmittedAttribute('MDDF', str)
    DmdId = SubmittedAttribute('DmdId', str)
    RunDate = SubmittedAttribute('RunDate', str)
    RunTime = SubmittedAttribute('RunTime', str)

    AdministrationPrimaryKey = DerivedAttributePlaceholder('AdministrationPrimaryKey', str)
    ReportedDateTime = DerivedAttributePlaceholder('ReportedDateTime', datetime)
    SourceSystemType = DerivedAttributePlaceholder('SourceSystemType', str)
    TempMddf = DerivedAttributePlaceholder('TempMddf', str)
    NHSorCHINumber = DerivedAttributePlaceholder('NHSorCHINumber', str)
    LocalPatientIdentifier = DerivedAttributePlaceholder('LocalPatientIdentifier', str)
    Source = DerivedAttributePlaceholder('Source', str)
    DosageAsNeededBoolean = DerivedAttributePlaceholder('DosageAsNeededBoolean', str)
    NHSlrRemoved = DerivedAttributePlaceholder('NHSlrRemoved', str)

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


class EPMAWellSkyAdministrationModel2(_EPMAWellSkyAdministrationModel2):
    __concrete__ = True
    __table__ = DS.EPMAWSAD2

    META = SubmittedAttribute('META', META)

    AdministrationPrimaryKey = DerivedAttribute('AdministrationPrimaryKey', str,
                                                Concat([Select(_EPMAWellSkyAdministrationModel2.ODS),
                                                        Select(_EPMAWellSkyAdministrationModel2.ADMLink),
                                                        Select(_EPMAWellSkyAdministrationModel2.ScheduledAdmin)]
                                                       )
                                                )

    ReportedDateTime = DerivedAttribute('ReportedDateTime', datetime,
                                        ValToDateTime(
                                            Concat([Select(_EPMAWellSkyAdministrationModel2.RunDate), Literal(" "),
                                                    Select(_EPMAWellSkyAdministrationModel2.RunTime)]),
                                            '%d/%m/%Y %H:%M:%S')
                                        )

    SourceSystemType = DerivedAttribute(
        'SourceSystemType', str,
        Literal('WellSky')
    )

    TempMddf = DerivedAttribute(
        'TempMddf', str,
        If(Includes(Select(_EPMAWellSkyAdministrationModel2.ODS), Literal(OdsOldMddfUsers.Ods_legacy_Mddf_Lookup)),
           ConvertLegacyMddf(Select(_EPMAWellSkyAdministrationModel2.MDDF)),
           Select(_EPMAWellSkyAdministrationModel2.MDDF))
    )

    NHSlrRemoved = DerivedAttribute(
        'NHSlrRemoved', str,
        CompareTwoColumnsToTwoLists(input1=Select(_EPMAWellSkyAdministrationModel2.TempMddf),
                                    lookup1=Literal(MDDFLegallyExcludedCodes.MDDF_Legally_Sensitive_Lookup),
                                    input2=Select(_EPMAWellSkyAdministrationModel2.DmdId),
                                    lookup2=Literal(DmdLegallyExcludedCodes.Dmd_Legally_Sensitive_Lookup),
                                    val_if_true=Literal('Removed'),
                                    val_if_false=Select(_EPMAWellSkyAdministrationModel2.NHS)),
    )

    NHSorCHINumber = DerivedAttribute(
        'NHSorCHINumber', str,
        If(Equals(Select(_EPMAWellSkyAdministrationModel2.NHSlrRemoved), Literal('Removed')),
           Literal('Removed'),
           If(Equals(FieldBeginsWith(Select(_EPMAWellSkyAdministrationModel2.NHS), Literal('P')), Literal(False)),
              Select(_EPMAWellSkyAdministrationModel2.NHS),
              Literal(None)))
    )

    LocalPatientIdentifier = DerivedAttribute(
        'LocalPatientIdentifier', str,
        If(Equals(Select(_EPMAWellSkyAdministrationModel2.NHSlrRemoved), Literal('Removed')),
           Literal('Removed'),
           If(Equals(FieldBeginsWith(Select(_EPMAWellSkyAdministrationModel2.NHS), Literal('P')), Literal(True)),
              Select(_EPMAWellSkyAdministrationModel2.NHS),
              Literal(None)))
    )

    Source = DerivedAttribute(
        'Source', str,
        Literal('Recorded by Trust')
    )

    DosageAsNeededBoolean = DerivedAttribute('DosageAsNeededBoolean', str,
                                             If(Equals(Select(_EPMAWellSkyAdministrationModel2.PRN),
                                                       Literal('Y'))
                                                , Literal('TRUE'), If(
                                                     Equals(Select(_EPMAWellSkyAdministrationModel2.PRN),
                                                            Literal('N')), Literal('FALSE'),
                                                     Select(_EPMAWellSkyAdministrationModel2.PRN)))
                                             )
