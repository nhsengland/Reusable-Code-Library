from datetime import date, datetime
from typing import List

from dsp.datasets.epma_national.constants import (
    EPMANationalCollection,
    EPMANationalPrescriptionDerivedColumns,
)
from dsp.datasets.models.epmanationalpres_base import (
    _EPMAHeader,
    _EPMAAdditionalModel,
    _EPMAChemicalModel,
    _EPMADosageModel,
    _EPMAMedicationIndicationModel,
    _EPMAPrescriptionModel,
)
from dsp.model.ons_record import ONSRecordPaths
from dsp.common.epma_national_expressions import (
    ReportingPeriodStartMonth,
    ValidateTreatmentSiteOdsCode,
    TreatmentTrustODScode,
    CheckLegallyRestrictedDoB,
    NHSNumberPdsCheck,
    NotSensitiveAndPDSCheckIsOne,
    GetIMDDecileEPMA,
    GetIMDDecileYearEPMA,
    AgeAtDateEPMA,
    DeriveStandardFiltered,
    DeriveDoseUnitFiltered,
    DeriveWhenFiltered,
)
from dsp.common.pcaremeds_expressions import Or
from dsp.common.expressions import (
    CCGFromGPPracticeCode,
    CcgFromPostcode,
    Coalesce,
    DerivePostcode,
    PostcodeFromODS,
    Select,
    If,
    ProviderFromSiteCode,
    NotNull,
    Literal,
    LowerLayerSuperOutputAreaFromPostcode,
)
from dsp.common.structured_model import (
    AssignableAttribute,
    DerivedAttribute,
    MPSConfidenceScores,
    SubmittedAttribute,
    RepeatingSubmittedAttribute,
    META,
)


__all__ = [
    "EPMAHeaderModel",
    "EPMAAdditionalModel",
    "EPMAChemicalModel",
    "EPMADosageModel",
    "EPMAMedicationIndicationModel",
    "EPMAPrescriptionModel",
]


class EPMAHeaderModel(_EPMAHeader):
    """
    epmapresc_header
    """

    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAP_HEADER


class EPMAAdditionalModel(_EPMAAdditionalModel):
    """
    epmapresc_additional
    """

    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAP_ADD

    ADDITIONAL_INSTRUCTION_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.ADDITIONAL_INSTRUCTION_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdditionalModel.ADDITIONAL_INSTRUCTION_SNOMED_CT_CHECK),
            Select(
                _EPMAAdditionalModel.PrescribedMedicationAdditionalDosageInstruction_SnomedCt
            ),
            True,
        ),
    )


class EPMAChemicalModel(_EPMAChemicalModel):
    """
    epmapresc_chemical
    """

    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAP_CHEM


class EPMADosageModel(_EPMADosageModel):
    """
    epmapresc_dosage
    """

    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAP_DOSAGE

    PrescribedMedicationAdditionalDosageInstructions = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAP_ADD, EPMAAdditionalModel
    )
    SITE_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.SITE_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMADosageModel.SITE_FILTERED_CHECK),
            Select(_EPMADosageModel.BodySiteOfAdministrationPrescribedDesc),
            False,
        ),
    )
    SITE_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.SITE_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMADosageModel.SITE_SNOMED_CT_CHECK),
            Select(_EPMADosageModel.BodySiteOfAdministrationPrescribed_SnomedCt),
            True,
        ),
    )
    ROUTE_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.ROUTE_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMADosageModel.ROUTE_FILTERED_CHECK),
            Select(_EPMADosageModel.RouteOfAdministrationPrescribedDesc),
            False,
        ),
    )
    ROUTE_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.ROUTE_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMADosageModel.ROUTE_SNOMED_CT_CHECK),
            Select(_EPMADosageModel.RouteOfAdministrationPrescribed_SnomedCt),
            True,
        ),
    )
    METHOD_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.METHOD_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMADosageModel.METHOD_FILTERED_CHECK),
            Select(_EPMADosageModel.MethodOfAdministrationPrescribedDesc),
            False,
        ),
    )
    METHOD_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.METHOD_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMADosageModel.METHOD_SNOMED_CT_CHECK),
            Select(_EPMADosageModel.MethodOfAdministrationPrescribed_SnomedCt),
            True,
        ),
    )
    DOSE_QUANTITY_UNIT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSE_QUANTITY_UNIT_FILTERED,
        str,
        DeriveDoseUnitFiltered(
            Select(
                _EPMADosageModel.PrescribedMedicationDoseQuantityValueUnitOfMeasurementDesc
            ),
            Select(_EPMADosageModel.DOSE_QUANTITY_UNIT_FILTERED_CHECK),
        ),
    )
    DOSERANGE_LOW_UNIT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSERANGE_LOW_UNIT_FILTERED,
        str,
        DeriveDoseUnitFiltered(
            Select(
                _EPMADosageModel.PrescribedMedicationDoseRangeLowQuantityValueUnitOfMeasurementDesc
            ),
            Select(_EPMADosageModel.DOSERANGE_LOW_UNIT_FILTERED_CHECK),
        ),
    )
    DOSERANGE_HIGH_UNIT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSERANGE_HIGH_UNIT_FILTERED,
        str,
        DeriveDoseUnitFiltered(
            Select(
                _EPMADosageModel.PrescribedMedicationDoseRangeHighQuantityValueUnitOfMeasurementDesc
            ),
            Select(_EPMADosageModel.DOSERANGE_HIGH_UNIT_FILTERED_CHECK),
        ),
    )
    WHEN_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.WHEN_FILTERED,
        List[str],
        DeriveWhenFiltered(
           Select(_EPMADosageModel.PrescribedMedicationDoseAssociatedEvent_FHIRR4),
           Select(_EPMADosageModel.WHEN_FILTERED_CHECK),
        ),
    )


class EPMAMedicationIndicationModel(_EPMAMedicationIndicationModel):
    """
    epmapresc_medication_indication
    """

    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAP_MEDIND

    INDICATION_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.INDICATION_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAMedicationIndicationModel.INDICATION_SNOMED_CT_CHECK),
            Select(_EPMAMedicationIndicationModel.TherapeuticIndicationCode_SnomedCt),
            True,
        ),
    )


class EPMAPrescriptionModel(_EPMAPrescriptionModel):
    """
    epmapresc_presc
    """

    _EPMAPrescriptionModel
    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAP_PRESC

    META = SubmittedAttribute("META", META)
    Header = SubmittedAttribute(EPMANationalCollection.EPMAP_HEADER, EPMAHeaderModel)
    PrescribedMedicationActiveIngredientSubstance = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAP_CHEM, EPMAChemicalModel
    )
    PrescribedMedicationDosage = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAP_DOSAGE, EPMADosageModel
    )
    PrescribedMedicationTherapeuticIndication = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAP_MEDIND, EPMAMedicationIndicationModel
    )
    RP_START_MONTH = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.RP_START_MONTH,
        int,
        ReportingPeriodStartMonth(
            Select(_EPMAPrescriptionModel.Header.ReportingPeriodStartDate)
        ),
    )
    SOURCE_SYSTEM_TRUST_ODS_CODE = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.SOURCE_SYSTEM_TRUST_ODS_CODE,
        str,
        ProviderFromSiteCode(
            Select(
                _EPMAPrescriptionModel.Header.OrganisationSiteIdentifierSystemLocation
            ),
            Literal(None),
        ),
    )
    TREATMENT_SITE_ODS_CODE_CHECK = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.TREATMENT_SITE_ODS_CODE_CHECK,
        str,
        ValidateTreatmentSiteOdsCode(
            Select(_EPMAPrescriptionModel.OrganisationSiteIdentifierOfTreatment),
            Coalesce(
                [
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                    ),
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                    ),
                ]
            ),
        ),
    )
    TREATMENT_TRUST_ODS_CODE = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.TREATMENT_TRUST_ODS_CODE,
        str,
        TreatmentTrustODScode(
            Select(_EPMAPrescriptionModel.TREATMENT_SITE_ODS_CODE_CHECK),
            Select(_EPMAPrescriptionModel.OrganisationSiteIdentifierOfTreatment),
            Coalesce(
                [
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                    ),
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                    ),
                ]
            ),
        ),
    )
    TREATMENT_SITE_ODS_CODE_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.TREATMENT_SITE_ODS_CODE_FILTERED,
        str,
        If(
            condition=(
                Select(_EPMAPrescriptionModel.TREATMENT_SITE_ODS_CODE_CHECK)
                == Literal("01")
            ),
            then=Select(_EPMAPrescriptionModel.OrganisationSiteIdentifierOfTreatment),
            otherwise=Literal(None),
        ),
    )
    DATE_OF_BIRTH = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.DOB,
        date,
        CheckLegallyRestrictedDoB(
            Select(_EPMAPrescriptionModel.NHS_NUMBER_LEGALLY_RESTRICTED),
            Select(_EPMAPrescriptionModel.PersonBirthDate),
        ),
    )
    MPS_CONFIDENCE = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.MPS_CONFIDENCE, MPSConfidenceScores
    )
    NHS_NUMBER_PDS_CHECK = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.NHS_NUMBER_PDS_CHECK,
        str,
        NHSNumberPdsCheck(
            Select(_EPMAPrescriptionModel.MPS_CONFIDENCE),
            Select(_EPMAPrescriptionModel.PersonBirthDate),
            Select(_EPMAPrescriptionModel.NHSNumber),
            Select(_EPMAPrescriptionModel.NHS_NUMBER_LEGALLY_RESTRICTED),
        ),
    )
    AGE = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.AGE,
        int,
        If(
            condition=NotSensitiveAndPDSCheckIsOne(
                Select(_EPMAPrescriptionModel.NHS_NUMBER_LEGALLY_RESTRICTED),
                Select(_EPMAPrescriptionModel.NHS_NUMBER_PDS_CHECK),
            ),
            then=AgeAtDateEPMA(
                Select(_EPMAPrescriptionModel.PersonBirthDate),
                Select(_EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp),
                Select(
                    _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                ),
            ),
            otherwise=Literal(None),
        ),
    )
    LSOA_OF_RESIDENCE = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.LSOA_OF_RESIDENCE,
        str,
        LowerLayerSuperOutputAreaFromPostcode(
            Select(_EPMAPrescriptionModel.PATIENT_POSTCODE),
            Select(_EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp),
            Select(
                _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
            ),
        ),
    )
    LSOA_OF_REGISTRATION = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.LSOA_OF_REGISTRATION,
        str,
        DerivePostcode(
            PostcodeFromODS(
                Select(_EPMAPrescriptionModel.REGISTERED_GP_PRACTICE),
                Coalesce(
                    [
                        Select(
                            _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                        ),
                        Select(
                            _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                        ),
                    ]
                ),
            ),
            Coalesce(
                [
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                    ),
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                    ),
                ]
            ),
            ONSRecordPaths.LOWER_LAYER_SOA,
        ),
    )
    LSOA_VERSION = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.LSOA_VERSION,
        str,
        Or(
            conditions=[
                NotNull(Select(_EPMAPrescriptionModel.LSOA_OF_RESIDENCE)),
                NotNull(Select(_EPMAPrescriptionModel.LSOA_OF_REGISTRATION)),
            ],
            then=Literal("LSOA11"),
            otherwise=Literal(None),
        ),
    )
    CCG_OF_RESIDENCE = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.CCG_OF_RESIDENCE,
        str,
        CcgFromPostcode(
            Select(_EPMAPrescriptionModel.PATIENT_POSTCODE),
            Coalesce(
                [
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                    ),
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                    ),
                ]
            ),
        ),
    )
    CCG_OF_REGISTRATION = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.CCG_OF_REGISTRATION,
        str,
        CCGFromGPPracticeCode(
            Select(_EPMAPrescriptionModel.REGISTERED_GP_PRACTICE),
            Coalesce(
                [
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                    ),
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                    ),
                ]
            ),
        ),
    )
    LA_DISTRICT_OF_RESIDENCE = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.LA_DISTRICT_OF_RESIDENCE,
        str,
        DerivePostcode(
            Select(_EPMAPrescriptionModel.PATIENT_POSTCODE),
            Coalesce(
                [
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                    ),
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                    ),
                ]
            ),
            ONSRecordPaths.UNITARY_AUTHORITY,
        ),
    )
    LA_DISTRICT_OF_REGISTRATION = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.LA_DISTRICT_OF_REGISTRATION,
        str,
        DerivePostcode(
            PostcodeFromODS(
                Select(_EPMAPrescriptionModel.REGISTERED_GP_PRACTICE),
                Coalesce(
                    [
                        Select(
                            _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                        ),
                        Select(
                            _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                        ),
                    ]
                ),
            ),
            Coalesce(
                [
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                    ),
                    Select(
                        _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                    ),
                ]
            ),
            ONSRecordPaths.UNITARY_AUTHORITY,
        ),
    )
    INTEGRATED_CARE_SYSTEM_OF_RESIDENCE = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.INTEGRATED_CARE_SYSTEM_OF_RESIDENCE,
        str,
        If(
            condition=(
                Coalesce([Select(_EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp),
                          Select(_EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp)])
                >= Literal(datetime(2022, 7, 1, 0, 0, 0))
            ),
            then=DerivePostcode(
                Select(_EPMAPrescriptionModel.PATIENT_POSTCODE),
                Coalesce(
                    [
                        Select(
                            _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                        ),
                        Select(
                            _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                        ),
                    ]
                ),
                ONSRecordPaths.ICB,
            ),
            otherwise=Literal(None),
        ),
    )
    INTEGRATED_CARE_SYSTEM_OF_REGISTRATION = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.INTEGRATED_CARE_SYSTEM_OF_REGISTRATION,
        str,
        If(
            condition=(
                Coalesce([Select(_EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp),
                          Select(_EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp)])
                >= Literal(datetime(2022, 7, 1, 0, 0, 0))
            ),
            then=DerivePostcode(
                PostcodeFromODS(
                    Select(_EPMAPrescriptionModel.REGISTERED_GP_PRACTICE),
                    Coalesce(
                        [
                            Select(
                                _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                            ),
                            Select(
                                _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                            ),
                        ]
                    ),
                ),
                Coalesce(
                    [
                        Select(
                            _EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp
                        ),
                        Select(
                            _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
                        ),
                    ]
                ),
                ONSRecordPaths.ICB,
            ),
            otherwise=Literal(None),
        ),
    )
    DEPRIVATION_IMD_VERSION = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.DEPRIVATION_IMD_VERSION,
        str,
        GetIMDDecileYearEPMA(
            Select(_EPMAPrescriptionModel.LSOA_OF_RESIDENCE),
            Select(_EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp),
            Select(
                _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
            ),
        ),
    )
    IMD_DECILE = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.IMD_DECILE,
        str,
        GetIMDDecileEPMA(
            Select(_EPMAPrescriptionModel.LSOA_OF_RESIDENCE),
            Select(_EPMAPrescriptionModel.PrescribedMedicationAuthorisedTimestamp),
            Select(_EPMAPrescriptionModel.DEPRIVATION_IMD_VERSION),
            Select(
                _EPMAPrescriptionModel.PrescribedMedicationRecordLastUpdatedTimestamp
            ),
        ),
    )
    FORM_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.FORM_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAPrescriptionModel.FORM_FILTERED_CHECK),
            Select(_EPMAPrescriptionModel.PrescribedMedicationDoseFormDescription),
            False,
        ),
    )
    FORM_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalPrescriptionDerivedColumns.FORM_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAPrescriptionModel.FORM_SNOMED_CT_CHECK),
            Select(_EPMAPrescriptionModel.PrescribedMedicationDoseForm_SnomedCt),
            True,
        ),
    )
