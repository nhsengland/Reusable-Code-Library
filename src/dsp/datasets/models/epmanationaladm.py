from datetime import date, datetime

from dsp.datasets.epma_national.constants import (
    EPMANationalAdministrationDerivedColumns,
    EPMANationalCollection,
)
from dsp.datasets.models.epmanationaladm_base import (
    _EPMAHeaderModel,
    _EPMAAdministrationModel,
    _EPMAChemicalModel,
)
from dsp.model.ons_record import ONSRecordPaths
from dsp.common.epma_national_expressions import (
    ReportingPeriodStartMonth,
    ValidateTreatmentSiteOdsCode,
    TreatmentTrustODScode,
    CheckLegallyRestrictedDoB,
    NHSNumberPdsCheck,
    NotSensitiveAndPDSCheckIsOne,
    GetIMDDecileYearEPMA,
    AgeAtDateEPMA,
    GetIMDDecileEPMA,
    DeriveStandardFiltered,
    DeriveDoseUnitFiltered,
)
from dsp.common.pcaremeds_expressions import Or
from dsp.common.expressions import (
    CCGFromGPPracticeCode,
    CcgFromPostcode,
    DerivePostcode,
    PostcodeFromODS,
    Select,
    ProviderFromSiteCode,
    Literal,
    If,
    LowerLayerSuperOutputAreaFromPostcode,
    NotNull,
)
from dsp.common.structured_model import (
    AssignableAttribute,
    DerivedAttribute,
    MPSConfidenceScores,
    SubmittedAttribute,
    RepeatingSubmittedAttribute,
    META,
)

__all__ = ["_EPMAHeaderModel", "_EPMAAdministrationModel", "_EPMAChemicalModel"]


class EPMAHeaderModel(_EPMAHeaderModel):
    """
    epmaadmin_header
    """

    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAA_HEADER


class EPMAChemicalModel(_EPMAChemicalModel):
    """
    epmaadmin_chemical
    """

    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAA_ACTIVE_ING


class EPMAAdministrationModel(_EPMAAdministrationModel):
    """
    epmaadmin_administration
    """

    __concrete__ = True
    __table__ = EPMANationalCollection.EPMAA_ADMIN

    META = SubmittedAttribute("META", META)
    Header = SubmittedAttribute(EPMANationalCollection.EPMAA_HEADER, EPMAHeaderModel)
    MedicationAdministrationActiveIngredientSubstance = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAA_ACTIVE_ING, EPMAChemicalModel
    )
    RP_START_MONTH = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.RP_START_MONTH,
        int,
        ReportingPeriodStartMonth(
            Select(_EPMAAdministrationModel.Header.ReportingPeriodStartDate)
        ),
    )
    MPS_CONFIDENCE = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.MPS_CONFIDENCE, MPSConfidenceScores
    )
    # ? Technically should be in the Header, but do we care considering it all gets split out in the views anyway?
    SOURCE_SYSTEM_TRUST_ODS_CODE = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.SOURCE_SYSTEM_TRUST_ODS_CODE,
        str,
        ProviderFromSiteCode(
            Select(
                _EPMAAdministrationModel.Header.OrganisationSiteIdentifierSystemLocation
            ),
            Literal(None),
        ),
    )
    TREATMENT_SITE_ODS_CODE_CHECK = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.TREATMENT_SITE_ODS_CODE_CHECK,
        str,
        ValidateTreatmentSiteOdsCode(
            Select(_EPMAAdministrationModel.OrganisationSiteIdentifierOfTreatment),
            Select(
                _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
            ),
        ),
    )
    TREATMENT_SITE_ODS_CODE_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.TREATMENT_SITE_ODS_CODE_FILTERED,
        str,
        If(
            condition=(
                Select(_EPMAAdministrationModel.TREATMENT_SITE_ODS_CODE_CHECK)
                == Literal("01")
            ),
            then=Select(_EPMAAdministrationModel.OrganisationSiteIdentifierOfTreatment),
            otherwise=Literal(None),
        ),
    )
    TREATMENT_TRUST_ODS_CODE = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.TREATMENT_TRUST_ODS_CODE,
        str,
        TreatmentTrustODScode(
            Select(_EPMAAdministrationModel.TREATMENT_SITE_ODS_CODE_CHECK),
            Select(_EPMAAdministrationModel.OrganisationSiteIdentifierOfTreatment),
            Select(
                _EPMAAdministrationModel.MedicationAdministrationRecordLastUpdatedTimeStamp
            ),
        ),
    )
    DATE_OF_BIRTH = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.DATE_OF_BIRTH,
        date,
        CheckLegallyRestrictedDoB(
            Select(_EPMAAdministrationModel.NHS_NUMBER_LEGALLY_RESTRICTED),
            Select(_EPMAAdministrationModel.PersonBirthDate),
        ),
    )
    NHS_NUMBER_PDS_CHECK = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.NHS_NUMBER_PDS_CHECK,
        str,
        NHSNumberPdsCheck(
            Select(_EPMAAdministrationModel.MPS_CONFIDENCE),
            Select(_EPMAAdministrationModel.PersonBirthDate),
            Select(_EPMAAdministrationModel.NHSNumber),
            Select(_EPMAAdministrationModel.NHS_NUMBER_LEGALLY_RESTRICTED),
        ),
    )
    AGE = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.AGE,
        int,
        If(
            condition=NotSensitiveAndPDSCheckIsOne(
                Select(_EPMAAdministrationModel.NHS_NUMBER_LEGALLY_RESTRICTED),
                Select(_EPMAAdministrationModel.NHS_NUMBER_PDS_CHECK),
            ),
            then=AgeAtDateEPMA(
                Select(_EPMAAdministrationModel.PersonBirthDate),
                Select(
                    _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                ),
            ),
            otherwise=Literal(None),
        ),
    )
    LSOA_OF_RESIDENCE = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.LSOA_OF_RESIDENCE,
        str,
        LowerLayerSuperOutputAreaFromPostcode(
            Select(_EPMAAdministrationModel.PATIENT_POSTCODE),
            Select(
                _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
            ),
        ),
    )
    LSOA_OF_REGISTRATION = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.LSOA_OF_REGISTRATION,
        str,
        DerivePostcode(
            PostcodeFromODS(
                Select(_EPMAAdministrationModel.REGISTERED_GP_PRACTICE),
                Select(
                    _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                ),
            ),
            Select(
                _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
            ),
            ONSRecordPaths.LOWER_LAYER_SOA,
        ),
    )
    LSOA_VERSION = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.LSOA_VERSION,
        str,
        Or(
            conditions=[
                NotNull(Select(_EPMAAdministrationModel.LSOA_OF_RESIDENCE)),
                NotNull(Select(_EPMAAdministrationModel.LSOA_OF_REGISTRATION)),
            ],
            then=Literal("LSOA11"),
            otherwise=Literal(None),
        ),
    )
    DEPRIVATION_IMD_VERSION = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.DEPRIVATION_IMD_VERSION,
        str,
        If(
            condition=NotNull(Select(_EPMAAdministrationModel.LSOA_OF_RESIDENCE)),
            then=GetIMDDecileYearEPMA(
                Select(_EPMAAdministrationModel.LSOA_OF_RESIDENCE),
                Select(
                    _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                ),
            ),
            otherwise=Literal(None),
        ),
    )
    CCG_OF_RESIDENCE = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.CCG_OF_RESIDENCE,
        str,
        CcgFromPostcode(
            Select(_EPMAAdministrationModel.PATIENT_POSTCODE),
            Select(
                _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
            ),
        ),
    )
    CCG_OF_REGISTRATION = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.CCG_OF_REGISTRATION,
        str,
        CCGFromGPPracticeCode(
            Select(_EPMAAdministrationModel.REGISTERED_GP_PRACTICE),
            Select(
                _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
            ),
        ),
    )
    LA_DISTRICT_OF_RESIDENCE = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.LA_DISTRICT_OF_RESIDENCE,
        str,
        DerivePostcode(
            Select(_EPMAAdministrationModel.PATIENT_POSTCODE),
            Select(
                _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
            ),
            ONSRecordPaths.UNITARY_AUTHORITY,
        ),
    )
    LA_DISTRICT_OF_REGISTRATION = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.LA_DISTRICT_OF_REGISTRATION,
        str,
        DerivePostcode(
            PostcodeFromODS(
                Select(_EPMAAdministrationModel.REGISTERED_GP_PRACTICE),
                Select(
                    _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                ),
            ),
            Select(
                _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
            ),
            ONSRecordPaths.UNITARY_AUTHORITY,
        ),
    )
    INTEGRATED_CARE_SYSTEM_OF_RESIDENCE = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.INTEGRATED_CARE_SYSTEM_OF_RESIDENCE,
        str,
        If(
            condition=(
                Select(
                    _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                )
                >= Literal(datetime(2022, 7, 1, 0, 0, 0))
            ),
            then=DerivePostcode(
                Select(_EPMAAdministrationModel.PATIENT_POSTCODE),
                Select(
                    _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                ),
                ONSRecordPaths.ICB,
            ),
            otherwise=Literal(None),
        ),
    )
    INTEGRATED_CARE_SYSTEM_OF_REGISTRATION = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.INTEGRATED_CARE_SYSTEM_OF_REGISTRATION,
        str,
        If(
            condition=(
                Select(
                    _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                )
                >= Literal(datetime(2022, 7, 1, 0, 0, 0))
            ),
            then=DerivePostcode(
                PostcodeFromODS(
                    Select(_EPMAAdministrationModel.REGISTERED_GP_PRACTICE),
                    Select(
                        _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                    ),
                ),
                Select(
                    _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
                ),
                ONSRecordPaths.ICB,
            ),
            otherwise=Literal(None),
        ),
    )
    IMD_DECILE = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.IMD_DECILE,
        str,
        GetIMDDecileEPMA(
            Select(_EPMAAdministrationModel.LSOA_OF_RESIDENCE),
            Select(
                _EPMAAdministrationModel.PrescribedMedicationDoseToBeAdministeredTimestamp
            ),
            Select(_EPMAAdministrationModel.DEPRIVATION_IMD_VERSION),
        ),
    )
    FORM_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.FORM_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdministrationModel.FORM_FILTERED_CHECK),
            Select(_EPMAAdministrationModel.MedicationAdministrationDoseFormDesc),
            False,
        ),
    )
    FORM_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.FORM_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdministrationModel.FORM_SNOMED_CT_CHECK),
            Select(_EPMAAdministrationModel.MedicationAdministrationDoseForm_SnomedCt),
            True,
        ),
    )
    SITE_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.SITE_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdministrationModel.SITE_FILTERED_CHECK),
            Select(_EPMAAdministrationModel.BodySiteOfAdministrationActualDesc),
            False,
        ),
    )
    SITE_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.SITE_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdministrationModel.SITE_SNOMED_CT_CHECK),
            Select(_EPMAAdministrationModel.BodySiteOfAdministration_SnomedCt),
            True,
        ),
    )
    ROUTE_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.ROUTE_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdministrationModel.ROUTE_FILTERED_CHECK),
            Select(_EPMAAdministrationModel.RouteOfAdministrationActualDesc),
            False,
        ),
    )
    ROUTE_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.ROUTE_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdministrationModel.ROUTE_SNOMED_CT_CHECK),
            Select(_EPMAAdministrationModel.RouteOfAdministration_SnomedCt),
            True,
        ),
    )
    METHOD_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.METHOD_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdministrationModel.METHOD_FILTERED_CHECK),
            Select(_EPMAAdministrationModel.MethodOfAdministrationActualDesc),
            False,
        ),
    )
    METHOD_SNOMED_CT_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.METHOD_SNOMED_CT_FILTERED,
        str,
        DeriveStandardFiltered(
            Select(_EPMAAdministrationModel.METHOD_SNOMED_CT_CHECK),
            Select(_EPMAAdministrationModel.MethodOfAdministrationActual_SnomedCt),
            True,
        ),
    )
    DOSE_QUANTITY_UNIT_FILTERED = DerivedAttribute(
        EPMANationalAdministrationDerivedColumns.DOSE_QUANTITY_UNIT_FILTERED,
        str,
        DeriveDoseUnitFiltered(
            Select(
                _EPMAAdministrationModel.MedicationAdministrationDoseQuantityValueUnitOfMeasurementDesc
            ),
            Select(_EPMAAdministrationModel.DOSE_QUANTITY_UNIT_FILTERED_CHECK),
        ),
    )
