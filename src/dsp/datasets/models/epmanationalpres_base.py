from datetime import date, datetime
from typing import List

from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.epma_national.constants import (
    EPMANationalCollection,
    EPMANationalPrescriptionHeaderColumns,
    EPMANationalPrescriptionPrescriptionColumns,
    EPMANationalPrescriptionChemicalColumns,
    EPMANationalPrescriptionMedicationIndicationColumns,
    EPMANationalPrescriptionDosageColumns,
    EPMANationalPrescriptionAdditionalColumns,
    EPMANationalPrescriptionDerivedColumns
)
from dsp.common.structured_model import (
    _MPSConfidenceScores,
    AssignableAttribute,
    BaseTableModel,
    RepeatingSubmittedAttribute,
    SubmittedAttribute,
    DerivedAttributePlaceholder,
    _META,
    Decimal_16_6
)
from dsp.common.model_accessor import ModelAttribute
from pyspark.sql.types import (
    StructType,
    IntegerType,
    DecimalType,
    StringType,
    TimestampType,
    DateType,
    BooleanType
)

__all__ = [
    '_EPMAPrescriptionModel',
    '_EPMAAdditionalModel',
    '_EPMAChemicalModel',
    '_EPMADosageModel',
    '_EPMAMedicationIndicationModel',
    '_EPMAHeader'
]


class BaseEPMAPresc(BaseTableModel):
    """
    epma prescription shared field(s) table
    """
    PrescribedItemIdentifier = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_ITEM_IDENTIFIER,
        str,
        spark_type=StringType()
    )
    RowNumber = AssignableAttribute('RowNumber', int, spark_type=IntegerType())


class BaseEPMADosageShared(BaseEPMAPresc):
    """
    epma dosage shared field(s) table
    """
    DOSAGE_ID = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSAGE_ID,
        str,
        spark_type=StringType()
    )


class _EPMAHeader(BaseTableModel):
    """
    epmapresc header table
    """
    OrganisationSiteIdentifierSystemLocation = SubmittedAttribute(
        EPMANationalPrescriptionHeaderColumns.ORGANISATION_SITE_IDENTIFIER_SYSTEM,
        str,
        spark_type=StringType()
    )
    DataSetCreatedTimestamp = SubmittedAttribute(
        EPMANationalPrescriptionHeaderColumns.DATASET_CREATED,
        datetime,
        spark_type=TimestampType()
    )
    PrimSystemInUse = SubmittedAttribute(
        EPMANationalPrescriptionHeaderColumns.PRIMARY_DATA_COLLECTION_SYSTEM_IN_USE,
        str,
        spark_type=StringType()
    )
    ReportingPeriodStartDate = SubmittedAttribute(
        EPMANationalPrescriptionHeaderColumns.REPORTING_PERIOD_START_DATE,
        date,
        spark_type=DateType()
    )
    ReportingPeriodEndDate = SubmittedAttribute(
        EPMANationalPrescriptionHeaderColumns.REPORTING_PERIOD_END_DATE,
        date,
        spark_type=DateType()
    )


class _EPMAChemicalModel(BaseEPMAPresc):
    """
    epma national presc chemical
    """
    PrescribedMedicationActiveIngredientSubstanceDesc = SubmittedAttribute(
        EPMANationalPrescriptionChemicalColumns.PRESCRIBED_MEDICATION_ACTIVE_INGREDIENT_SUBSTANCE_DESC,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationActiveIngredientSubstanceStrengthDesc = SubmittedAttribute(
        EPMANationalPrescriptionChemicalColumns.PRESCRIBED_MEDICATION_ACTIVE_INGREDIENT_SUBTANCE_STRENGTH_DESC,
        str,
        spark_type=StringType()
    )


class _EPMAMedicationIndicationModel(BaseEPMAPresc):
    """
    epmanational presc medication indication
    """
    PrescribedMedicationTherapeuticIndicationDesc = SubmittedAttribute(
        EPMANationalPrescriptionMedicationIndicationColumns.PRESCRIBED_MEDICATION_THERAPEUTIC_INDICATION_DESC,
        str,
        spark_type=StringType()
    )
    TherapeuticIndicationCode_SnomedCt = SubmittedAttribute(
        EPMANationalPrescriptionMedicationIndicationColumns.THERAPEUTIC_INDICATION_SNOMED_CODE,
        str,
        spark_type=StringType()
    )

    # DERIVS

    INDICATION_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.INDICATION_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    INDICATION_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.INDICATION_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )


class _EPMAAdditionalModel(BaseEPMADosageShared):
    """
    epmanational presc additional
    """
    # Inherits PrescribedItemIdentifier, RowNumber and DOSAGE_ID
    PrescribedMedicationAdditionalDosageInstructionDesc = SubmittedAttribute(
        EPMANationalPrescriptionAdditionalColumns.PRESCRIBED_MEDICATION_ADDITIONAL_DOSAGE_INSTRUCTION_DESC,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationAdditionalDosageInstruction_SnomedCt = SubmittedAttribute(
        EPMANationalPrescriptionAdditionalColumns.PRESCRIBED_MEDICATION_ADDITIONAL_DOSAGE_INSTRUCTION_SNOMED_CT,
        str,
        spark_type=StringType()
    )

    # DERIVS

    ADDITIONAL_INSTRUCTION_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.ADDITIONAL_INSTRUCTION_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    ADDITIONAL_INSTRUCTION_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.ADDITIONAL_INSTRUCTION_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )


class _EPMADosageModel(BaseEPMADosageShared):
    """
    epmanational_presc_dosage
    """
    # Inherits PrescribedItemIdentifier, RowNumber and DOSAGE_ID
    PrescribedMedicationDosageInstrSeqNo = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSAGE_INSTRUCTION_SEQUENCE_NO,
        int,
        spark_type=IntegerType()
    )
    PrescribedMedicationDosageInstrDesc = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSAGE_INSTRUCTION_DESC,
        str,
        spark_type=StringType()
    )
    BodySiteOfAdministrationPrescribedDesc = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.BODY_SITE_OF_ADMINISTRATION_PRESCRIBED_DESC,
        str,
        spark_type=StringType()
    )
    BodySiteOfAdministrationPrescribed_SnomedCt = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.BODY_SITE_OF_ADMINISTRATION_PRESCRIBED_SNOMED_CT,
        str,
        spark_type=StringType()
    )
    RouteOfAdministrationPrescribedDesc = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.ROUTE_OF_ADMINISTRATION_PRESCRIBED_DESC,
        str,
        spark_type=StringType()
    )
    RouteOfAdministrationPrescribed_SnomedCt = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.ROUTE_OF_ADMINISTRATION_PRESCRIBED_SNOMED_CT,
        str,
        spark_type=StringType()
    )
    MethodOfAdministrationPrescribedDesc = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.METHOD_OF_ADMINISTRATION_PRESCRIBED_DESC,
        str,
        spark_type=StringType()
    )
    MethodOfAdministrationPrescribed_SnomedCt = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.METHOD_OF_ADMINISTRATION_PRESCRIBED_SNOMED_CT,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationDoseQuantityValue = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_QUANTITY_VALUE,
        Decimal_16_6,
        spark_type=DecimalType(16, 6)
    )
    PrescribedMedicationDoseQuantityValueUnitOfMeasurementDesc = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_QUANTITY_VALUE_DESC,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationDoseRangeLowQuantityValue = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_RANGE_LOW_QUANTITY_VALUE,
        Decimal_16_6,
        spark_type=DecimalType(16, 6)
    )
    PrescribedMedicationDoseRangeLowQuantityValueUnitOfMeasurementDesc = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_RANGE_LOW_QUANTITY_VALUE_DESC,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationDoseRangeHighQuantityValue = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_RANGE_HIGH_QUANTITY_VALUE,
        Decimal_16_6,
        spark_type=DecimalType(16, 6)
    )
    PrescribedMedicationDoseRangeHighQuantityValueUnitOfMeasurementDesc = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_RANGE_HIGH_QUANTITY_VALUE_DESC,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationDoseRepeatFrequencyValue = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_REPEAT_FREQUENCY_VALUE,
        int,
        spark_type=IntegerType()
    )
    PrescribedMedicationDoseRepeatPeriod = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_REPEAT_PERIOD,
        int,
        spark_type=IntegerType()
    )
    PrescribedMedicationDoseRepeatPeriodUnitOfMeasurement_FHIRR4 = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_REPEAT_PERIOD_UOM,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationDoseDayOfWeek_FHIRR4 = RepeatingSubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_DAY_OF_WEEK,
        str
    )
    PrescribedMedicationDoseTimeOfDay = RepeatingSubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_TIME_OF_DAY,
        str
    )
    PrescribedMedicationDoseAssociatedEvent_FHIRR4 = RepeatingSubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_ASSOCIATED_EVENT,
        str
    )
    PrescribedMedicationDoseToBeAdministeredTimestamp = RepeatingSubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_TO_BE_ADMINISTERED_TIMESTAMP,
        datetime
    )
    PrescribedMedicationDoseAdministeredAsNeededBoolean = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_ADMINISTERED_BOOL,
        bool,
        spark_type=BooleanType()
    )
    PrescribedMedicationValidityPeriodStartTimestamp = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_VALIDITY_PERIOD_START_TIMESTAMP,
        datetime,
        spark_type=TimestampType()
    )
    PrescribedMedicationValidityPeriodEndTimestamp = SubmittedAttribute(
        EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_VALIDITY_PERIOD_END_TIMESTAMP,
        datetime,
        spark_type=TimestampType()
    )

    # NESTED TABLES

    PrescribedMedicationAdditionalDosageInstructions = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAP_ADD,
        _EPMAAdditionalModel
    )

    # DERIVS
    SITE_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.SITE_FILTERED,
        str,
        spark_type=StringType()
    )
    SITE_FILTERED_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.SITE_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    SITE_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.SITE_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    SITE_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.SITE_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )
    ROUTE_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.ROUTE_FILTERED,
        str,
        spark_type=StringType()
    )
    ROUTE_FILTERED_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.ROUTE_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    ROUTE_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.ROUTE_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    ROUTE_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.ROUTE_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )
    METHOD_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.METHOD_FILTERED,
        str,
        spark_type=StringType()
    )
    METHOD_FILTERED_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.METHOD_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    METHOD_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.METHOD_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    METHOD_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.METHOD_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )
    DOSE_QUANTITY_UNIT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSE_QUANTITY_UNIT_CHECK,
        str,
        spark_type=StringType()
    )
    DOSE_QUANTITY_UNIT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.DOSE_QUANTITY_UNIT_FILTERED,
        str,
        spark_type=StringType()
    )
    DOSE_QUANTITY_UNIT_FILTERED_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSE_QUANTITY_UNIT_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    DOSERANGE_LOW_UNIT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSERANGE_LOW_UNIT_CHECK,
        str,
        spark_type=StringType()
    )
    DOSERANGE_LOW_UNIT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.DOSERANGE_LOW_UNIT_FILTERED,
        str,
        spark_type=StringType()
    )
    DOSERANGE_LOW_UNIT_FILTERED_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSERANGE_LOW_UNIT_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    DOSERANGE_HIGH_UNIT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSERANGE_HIGH_UNIT_CHECK,
        str,
        spark_type=StringType()
    )
    DOSERANGE_HIGH_UNIT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.DOSERANGE_HIGH_UNIT_FILTERED,
        str,
        spark_type=StringType()
    )
    DOSERANGE_HIGH_UNIT_FILTERED_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.DOSERANGE_HIGH_UNIT_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    # WHEN arn't submitted values. They're derived, but we have to use the RepeatingSubmittedAttribute as it's the only
    # model attribute that will support an Array(string) datatype.
    WHEN_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.WHEN_CHECK,
        List[str]
    )
    WHEN_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.WHEN_FILTERED,
        List[str]
    )
    WHEN_FILTERED_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.WHEN_FILTERED_CHECK,
        List[str]
    )


class _EPMAPrescriptionModel(BaseEPMAPresc):
    """
    epma national presc prescription
    """
    META = SubmittedAttribute(
        CommonFields.META,
        _META,
        spark_type=StructType()
    )  # type: dict
    NHSNumber = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.NHS_NUMBER,
        str,
        spark_type=StringType()
    )
    NHSNumberStatusIndicatorCode = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.NHS_NUMBER_STATUS_INDICATOR_CODE,
        str,
        spark_type=StringType()
    )
    PersonBirthDate = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PERSON_BIRTH_DATE,
        date,
        spark_type=DateType()
    )
    OrganisationSiteIdentifierOfTreatment = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.ORGANISATION_SITE_IDENTIFIER,
        str,
        spark_type=StringType()
    )
    MedicationAdministrationSettingType_Prescribed = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.MEDICATION_ADMINISTRATION_SETTING_TYPE,
        str,
        spark_type=StringType()
    )
    OtherMedicationAdminSettingDesc = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.OTHER_MEDICATION_ADMINISTRATION_SETTING,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationStatusDesc = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_STATUS,
        str,
        spark_type=StringType()
    )
    # PrescribedItemIdentifier (in BaseEPMAPresc Model)
    PrescribedMedicationAuthorisedTimestamp = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_AUTHORISED_TIMESTAMP,
        datetime,
        spark_type=TimestampType()
    )
    PrescribedMedicationGPManagedPostDischBoolean = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_GP_MANAGED_POST_DISCHARGE_BOOL,
        bool,
        spark_type=BooleanType()
    )
    PrescribedMedicationRecordLastUpdatedTimestamp = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_RECORD_LAST_UPDATED,
        datetime,
        spark_type=TimestampType()
    )
    PrescribedMedicationName = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_NAME,
        str,
        spark_type=StringType()
    )
    PrescribedMedication_DmD = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_SNOMED_DMD,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationDoseFormDescription = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_DOSE_FORM_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationDoseForm_SnomedCt = SubmittedAttribute(
        EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_DOSE_FORM_SNOMED_CT,
        str,
        spark_type=StringType()
    )

    # NESTED TABLES

    Header = SubmittedAttribute(
        EPMANationalCollection.EPMAP_HEADER,
        _EPMAHeader
    )
    PrescribedMedicationActiveIngredientSubstance = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAP_CHEM,
        _EPMAChemicalModel
    )
    PrescribedMedicationDosage = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAP_DOSAGE,
        _EPMADosageModel
    )
    PrescribedMedicationTherapeuticIndication = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAP_MEDIND,
        _EPMAMedicationIndicationModel
    )

    # DERIVS

    RP_START_MONTH = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.RP_START_MONTH,
        int
    )
    SOURCE_SYSTEM_TRUST_ODS_CODE = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.SOURCE_SYSTEM_TRUST_ODS_CODE,
        str,
        spark_type=StringType()
    )
    NHS_NUMBER_PDS_CHECK = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.NHS_NUMBER_PDS_CHECK,
        str,
        spark_type=StringType()
    )
    PERSON_ID = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.PERSON_ID,
        str,
        spark_type=StringType()
    )
    DATE_OF_BIRTH = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.DOB,
        date,
        spark_type=DateType()
    )
    TREATMENT_SITE_ODS_CODE_CHECK = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.TREATMENT_SITE_ODS_CODE_CHECK,
        str,
        spark_type=StringType()
    )
    TREATMENT_SITE_ODS_CODE_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.TREATMENT_SITE_ODS_CODE_FILTERED,
        str,
        spark_type=StringType()
    )
    TREATMENT_TRUST_ODS_CODE = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.TREATMENT_TRUST_ODS_CODE,
        str,
        spark_type=StringType()
    )
    AGE = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.AGE,
        int,
        spark_type=IntegerType()
    )
    GENDER = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.GENDER,
        str,
        spark_type=StringType()
    )
    IMD_DECILE = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.IMD_DECILE,
        str,
        spark_type=StringType()
    )
    DEPRIVATION_IMD_VERSION = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.DEPRIVATION_IMD_VERSION,
        str,
        spark_type=StringType()
    )
    LSOA_OF_RESIDENCE = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.LSOA_OF_RESIDENCE,
        str,
        spark_type=StringType()
    )
    LSOA_OF_REGISTRATION = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.LSOA_OF_REGISTRATION,
        str,
        spark_type=StringType()
    )
    LSOA_VERSION = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.LSOA_VERSION,
        str,
        spark_type=StringType()
    )
    CCG_OF_RESIDENCE = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.CCG_OF_RESIDENCE,
        str,
        spark_type=StringType()
    )
    CCG_OF_REGISTRATION = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.CCG_OF_REGISTRATION,
        str,
        spark_type=StringType()
    )
    LA_DISTRICT_OF_RESIDENCE = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.LA_DISTRICT_OF_RESIDENCE,
        str,
        spark_type=StringType()
    )
    LA_DISTRICT_OF_REGISTRATION = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.LA_DISTRICT_OF_REGISTRATION,
        str,
        spark_type=StringType()
    )
    INTEGRATED_CARE_SYSTEM_OF_RESIDENCE = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.INTEGRATED_CARE_SYSTEM_OF_RESIDENCE,
        str,
        spark_type=StringType()
    )
    INTEGRATED_CARE_SYSTEM_OF_REGISTRATION = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.INTEGRATED_CARE_SYSTEM_OF_REGISTRATION,
        str,
        spark_type=StringType()
    )
    REGISTERED_GP_PRACTICE = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.REGISTERED_GP_PRACTICE,
        str,
        spark_type=StringType()
    )
    TRUST_DMD_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.TRUST_DMD_CHECK,
        str,
        spark_type=StringType()
    )
    AUTOMAPPED_DMD = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.AUTOMAPPED_DMD,
        str,
        spark_type=StringType()
    )
    NHS_NUMBER_LEGALLY_RESTRICTED = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.NHS_NUMBER_LEGALLY_RESTRICTED,
        str,
        spark_type=StringType()
    )
    AUTOMAPPED_DMD_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.AUTOMAPPED_DMD_CHECK,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.PRIMARY_DMD,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.PRIMARY_DMD_CHECK,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD_NAME = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.PRIMARY_DMD_NAME,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD_LEVEL = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.PRIMARY_DMD_LEVEL,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD_SOURCE = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.PRIMARY_DMD_SOURCE,
        str,
        spark_type=StringType()
    )
    VTM = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.VTM,
        str,
        spark_type=StringType()
    )
    VTM_TERM_TEXT = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.VTM_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    VMP = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.VMP,
        str,
        spark_type=StringType()
    )
    VMP_TERM_TEXT = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.VMP_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    AMP = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.AMP,
        str,
        spark_type=StringType()
    )
    AMP_TERM_TEXT = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.AMP_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    VMPP = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.VMPP,
        str,
        spark_type=StringType()
    )
    VMPP_TERM_TEXT = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.VMPP_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    AMPP = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.AMPP,
        str,
        spark_type=StringType()
    )
    AMPP_TERM_TEXT = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.AMPP_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    FORM_FILTERED = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.FORM_FILTERED,
        str,
        spark_type=StringType()
    )
    FORM_FILTERED_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.FORM_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    FORM_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.FORM_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    FORM_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalPrescriptionDerivedColumns.FORM_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )
    MPS_CONFIDENCE = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.MPS_CONFIDENCE,
        _MPSConfidenceScores
    )
    PATIENT_POSTCODE = AssignableAttribute(
        EPMANationalPrescriptionDerivedColumns.PATIENT_POSTCODE,
        str,
        spark_type=StringType()
    )


_EPMAPrescriptionModel.Root = ModelAttribute('root', _EPMAPrescriptionModel)
_EPMAPrescriptionModel.Parent = ModelAttribute('parent',
                                               _EPMAPrescriptionModel)

_EPMAAdditionalModel.Root = ModelAttribute('root', _EPMAPrescriptionModel)
_EPMAAdditionalModel.Parent = ModelAttribute('parent', _EPMADosageModel)

_EPMAChemicalModel.Root = ModelAttribute('root', _EPMAPrescriptionModel)
_EPMAChemicalModel.Parent = ModelAttribute('parent', _EPMAPrescriptionModel)

_EPMADosageModel.Root = ModelAttribute('root', _EPMAPrescriptionModel)
_EPMADosageModel.Parent = ModelAttribute('parent', _EPMAPrescriptionModel)

_EPMAMedicationIndicationModel.Root = ModelAttribute('root',
                                                     _EPMAPrescriptionModel)
_EPMAMedicationIndicationModel.Parent = ModelAttribute('parent',
                                                       _EPMAPrescriptionModel)

_EPMAHeader.Root = ModelAttribute('root', _EPMAPrescriptionModel)
_EPMAHeader.Parent = ModelAttribute('parent', _EPMAPrescriptionModel)
