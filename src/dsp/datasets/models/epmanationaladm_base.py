from dsp.datasets.common import Fields as CommonFields
from dsp.common.structured_model import (
    AssignableAttribute,
    BaseTableModel,
    RepeatingSubmittedAttribute,
    SubmittedAttribute,
    DerivedAttributePlaceholder,
    _META,
    Decimal_16_6,
    _MPSConfidenceScores
)
from dsp.datasets.epma_national.constants import (
    EPMANationalCollection,
    EPMANationalAdministrationHeaderColumns,
    EPMANationalAdministrationMedicationColumns,
    EPMANationalAdministrationMedicationActiveIngredientSubstanceColumns,
    EPMANationalAdministrationDerivedColumns
)
from dsp.common.model_accessor import ModelAttribute
from pyspark.sql.types import (
    StructType,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType,
    DateType,
    BooleanType
)
from datetime import date, datetime

__all__ = [
    '_EPMAHeaderModel',
    '_EPMAAdministrationModel',
    '_EPMAChemicalModel'
]


class BaseEPMAAdmin(BaseTableModel):
    OrganisationSiteIdentifierOfTreatment = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.ORGANISATION_SITE_IDENTIFIER_TREATMENT,
        str,
        spark_type=StringType()
    )
    MedicationAdminIdentifier = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_IDENTIFIER,
        str,
        spark_type=StringType()
    )
    RowNumber = AssignableAttribute('RowNumber', int, spark_type=IntegerType())


class _EPMAHeaderModel(BaseTableModel):
    """
    epmaadmin_header
    """
    OrganisationSiteIdentifierSystemLocation = SubmittedAttribute(
        EPMANationalAdministrationHeaderColumns.ORGANISATION_SITE_IDENTIFIER_SYSTEM,
        str,
        spark_type=StringType()
    )
    DataSetCreatedTimestamp = SubmittedAttribute(
        EPMANationalAdministrationHeaderColumns.DATASET_CREATED,
        datetime,
        spark_type=TimestampType()
    )
    PrimSystemInUse = SubmittedAttribute(
        EPMANationalAdministrationHeaderColumns.PRIMARY_DATA_COLLECTION_SYSTEM_IN_USE,
        str,
        spark_type=StringType()
    )
    ReportingPeriodStartDate = SubmittedAttribute(
        EPMANationalAdministrationHeaderColumns.REPORTING_PERIOD_START_DATE,
        date,
        spark_type=DateType()
    )
    ReportingPeriodEndDate = SubmittedAttribute(
        EPMANationalAdministrationHeaderColumns.REPORTING_PERIOD_END_DATE,
        date,
        spark_type=DateType()
    )


class _EPMAChemicalModel(BaseEPMAAdmin):
    # OrganisationSiteIdentifierOfTreatment inherited from BaseEPMAAdmin
    # MedicationAdminIdentifier inherited from BaseEPMAAdmin
    MedicationAdministeredActiveIngredientSubstanceDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationActiveIngredientSubstanceColumns.MEDICATION_ADMINISTERED_ACTIVE_INGREDIENT_SUBSTANCE_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    MedicationAdministeredActiveIngredientSubstanceStrengthDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationActiveIngredientSubstanceColumns.MEDICATION_ADMINISTERED_ACTIVE_INGREDIENT_SUBSTANCE_STRENGTH_DESCRIPTION,
        str,
        spark_type=StringType()
    )


class _EPMAAdministrationModel(BaseEPMAAdmin):

    # Submitted Columns

    META = SubmittedAttribute(
        CommonFields.META,
        _META,
        spark_type=StructType()
    )
    NHSNumber = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.NHS_NUMBER,
        str,
        spark_type=StringType()
    )
    NHSNumberStatusIndicatorCode = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.NHS_NUMBER_STATUS_INDICATOR_CODE,
        str,
        spark_type=StringType()
    )
    PersonBirthDate = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.PERSON_BIRTH_DATE,
        date,
        spark_type=DateType()
    )
    # OrganisationSiteIdentifierOfTreatment inherited from BaseEPMAAdmin
    MedicationAdministrationSettingType_Actual = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_SETTING_TYPE_ACTUAL,
        str,
        spark_type=StringType()
    )
    OtherMedicationAdminSettingDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.OTHER_MEDICATION_ADMINISTRATION_SETTING_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    MedicationAdminStatusDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_STATUS,
        str,
        spark_type=StringType()
    )
    # MedicationAdminIdentifier inherited from BaseEPMAAdmin
    PrescribedItemIdentifier = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.PRESCRIBED_ITEM_IDENTIFIER,
        str,
        spark_type=StringType()
    )
    PrescribedMedicationDoseToBeAdministeredTimestamp = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.PRESCRIBED_MEDICATION_DOSE_TO_BE_ADMINISTERED_TIMESTAMP,
        datetime,
        spark_type=TimestampType()
    )
    CodedProcedureTimeStamp_MedicationAdmin = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.CODED_PROCEDURE_TIMESTAMP_MEDICATION_ADMINISTRATION,
        datetime,
        spark_type=TimestampType()
    )
    MedicationAdministrationRecordedTimeStamp = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_RECORDED_TIMESTAMP,
        datetime,
        spark_type=TimestampType()
    )
    PrescribedMedicationDoseNotAdministeredBoolean = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.PRESCRIBED_MEDICATION_DOSE_NOT_ADMINISTERED_BOOLEAN,
        bool,
        spark_type=BooleanType()
    )
    PrescribedMedicationDoseNotAdministeredReasonDescription = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.PRESCRIBED_MEDICATION_DOSE_NOT_ADMINISTERED_REASON_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    MedicationAdministrationRecordLastUpdatedTimeStamp = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_RECORD_LAST_UPDATED_TIMESTAMP,
        datetime,
        spark_type=TimestampType()
    )
    MedicationAdministeredName = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTERED_NAME,
        str,
        spark_type=StringType()
    )
    MedicationAdministered_DmD = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTERED_DMD,
        str,
        spark_type=StringType()
    )
    MedicationAdministrationDoseFormDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_DOSE_FORM_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    MedicationAdministrationDoseForm_SnomedCt = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_DOSE_FORM_SNOMED_CT,
        str,
        spark_type=StringType()
    )
    MedicationAdministrationDoseActualDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_DOSE_ACTUAL_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    BodySiteOfAdministrationActualDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.BODY_SITE_OF_ADMINISTRATION_ACTUAL_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    BodySiteOfAdministration_SnomedCt = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.BODY_SITE_OF_ADMINISTRATION_ACTUAL_SNOMED_CT,
        str,
        spark_type=StringType()
    )
    RouteOfAdministrationActualDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.ROUTE_OF_ADMINISTRATION_ACTUAL_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    RouteOfAdministration_SnomedCt = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.ROUTE_OF_ADMINISTRATION_ACTUAL_SNOMED_CT,
        str,
        spark_type=StringType()
    )
    MethodOfAdministrationActualDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.METHOD_OF_ADMINISTRATION_ACTUAL_DESCRIPTION,
        str,
        spark_type=StringType()
    )
    MethodOfAdministrationActual_SnomedCt = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.METHOD_OF_ADMINISTRATION_ACTUAL_SNOMED_CT,
        str,
        spark_type=StringType()
    )
    MedicationAdministrationDoseQuantityValue = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_DOSE_QUANTITY_VALUE,
        Decimal_16_6,
        spark_type=DecimalType(16, 6)
    )
    MedicationAdministrationDoseQuantityValueUnitOfMeasurementDesc = SubmittedAttribute(
        EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_DOSE_QUANTITY_VALUE_UNIT_OF_MEASUREMENT_DESCRIPTION,
        str,
        spark_type=StringType()
    )

    # Nested Columns

    Header = SubmittedAttribute(
        EPMANationalCollection.EPMAA_HEADER,
        _EPMAHeaderModel
    )
    MedicationAdministrationActiveIngredientSubstance = RepeatingSubmittedAttribute(
        EPMANationalCollection.EPMAA_ACTIVE_ING,
        _EPMAChemicalModel
    )

    # Derived Columns
    RP_START_MONTH = DerivedAttributePlaceholder(EPMANationalAdministrationDerivedColumns.RP_START_MONTH,
                                                 int)

    SOURCE_SYSTEM_TRUST_ODS_CODE = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.SOURCE_SYSTEM_TRUST_ODS_CODE,
        str,
        spark_type=StringType()
    )
    NHS_NUMBER_PDS_CHECK = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.NHS_NUMBER_PDS_CHECK,
        str,
        spark_type=StringType()
    )
    PERSON_ID = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.PERSON_ID,
        str,
        spark_type=StringType()
    )
    TREATMENT_SITE_ODS_CODE_CHECK = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.TREATMENT_SITE_ODS_CODE_CHECK,
        str,
        spark_type=StringType()
    )
    TREATMENT_SITE_ODS_CODE_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.TREATMENT_SITE_ODS_CODE_FILTERED,
        str,
        spark_type=StringType()
    )
    TREATMENT_TRUST_ODS_CODE = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.TREATMENT_TRUST_ODS_CODE,
        str,
        spark_type=StringType()
    )
    AGE = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.AGE,
        int,
        spark_type=IntegerType()
    )
    GENDER = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.GENDER,
        str,
        spark_type=StringType()
    )
    IMD_DECILE = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.IMD_DECILE,
        str,
        spark_type=StringType()
    )
    DEPRIVATION_IMD_VERSION = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.DEPRIVATION_IMD_VERSION,
        str,
        spark_type=StringType()
    )
    CCG_OF_RESIDENCE = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.CCG_OF_RESIDENCE,
        str,
        spark_type=StringType()
    )
    CCG_OF_REGISTRATION = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.CCG_OF_REGISTRATION,
        str,
        spark_type=StringType()
    )
    LA_DISTRICT_OF_RESIDENCE = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.LA_DISTRICT_OF_RESIDENCE,
        str,
        spark_type=StringType()
    )
    LA_DISTRICT_OF_REGISTRATION = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.LA_DISTRICT_OF_REGISTRATION,
        str,
        spark_type=StringType()
    )
    INTEGRATED_CARE_SYSTEM_OF_RESIDENCE = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.INTEGRATED_CARE_SYSTEM_OF_RESIDENCE,
        str,
        spark_type=StringType()
    )
    INTEGRATED_CARE_SYSTEM_OF_REGISTRATION = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.INTEGRATED_CARE_SYSTEM_OF_REGISTRATION,
        str,
        spark_type=StringType()
    )
    LSOA_OF_REGISTRATION = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.LSOA_OF_REGISTRATION,
        str,
        spark_type=StringType()
    )
    LSOA_OF_RESIDENCE = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.LSOA_OF_RESIDENCE,
        str,
        spark_type=StringType()
    )
    LSOA_VERSION = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.LSOA_VERSION,
        str,
        spark_type=StringType()
    )
    REGISTERED_GP_PRACTICE = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.REGISTERED_GP_PRACTICE,
        str,
        spark_type=StringType()
    )
    TRUST_DMD_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.TRUST_DMD_CHECK,
        str,
        spark_type=StringType()
    )
    AUTOMAPPED_DMD = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.AUTOMAPPED_DMD,
        str,
        spark_type=StringType()
    )
    NHS_NUMBER_LEGALLY_RESTRICTED = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.NHS_NUMBER_LEGALLY_RESTRICTED,
        str,
        spark_type=StringType())
    AUTOMAPPED_DMD_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.AUTOMAPPED_DMD_CHECK,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.PRIMARY_DMD,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.PRIMARY_DMD_CHECK,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD_NAME = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.PRIMARY_DMD_NAME,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD_LEVEL = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.PRIMARY_DMD_LEVEL,
        str,
        spark_type=StringType()
    )
    PRIMARY_DMD_SOURCE = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.PRIMARY_DMD_SOURCE,
        str,
        spark_type=StringType()
    )
    VTM = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.VTM,
        str,
        spark_type=StringType()
    )
    VTM_TERM_TEXT = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.VTM_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    VMP = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.VMP,
        str,
        spark_type=StringType()
    )
    VMP_TERM_TEXT = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.VMP_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    AMP = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.AMP,
        str,
        spark_type=StringType()
    )
    AMP_TERM_TEXT = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.AMP_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    VMPP = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.VMPP,
        str,
        spark_type=StringType()
    )
    VMPP_TERM_TEXT = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.VMP_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    AMPP = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.AMPP,
        str,
        spark_type=StringType()
    )
    AMPP_TERM_TEXT = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.AMPP_TERM_TEXT,
        str,
        spark_type=StringType()
    )
    FORM_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.FORM_FILTERED,
        str,
        spark_type=StringType()
    )
    FORM_FILTERED_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.FORM_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    FORM_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.FORM_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    FORM_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.FORM_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )
    SITE_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.SITE_FILTERED,
        str,
        spark_type=StringType()
    )
    SITE_FILTERED_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.SITE_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    SITE_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.SITE_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    SITE_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.SITE_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )
    ROUTE_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.ROUTE_FILTERED,
        str,
        spark_type=StringType()
    )
    ROUTE_FILTERED_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.ROUTE_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    ROUTE_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.ROUTE_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    ROUTE_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.ROUTE_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )
    METHOD_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.METHOD_FILTERED,
        str,
        spark_type=StringType()
    )
    METHOD_FILTERED_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.METHOD_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    METHOD_SNOMED_CT_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.METHOD_SNOMED_CT_CHECK,
        str,
        spark_type=StringType()
    )
    METHOD_SNOMED_CT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.METHOD_SNOMED_CT_FILTERED,
        str,
        spark_type=StringType()
    )
    DOSE_QUANTITY_UNIT_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.DOSE_QUANTITY_UNIT_CHECK,
        str,
        spark_type=StringType()
    )
    DOSE_QUANTITY_UNIT_FILTERED = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.DOSE_QUANTITY_UNIT_FILTERED,
        str,
        spark_type=StringType()
    )
    DOSE_QUANTITY_UNIT_FILTERED_CHECK = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.DOSE_QUANTITY_UNIT_FILTERED_CHECK,
        str,
        spark_type=StringType()
    )
    PATIENT_POSTCODE = AssignableAttribute(
        EPMANationalAdministrationDerivedColumns.PATIENT_POSTCODE,
        str,
        spark_type=StringType()
    )
    MPS_CONFIDENCE = AssignableAttribute(
         EPMANationalAdministrationDerivedColumns.MPS_CONFIDENCE,
         _MPSConfidenceScores
     )
    DATE_OF_BIRTH = DerivedAttributePlaceholder(
        EPMANationalAdministrationDerivedColumns.DATE_OF_BIRTH,
        date,
        spark_type=DateType()
    )


_EPMAHeaderModel.Root = ModelAttribute('root', _EPMAAdministrationModel)
_EPMAHeaderModel.Parent = ModelAttribute('parent', _EPMAAdministrationModel)

_EPMAChemicalModel.Root = ModelAttribute('root', _EPMAAdministrationModel)
_EPMAChemicalModel.Parent = ModelAttribute('parent', _EPMAAdministrationModel)
