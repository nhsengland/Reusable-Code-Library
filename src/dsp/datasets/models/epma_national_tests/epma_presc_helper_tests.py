import copy
from datetime import datetime, date
from decimal import Decimal

from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.epma_national.constants import (
    EPMANationalCollection,
    EPMANationalPrescriptionHeaderColumns,
    EPMANationalPrescriptionChemicalColumns,
    EPMANationalPrescriptionMedicationIndicationColumns,
    EPMANationalPrescriptionAdditionalColumns,
    EPMANationalPrescriptionDosageColumns,
    EPMANationalPrescriptionPrescriptionColumns
)
from dsp.datasets.models.epmanationalpres import EPMAPrescriptionModel


META_DICT = {
    CommonFields.DATASET_VERSION: '1',
    CommonFields.EVENT_ID: '1:1',
    CommonFields.EVENT_RECEIVED_TS: datetime(2021, 11, 2),
    CommonFields.RECORD_INDEX: 0,
    CommonFields.RECORD_VERSION: '1'
}

EPMA_PRESC_HEADER_DICT = {
    EPMANationalPrescriptionHeaderColumns.DATASET_CREATED: datetime(2022, 2, 11, 15, 59, 26),
    EPMANationalPrescriptionHeaderColumns.ORGANISATION_SITE_IDENTIFIER_SYSTEM: "RX101",
    EPMANationalPrescriptionHeaderColumns.PRIMARY_DATA_COLLECTION_SYSTEM_IN_USE: "TDT Eddie Gen",
    EPMANationalPrescriptionHeaderColumns.REPORTING_PERIOD_END_DATE: date(2022, 2, 28),
    EPMANationalPrescriptionHeaderColumns.REPORTING_PERIOD_START_DATE: date(2022, 2, 1),
}

EPMA_PRESC_CHEM_DICT = [{
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_ITEM_IDENTIFIER: "LOCALTEST01",
    EPMANationalPrescriptionChemicalColumns.PRESCRIBED_MEDICATION_ACTIVE_INGREDIENT_SUBSTANCE_DESC: "ABC",
    EPMANationalPrescriptionChemicalColumns.PRESCRIBED_MEDICATION_ACTIVE_INGREDIENT_SUBTANCE_STRENGTH_DESC: "ABC"
}]

EPMA_PRESC_MI_DICT = [{
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_ITEM_IDENTIFIER: "LOCALTEST01",
    EPMANationalPrescriptionMedicationIndicationColumns.PRESCRIBED_MEDICATION_THERAPEUTIC_INDICATION_DESC: "ABC",
    EPMANationalPrescriptionMedicationIndicationColumns.THERAPEUTIC_INDICATION_SNOMED_CODE: "46884972797471651"
}]

EPMA_PRESC_ADDITIONAL_DICT = [{
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_ITEM_IDENTIFIER: "LOCALTEST01",
    EPMANationalPrescriptionAdditionalColumns.PRESCRIBED_MEDICATION_ADDITIONAL_DOSAGE_INSTRUCTION_DESC: "ABC",
    EPMANationalPrescriptionAdditionalColumns.PRESCRIBED_MEDICATION_ADDITIONAL_DOSAGE_INSTRUCTION_SNOMED_CT: "945952186498693120"
}]

EPMA_PRESC_DOSAGE_DICT = [{
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_ITEM_IDENTIFIER: "LOCALTEST01",
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSAGE_INSTRUCTION_SEQUENCE_NO: 1,
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSAGE_INSTRUCTION_DESC: "ABC",
    EPMANationalPrescriptionDosageColumns.BODY_SITE_OF_ADMINISTRATION_PRESCRIBED_DESC: "ABC",
    EPMANationalPrescriptionDosageColumns.BODY_SITE_OF_ADMINISTRATION_PRESCRIBED_SNOMED_CT: "910840577407870306",
    EPMANationalPrescriptionDosageColumns.ROUTE_OF_ADMINISTRATION_PRESCRIBED_DESC: "ABC",
    EPMANationalPrescriptionDosageColumns.ROUTE_OF_ADMINISTRATION_PRESCRIBED_SNOMED_CT: "910840577407870306",
    EPMANationalPrescriptionDosageColumns.METHOD_OF_ADMINISTRATION_PRESCRIBED_DESC: "ABC",
    EPMANationalPrescriptionDosageColumns.METHOD_OF_ADMINISTRATION_PRESCRIBED_SNOMED_CT: "910840577407870306",
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_QUANTITY_VALUE: Decimal(123.45),
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_QUANTITY_VALUE_DESC: "umol/kg",
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_RANGE_LOW_QUANTITY_VALUE: Decimal(123.45),
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_RANGE_LOW_QUANTITY_VALUE_DESC: "umol/kg",
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_RANGE_HIGH_QUANTITY_VALUE: Decimal(123.45),
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_RANGE_HIGH_QUANTITY_VALUE_DESC: "umol/kg",
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_REPEAT_FREQUENCY_VALUE: 123,
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_REPEAT_PERIOD: 18744619,
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_REPEAT_PERIOD_UOM: "s",
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_DAY_OF_WEEK: ["tue", "wed", "thu", "fri", "sat", "sun", "mon"],
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_TIME_OF_DAY: ["05:53:52", "10:35:36", "23:24:42"],
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_ASSOCIATED_EVENT: ["ICD", "ICD"],
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_TO_BE_ADMINISTERED_TIMESTAMP: [datetime(2022, 7, 22, 12, 00, 00), datetime(2022, 12, 30, 12, 00, 00)],
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_DOSE_ADMINISTERED_BOOL: False,
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_VALIDITY_PERIOD_START_TIMESTAMP: datetime(2022, 1, 30, 12, 00, 00),
    EPMANationalPrescriptionDosageColumns.PRESCRIBED_MEDICATION_VALIDITY_PERIOD_END_TIMESTAMP: datetime(2023, 1, 10, 12, 00, 00),
    EPMANationalCollection.EPMAP_ADD: EPMA_PRESC_ADDITIONAL_DICT
}]

EPMA_PRESC_PRESC_DICT = {
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_ITEM_IDENTIFIER: "LOCALTEST01",
    CommonFields.META: META_DICT,
    EPMANationalPrescriptionPrescriptionColumns.NHS_NUMBER: "5551000737",
    EPMANationalPrescriptionPrescriptionColumns.NHS_NUMBER_STATUS_INDICATOR_CODE: "01",
    EPMANationalPrescriptionPrescriptionColumns.PERSON_BIRTH_DATE: date(2025, 2, 14),
    EPMANationalPrescriptionPrescriptionColumns.ORGANISATION_SITE_IDENTIFIER: "RX101",
    EPMANationalPrescriptionPrescriptionColumns.MEDICATION_ADMINISTRATION_SETTING_TYPE: "2",
    EPMANationalPrescriptionPrescriptionColumns.OTHER_MEDICATION_ADMINISTRATION_SETTING: "ABC",
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_STATUS: "on-hold",
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_ITEM_IDENTIFIER: "LOCALTEST01",
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_AUTHORISED_TIMESTAMP: datetime(2025, 2, 1, 12, 00, 00),
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_GP_MANAGED_POST_DISCHARGE_BOOL: False,
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_RECORD_LAST_UPDATED: datetime(2025, 2, 1, 12, 00, 00),
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_NAME: "Epivir 150mg tablets",
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_SNOMED_DMD: "32234211000001101",
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_DOSE_FORM_DESCRIPTION: "tablet",
    EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_DOSE_FORM_SNOMED_CT: "555688418724578746",
    EPMANationalCollection.EPMAP_HEADER: EPMA_PRESC_HEADER_DICT,
    EPMANationalCollection.EPMAP_CHEM: EPMA_PRESC_CHEM_DICT,
    EPMANationalCollection.EPMAP_DOSAGE: EPMA_PRESC_DOSAGE_DICT,
    EPMANationalCollection.EPMAP_MEDIND: EPMA_PRESC_MI_DICT,
}


def epmanationalpresc_test_data(include_derivations: bool = False,
                                derivations_to_exclude: list = []):

    if include_derivations:
        data = {field: val for field, val in EPMAPrescriptionModel(EPMA_PRESC_PRESC_DICT).as_dict().items() if field not in derivations_to_exclude}
    else:
        data = EPMA_PRESC_PRESC_DICT

    return copy.deepcopy(data)
