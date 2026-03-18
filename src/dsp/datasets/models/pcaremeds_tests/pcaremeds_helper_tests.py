import copy
from datetime import datetime, date

import pytest

from dsp.dam import current_record_version
from dsp.shared.constants import DS
from decimal import Decimal
from dsp.datasets.models.pcaremeds import PrimaryCareMedicineModel

meta_dict = {
    'DATASET_VERSION': '1',
    'EVENT_ID': '1:1',
    'EVENT_RECEIVED_TS': datetime(2020, 5, 1),
    'RECORD_INDEX': 0,
    'RECORD_VERSION': current_record_version(DS.PCAREMEDS),
}

pcaremeds_dict = {
    'BSAPrescriptionID': Decimal(3471),
    'ItemID': 6998,
    'EPSPrescriptionID': 'EPSPrescriptionID1',
    'PrescriberType': 34,
    'PrescriberID': 'PrescriberID1',
    'CostCentreType': 54,
    'CostCentreODSCode': 'CostCentreODSCode1',
    'CostCentreSubType': '04',
    'DispensedPharmacyType': 22,
    'DispensedPharmacyODSCode': 'DispensedPharmacyODSCode1',
    'PrescribedCountryCode': 7,
    'DispensedCountryCode': 5,
    'ProcessedPeriod': 202003,
    'ChargeStatus': 'O',
    'ExemptionCode': 'L',
    'NHSNumber': '9134298541',
    'PatientDoB': date(1950, 2, 12),
    'PatientAge': 70,
    'PatientGender': 2,
    'ItemActualCost': Decimal(387.199),
    'ItemNIC': Decimal(8190),
    'PrescribeddmdCode': 3419,
    'PrescribedBNFCode': 'PrescribedBNFCode1',
    'PrescribedBNFName': 'PrescribedBNFName1',
    'PrescribedFormulation': '0187',
    'PrescribedSupplierName': 'PrescribedSupplierName1',
    'PrescribedMedicineStrength': 'PrescribedMedicineStrength1',
    'PrescribedQuantity': Decimal(686.924),
    'PaiddmdCode': 4872,
    'PaidBNFCode': 'PaidBNFCode1',
    'PaidBNFName': 'PaidBNFName1',
    'PaidFormulation': '0149',
    'PaidSupplierName': 'PaidSupplierName1',
    'PaidDrugStrength': 'PaidDrugStrength1',
    'PaidQuantity': Decimal(789.213),
    'PaidPADMIndicator': 'PaidPADMIndicator1',
    'PaidCDIndicator': '-',
    'PaidACBSIndicator': '-',
    'PaidFlavourIndicator': '0039',
    'PaidSpecContIndicator': '-',
    'NotDispensedIndicator': 'Y',
    'HighVolVaccineIndicator': 'N',
    'PaidDissallowedIndicator': 'N',
    'PaidDisallowedReason': 13,
    'OutOfHoursIndicator': 0,
    'PrivatePrescriptionIndicator': 0,
    'EPSPrescriptionIndicator': 1004,
    "MPSConfidence": None,
    "MPSDateOfBirth": None,
    "MPSEmailAddress": None,
    "MPSFirstName": None,
    "PatientGPODS": None,
    "MPSGender": None,
    "MPSLastName": None,
    "MPSMobileNumber": None,
    "MPSPostcode": None,
    "Person_ID": None,

    'META': meta_dict
}


def pcaremeds_test_data(include_derivations: bool = False, derivations_to_exclude: list = []):
    if include_derivations:
        data = {field: val for field, val in PrimaryCareMedicineModel(pcaremeds_dict).as_dict().items() if field not in derivations_to_exclude}
    else:
        data = pcaremeds_dict
    return copy.deepcopy(data)
