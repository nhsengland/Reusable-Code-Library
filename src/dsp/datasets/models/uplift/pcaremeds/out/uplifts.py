from typing import Tuple, Type

from pyspark.sql import Row, DataFrame
from pyspark.sql.types import StructType, DecimalType, StructField, StringType
from pyspark.sql.functions import col, lit, when, struct
from decimal import Decimal

from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.models.pcaremeds import PrimaryCareMedicineModel
from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.pcaremeds.out import version_2 as PCAREMEDS_V2
from dsp.datasets.models.uplift.pcaremeds.out import version_3 as PCAREMEDS_V3
from dsp.datasets.models.uplift.pcaremeds.out import version_4 as PCAREMEDS_V4

from dsp.common.structured_model import DSPStructuredModel
from dsp.udfs.organisation import postcode_from_org_code
from dsp.udfs.postcodes import ccg_at, unitary_authority_at


def getageband(age: int) -> str:
    if age is None or age > 129 or age < 0:
        return None
    ageband_list = [(lowLim, lowLim + 4, f"{lowLim}-{lowLim + 4}") for lowLim in range(0, 130, 5)]
    for LL, UL, AB in ageband_list:
        if LL <= int(age) <= UL:
            return AB
        else:
            continue


def getitemcount(highvolvaccineindicator: str, dispensedpharmacytype: int, paidformulation: str,
                 paidquantity: float) -> float:
    if all([highvolvaccineindicator == 'Y', dispensedpharmacytype == 7, paidformulation == '0020']):
        return Decimal(paidquantity) / 3
    elif all([highvolvaccineindicator == 'Y', not (dispensedpharmacytype == 7 and paidformulation == '0020')]):
        return Decimal(paidquantity)
    else:
        return Decimal(1)


def getpaidindicator(prescribedbnfcode: str, paidquantity: float, itemactualcost: float, paiddissallowedindicator: str,
                     notdispensedindicator: str, privateprescriptionindicator: int, outofhoursindicator: int):
    if all([prescribedbnfcode[0:2] in ('19,21'), float(paidquantity) == 0]):
        return 'N'
    elif all([not prescribedbnfcode[0:2] in ('19,21'), float(itemactualcost) == 0]):
        return 'N'
    elif not all([paiddissallowedindicator == 'N', notdispensedindicator == 'N',
                  int(privateprescriptionindicator) == 0, int(outofhoursindicator) == 0]):
        return 'N'
    else:
        return 'Y'


def non_pds_derivations_uplift(
        df: DataFrame,
        version_from: int,
        version_to: int,
        target_schema: StructType
) -> Tuple[int, DataFrame]:
    def uplift_row(row: Row) -> Row:
        row_dict = row.asDict(recursive=True)
        assert row_dict[CommonFields.META][CommonFields.RECORD_VERSION] == version_from
        row_dict['AgeBands'] = getageband(row_dict.get('PatientAge'))
        row_dict['PaidIndicator'] = getpaidindicator(row_dict.get('PrescribedBNFCode'), row_dict.get('PaidQuantity'),
                                                     row_dict.get('ItemActualCost'),
                                                     row_dict.get('PaidDissallowedIndicator'),
                                                     row_dict.get('NotDispensedIndicator'),
                                                     row_dict.get('PrivatePrescriptionIndicator'),
                                                     row_dict.get('OutOfHoursIndicator'))
        row_dict['ItemCount'] = getitemcount(row_dict.get('HighVolVaccineIndicator'),
                                             row_dict.get('DispensedPharmacyType'),
                                             row_dict.get('PaidFormulation'), row_dict.get('PaidQuantity'))

        row_dict[CommonFields.META][CommonFields.RECORD_VERSION] = version_to
        return Row(**row_dict)

    uplifted_rdd = df.rdd.map(uplift_row)
    df = uplifted_rdd.toDF(target_schema)

    return version_to, df


def add_patient_dscro_derivations(df: DataFrame,
                                  version_from: int,
                                  version_to: int,
                                  target_schema: StructType) -> Tuple[int, DataFrame]:
    _new_deriv_col_names = ['PatientCCG', 'PatientLA', 'PatientGPCCG', 'PatientGPLA']

    def _uplift_row(rw: Row) -> Row:
        rw_dict = rw.asDict(recursive=True)
        assert rw_dict[CommonFields.META][CommonFields.RECORD_VERSION] == version_from
        rw_dict['PatientCCG'] = ccg_at(rw_dict.get('MPSPostcode'), rw_dict.get('ProcessingPeriodDate'))
        rw_dict['PatientLA'] = unitary_authority_at(rw_dict.get('MPSPostcode'), rw_dict.get('ProcessingPeriodDate'))
        rw_dict['PatientGPCCG'] = ccg_at(
            postcode_from_org_code(rw_dict.get('PatientGPODS'), rw_dict.get('ProcessingPeriodDate')),
            rw_dict.get('ProcessingPeriodDate'))
        rw_dict['PatientGPLA'] = unitary_authority_at(
            postcode_from_org_code(rw_dict.get('PatientGPODS'), rw_dict.get('ProcessingPeriodDate')),
            rw_dict.get('ProcessingPeriodDate'))

        rw_dict[CommonFields.META][CommonFields.RECORD_VERSION] = version_to

        return Row(**rw_dict)

    pcm_rdd = df.rdd

    return_df = pcm_rdd.map(_uplift_row).toDF(target_schema)

    return version_to, return_df


UPLIFTS = {
    1: lambda df: non_pds_derivations_uplift(
        df, 1, 2, PCAREMEDS_V2.schema
    ),

    2: lambda df: simple_uplift(df, 2, 3, PCAREMEDS_V3.schema,
                                add_if_missing_fields=['Person_ID', 'PatientGPODS', 'MPSDateOfBirth', 'MPSGender',
                                                       'MPSPostcode', 'PatientLSOA', 'PatientGPLSOA']),

    3: lambda df: add_patient_dscro_derivations(df, 3, 4, PCAREMEDS_V4.schema)
}
