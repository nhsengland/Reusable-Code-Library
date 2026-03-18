from copy import deepcopy
from typing import Dict
from typing import List

import pytest
from pyspark.sql import SparkSession

from decimal import Decimal
from dsp.common.spark_helpers import schema_fill_values
from dsp.datasets.pcaremeds.enrichments.enrichments import enrich_pcaremeds_with_pds, mps_request
from dsp.datasets.fields.mps.request import Fields as MpsRequestFields
from dsp.datasets.models.pcaremeds import PrimaryCareMedicineModel
# noinspection PyUnresolvedReferences
from dsp.datasets.models.pcaremeds_tests.pcaremeds_helper_tests import \
    pcaremeds_test_data  # pylint: disable=unused-import
from dsp.integration.mps.mps_schema import mps_schema


def test_enrich_pcaremeds_with_pds(spark: SparkSession):
    # all fields populated
    pcaremeds_data = pcaremeds_test_data()
    pcaremeds_data['META']['EVENT_ID'] = '1:1234'
    pcaremeds_data['BSAPrescriptionID'] = Decimal('1')
    pcaremeds_match = PrimaryCareMedicineModel(pcaremeds_data)

    # no matching record in PDS/MPS response, no-op
    pcaremeds_data_no_match = pcaremeds_test_data()
    pcaremeds_data_no_match['META']['EVENT_ID'] = '2:1234'
    pcaremeds_data_no_match['NHSNumber'] = '9691547570'
    pcaremeds_data_no_match['BSAPrescriptionID'] = Decimal('2')
    pcaremeds_no_match = PrimaryCareMedicineModel(pcaremeds_data_no_match)

    df_pcaremeds = spark.createDataFrame([
        pcaremeds_match.as_row(force_derivation=False),
        pcaremeds_no_match.as_row(force_derivation=False)
    ], PrimaryCareMedicineModel.get_struct(include_derived_attr=False))

    nhs_no1 = '9134298541'
    gp_code1 = 'B82057'
    postcode1 = 'LS1 2AB'

    pds_mps_data = [
        schema_fill_values(
            mps_schema,
            UNIQUE_REFERENCE='1:1234',  # referral_all_populated
            MATCH=True,
            DATE_OF_BIRTH=19501202,
            DATE_OF_DEATH=20180101,
            GP_PRACTICE_CODE=gp_code1,
            GENDER='2',
            ERROR_SUCCESS_CODE='00',
            MATCHED_ALGORITHM_INDICATOR=1,
            MATCHED_NHS_NO=nhs_no1,
            POSTCODE=postcode1,
            MATCHED_CONFIDENCE_PERCENTAGE=Decimal('100.0'),

        )
    ]
    df_pds_mps = spark.createDataFrame(pds_mps_data, mps_schema)

    df_merged = enrich_pcaremeds_with_pds(1, df_pcaremeds, df_pds_mps)
    enriched_pcaremeds_records = df_merged.collect()
    pcaremeds_models_by_BSAPrescriptionID = {
        pcaremeds_BSAPrescriptionID: [pcaremeds_model for pcaremeds_model in enriched_pcaremeds_records
                                      if pcaremeds_model.BSAPrescriptionID == pcaremeds_BSAPrescriptionID]
        for pcaremeds_BSAPrescriptionID in
        {pcaremeds_record.BSAPrescriptionID for pcaremeds_record in enriched_pcaremeds_records}
    }  # type: Dict[int, List[PrimaryCareMedicineModel]]

    # all 'enrichable' fields filled - none enriched
    for pcaremeds_all_populated_enriched in pcaremeds_models_by_BSAPrescriptionID[Decimal('1')]:
        assert pcaremeds_all_populated_enriched.Person_ID == nhs_no1
        assert pcaremeds_all_populated_enriched.PatientGPODS == gp_code1
        assert pcaremeds_all_populated_enriched.MPSPostcode == postcode1

    # no match on EVENT_ID / UNIQUE_REFERENCE - nothing enriched
    for pcaremeds_no_match_enriched in pcaremeds_models_by_BSAPrescriptionID[Decimal('2')]:
        assert pcaremeds_no_match_enriched.Person_ID is None


@pytest.mark.parametrize("processedperiod, expecteddate", [
    ('202003', 20200301),
    ('202004', 20200401),
    ('202005', 20200501),
])
def test_mps_request_date_mapping(spark: SparkSession, processedperiod: str, expecteddate: str):
    pcaremeds_data = pcaremeds_test_data()
    pcaremeds_data['META']['EVENT_ID'] = '1:1234'
    pcaremeds_data['ProcessedPeriod'] = processedperiod

    df = spark.createDataFrame([
        PrimaryCareMedicineModel(pcaremeds_data).as_row(force_derivation=False),
    ], PrimaryCareMedicineModel.get_struct(include_derived_attr=False))

    mps_df = mps_request(df)

    rows = mps_df.collect()
    assert len(rows) == 1
    row = rows[0]

    assert row[MpsRequestFields.AS_AT_DATE] == expecteddate
