import gzip
import os
from pyspark.sql import SparkSession
import pytest
from pyspark.sql.functions import col, lit

from dsp.datasets.common import PDSCohortFields
from dsp.loaders import LoaderRejectionError
from dsp.shared.pds.pds_loader import pds_loader
from dsp.model.pds_record import PDSRecord
from dsp.shared import local_path


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


@pytest.fixture()
def temp_dir() -> str:
    return local_path('testdata/pds_test_data')


def test_csv_loader(spark):
    path = local_path('testdata/pds_test_data/input.csv')
    pds_df = pds_loader(spark, path, '0082dbe8f024dc7054b090992d76f8176b93391e')

    _check_processed_input_csv(pds_df)


def test_csv_loader_gzipped(spark, temp_dir):
    source = local_path('testdata/pds_test_data/input.csv')
    dest = os.path.join(temp_dir, 'input.csv.gz')
    with open(source, 'rb') as reader, gzip.open(dest, 'wb') as writer:
        for line in reader:
            writer.write(line)

    pds_df = pds_loader(spark, dest, '0082dbe8f024dc7054b090992d76f8176b93391e')
    _check_processed_input_csv(pds_df)


def _check_processed_input_csv(pds_df):
    assert pds_df.count() == 151  # the number of deduped roes

    # select one of the records with multiple SCNs and check the correct SCN is used
    selected_df = pds_df.where(
        col(PDSCohortFields.NHS_NUMBER) == lit('5990040121')
    )

    recs = selected_df.collect()
    assert len(recs) == 1

    record = recs[0]

    assert record[PDSCohortFields.SERIAL_CHANGE_NUMBER] == 16
    assert record[PDSCohortFields.TRANSFER_ID] == '0082dbe8f024dc7054b090992d76f8176b93391e'
    assert record["name"]['from'] == 20180521
    assert record["name"]['givenNames'] == ['JANE|PIPEY']
    assert record["name"]['familyName'] == 'BOW'
    assert record["name"]['scn'] == 16

    assert record["address"]['from'] == 20190225
    assert record["address"]['lines'] == ['270', 'Great Horton Road', 'Bradford']
    assert record["address"]['postCode'] == 'BD7 3AD'
    assert record['address']['scn'] == 16

    assert record["gender"]['from'] == 20180521
    assert record["gender"]['gender'] == '2'
    assert record["gender"]['scn'] == 16

    assert record["gp"]['from'] == 20180605
    assert record["gp"]['code'] == 'B82020'
    assert record["gp"]['scn'] == 16

    assert record['sensitive']
    assert record["death_status"] == "2"

    assert record['parsed_record']['mpsid_details'][0]['mpsID'] == 'mps_id_test1'
    assert record['parsed_record']['mpsid_details'][0]['localPatientID'] == 'lpi_test1'
    assert record['parsed_record']['mpsid_details'][0]['scn'] == 69


    pds_record = PDSRecord.from_json(record[PDSCohortFields.RECORD])
    assert len(pds_record.address_history()) == 15
    assert pds_record.sequence_number() == 16
    assert pds_record.given_names_at(20180524) == ['Jane|Pipey']


def test_csv_loader_row_count_mismatch(spark):
    path = local_path('testdata/pds_test_data/wrong_number_of_rows.csv')
    with pytest.raises(LoaderRejectionError) as excinfo:
        pds_loader(spark, path, '4872dbe8f024dc7054b090992d76f8176b93aa1e')
    assert str(excinfo.value) == "The number of rows in the file (1) doesn't match the header (2)."


def test_csv_loader_corrupt_header_line(spark):
    path = local_path('testdata/pds_test_data/corrupt_header_line.csv')
    with pytest.raises(LoaderRejectionError) as excinfo:
        pds_loader(spark, path, '0082dbe8f024dc7054b130992d76f8176b93aa1e')
    assert str(excinfo.value) == 'Header line appears corrupt. Expected DSP, but got ABC'


def test_csv_loader_all_bad_records_missing_mandatory_record_item(spark):
    path = local_path('testdata/pds_test_data/missing_gender_history.csv')

    with pytest.raises(LoaderRejectionError) as excinfo:
        pds_loader(spark, path, '0082dbe8f024dc7014b090992d76f8176b93aa1e')

    assert str(excinfo.value) == 'pds_load returned no valid records .. this seems incorrect'
    # Bad record has been stripped out


def test_csv_loader_one_bad_one_good(spark):
    path = local_path('testdata/pds_test_data/one_good_record.csv')

    df_loaded = pds_loader(spark, path, '0082dbe8f024dc7014b090992d76f8176b93aa1e')

    assert df_loaded.count() == 1


def test_csv_loader_one_duplicate_record(spark):
    path = local_path('testdata/pds_test_data/duplicate_record.csv')

    df_loaded = pds_loader(spark, path, '0082dbe8f024dc7014b090992d76f8176b93aa1e')

    assert df_loaded.count() == 2
