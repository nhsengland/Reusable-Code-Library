from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from dsp.datasets.common import PDSCohortFields
from dsp.shared.pds.pds_discovery import set_pds_source, get_pds_data
from dsp.shared.pds.pds_loader import PDS_SCHEMA
import pytest

@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


unmatched_nhs = [('20190304120900', 10, '9464886845', 19750903, 19990824, {}, '{}', 'transfer_id_a')]


def test_pds_discovery(spark: SparkSession):
    df = spark.createDataFrame(unmatched_nhs, PDS_SCHEMA)
    set_pds_source(df)
    pds_data = get_pds_data(spark)

    assert pds_data.collect()[0][PDSCohortFields.ACTIVITY_DATE] == '20190304120900'
    assert pds_data.collect()[0][PDSCohortFields.SERIAL_CHANGE_NUMBER] == 10
    assert pds_data.collect()[0][PDSCohortFields.NHS_NUMBER] == '9464886845'
    assert pds_data.collect()[0][PDSCohortFields.DATE_OF_BIRTH] == 19750903
    assert pds_data.collect()[0][PDSCohortFields.DATE_OF_DEATH] == 19990824
    assert pds_data.collect()[0][PDSCohortFields.RECORD] == '{}'
    assert pds_data.collect()[0][PDSCohortFields.PARSED_RECORD] == Row(NHS_NUMBER=None, replaced_by=None,
                                                                       emailAddress=None, mobilePhone=None,
                                                                       telephone=None, dob=None, dod=None,
                                                                       date=None, sensitive=None, death_status=None,
                                                                       gp_codes=None, address_history=None,
                                                                       name_history=None, gender_history=None,
                                                                       confidentiality=None, migrant_data=None,
                                                                       mpsid_details=None)
    assert pds_data.collect()[0][PDSCohortFields.TRANSFER_ID] == 'transfer_id_a'
