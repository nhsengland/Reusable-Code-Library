from uuid import uuid4

import pytest
from pyspark.sql import SparkSession

from src.nhs_reusable_code_library.resuable_codes.shared.dataset_schema_helpers import get_cols, create_view, create_pseudo_sensitive_view
from src.shared.common import concurrent_tasks

@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


def test_get_cols(spark: SparkSession, temp_db):
    table = "CYP001"
    spark.sql(
        f"""CREATE TABLE {temp_db}.{table} (
        Person_ID_Mother STRING, EFFECTIVE_FROM TIMESTAMP , EFFECTIVE_TO TIMESTAMP)""")

    cols = get_cols(spark, temp_db, table, 'EFFECTIVE_TO')
    assert cols == ['EFFECTIVE_FROM', 'Person_ID_Mother']


def test_create_view(spark: SparkSession):
    legacy_schema_name = f'create_view_test_db_{uuid4().hex}'
    spark.sql(f"CREATE DATABASE {legacy_schema_name}")

    table1 = f"cyp001_{uuid4().hex}"
    spark.sql(
        f"""CREATE TABLE {legacy_schema_name}.{table1} (
        Person_ID_Mother STRING, EFFECTIVE_FROM TIMESTAMP , EFFECTIVE_TO TIMESTAMP)""")
    table2 = f"cyp002_{uuid4().hex}"
    spark.sql(
        f"""CREATE TABLE {legacy_schema_name}.{table2} (
        Person_ID_Mother STRING, LPI string , name string)""")

    current_schema_name = f'create_view_test_db_{uuid4().hex}'
    spark.sql(f"CREATE DATABASE {current_schema_name}")

    spark.sql(
        f"""CREATE TABLE {current_schema_name}.{table2} (
        Person_ID_Mother STRING, LPI string , name string)""")

    table3 = f"cyp003_{uuid4().hex}"
    spark.sql(
        f"""CREATE TABLE {current_schema_name}.{table3} (
        SRV STRING, LPI string , name string)""")
    v1to3_tables = [t[1] for t in spark.sql(f'show tables from {legacy_schema_name}').collect() if
                    t[1] in [table1, table2]]
    v4_tables = [t[1] for t in spark.sql(f'show tables from {current_schema_name}').collect() if
                 t[1] in [table2, table3]]
    ignore = ['META', 'RFAs']
    v1to3_cols = {}
    for t in v1to3_tables:
        v1to3_cols[t] = get_cols(spark, legacy_schema_name, t, ignore)

    v4_cols = {}
    for t in v4_tables:
        v4_cols[t] = get_cols(spark, current_schema_name, t, ignore)

    all_tables = set(v1to3_cols.keys()) | set(v4_cols.keys())
    target_db = f'create_view_test_db_{uuid4().hex}'
    spark.sql(f"CREATE DATABASE {target_db}")

    for t in all_tables:
        create_view(spark, t, v1to3_cols, v4_cols, legacy_schema_name, current_schema_name, target_db)

    target_db_views = spark.sql(f"show tables from {target_db}").collect()
    views = [
        spark.sql(f"show create table {target_db}.{row['tableName']}").collect()[0]['createtab_stmt'].replace('\n', ' ')
        for row in target_db_views if row['tableName'] in [table1, table2, table3]]
    expected_view_statements = [
        f'CREATE VIEW `{target_db}`.`{table1}`(EFFECTIVE_FROM, EFFECTIVE_TO, Person_ID_Mother) AS SELECT EFFECTIVE_FROM, EFFECTIVE_TO, Person_ID_Mother FROM {legacy_schema_name}.{table1} ',
        f'CREATE VIEW `{target_db}`.`{table2}`(LPI, Person_ID_Mother, name) AS SELECT LPI, Person_ID_Mother, name FROM {current_schema_name}.{table2}   UNION ALL   SELECT LPI, Person_ID_Mother, name FROM {legacy_schema_name}.{table2} ',
        f'CREATE VIEW `{target_db}`.`{table3}`(LPI, SRV, name) AS SELECT LPI, SRV, name FROM {current_schema_name}.{table3} '
    ]
    assert views == expected_view_statements


@pytest.mark.parametrize("deid_type", ['pseudo', 'sensitive'])
def test_create_pseudo_sensitive_view(spark: SparkSession, pseudo_db_test: str, deid_type: str):
    source_db = f'pseudo_sensitive_views_source_db_{uuid4().hex}'
    spark.sql(f"CREATE DATABASE {source_db}")

    target_db = f'pseudo_sensitive_views_target_db_{uuid4().hex}'
    spark.sql(f"CREATE DATABASE {target_db}")

    domain1_pseudo_fields = ['Person_ID', 'LocalPatientId', 'NHSNumber']

    hash_fields = ['CarePersLocalId', 'ServiceRequestId', 'CareContactId', 'CareActId', 'Unique_CarePersonnelID_Local',
                   'Unique_ServiceRequestID', 'Unique_CareContactID', 'Unique_CareActivityID']

    redact_fields = ['PersonBirthDate', 'Postcode', 'RFAs', 'EFFECTIVE_FROM', 'EFFECTIVE_TO']

    map_hash_fields = {
        'CarePersLocalId': 'CareProfLocalId',
        'Unique_CarePersonnelID_Local': 'CareProfLocalId',
        'Unique_ServiceRequestID': 'UniqServReqID',
        'Unique_CareContactID': 'UniqCareContID',
        'Unique_CareActivityID': 'UniqCareActID'
    }
    clear_fields = ['ADSM', 'ADSM_FirstScore']

    table1 = f"cyp001_{uuid4().hex}"
    spark.sql(
        f"""CREATE TABLE {source_db}.{table1} (
        Person_ID STRING, ADSM STRING, ADSM_FirstScore STRING, ServiceRequestId STRING, Postcode STRING)""")

    table2 = f"cyp002_{uuid4().hex}"
    spark.sql(
        f"""CREATE TABLE {source_db}.{table2} (
        Person_ID STRING, ServiceRequestId STRING, Unique_CareActivityID STRING, PersonBirthDate STRING)""")

    tables = [table1, table2]
    concurrent_tasks(
        (t,
         create_pseudo_sensitive_view,
         [spark, t, domain1_pseudo_fields, hash_fields, map_hash_fields, deid_type, clear_fields, redact_fields,
          source_db, target_db, pseudo_db_test]) for t in tables
    )
    target_db_views = spark.sql(f"show tables from {target_db}").collect()
    actual_views = [
        spark.sql(f"show create table {target_db}.{row['tableName']}").collect()[0]['createtab_stmt'].replace('\n', ' ')
        for row in target_db_views if row['tableName'] in [table1, table2]
    ]
    if deid_type == 'pseudo':
        expected_pseudo_view_statements = \
            [
                f"CREATE VIEW `{target_db}`.`{table1}`(Person_ID, ADSM, ADSM_FirstScore, ServiceRequestId) AS SELECT p0.pseudo AS Person_ID, ADSM, ADSM_FirstScore, sha2(concat(ServiceRequestId, (SELECT salt FROM {pseudo_db_test}.salt WHERE field = 'ServiceRequestId' AND domain IS NULL)), 256) as ServiceRequestId FROM {source_db}.{table1} lhs LEFT JOIN {pseudo_db_test}.pseudo p0 ON p0.domain = 'Domain-1' AND p0.pseudo_type = 'Person_ID' AND lhs.Person_ID = p0.clear ",
                f"CREATE VIEW `{target_db}`.`{table2}`(Person_ID, ServiceRequestId, Unique_CareActivityID) AS SELECT p0.pseudo AS Person_ID, sha2(concat(ServiceRequestId, (SELECT salt FROM {pseudo_db_test}.salt WHERE field = 'ServiceRequestId' AND domain IS NULL)), 256) as ServiceRequestId, sha2(concat(Unique_CareActivityID, (SELECT salt FROM {pseudo_db_test}.salt WHERE field = 'UniqCareActID' AND domain IS NULL)), 256) as Unique_CareActivityID FROM {source_db}.{table2} lhs LEFT JOIN {pseudo_db_test}.pseudo p0 ON p0.domain = 'Domain-1' AND p0.pseudo_type = 'Person_ID' AND lhs.Person_ID = p0.clear "

            ]
        assert actual_views == expected_pseudo_view_statements

    if deid_type == 'sensitive':
        expected_sensitive_view_statement = \
            [
                f"CREATE VIEW `{target_db}`.`{table1}`(Person_ID, ADSM, ADSM_FirstScore, ServiceRequestId) AS SELECT p0.pseudo AS Person_ID, ADSM, ADSM_FirstScore, ServiceRequestId FROM {source_db}.{table1} lhs LEFT JOIN {pseudo_db_test}.pseudo p0 ON p0.domain = 'Domain-1' AND p0.pseudo_type = 'Person_ID' AND lhs.Person_ID = p0.clear ",
                f"CREATE VIEW `{target_db}`.`{table2}`(Person_ID, ServiceRequestId, Unique_CareActivityID) AS SELECT p0.pseudo AS Person_ID, ServiceRequestId, Unique_CareActivityID FROM {source_db}.{table2} lhs LEFT JOIN {pseudo_db_test}.pseudo p0 ON p0.domain = 'Domain-1' AND p0.pseudo_type = 'Person_ID' AND lhs.Person_ID = p0.clear "
            ]
        assert actual_views == expected_sensitive_view_statement
