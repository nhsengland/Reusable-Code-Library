from uuid import uuid4

import pytest
from pyspark.sql import SparkSession

from nhs_reusable_code_library.resuable_codes.dataset_schema_helpers import get_cols, create_view, create_pseudo_sensitive_view
from nhs_reusable_code_library.resuable_codes.shared.common import concurrent_tasks

import os

import sqlite3

#import PRAGMA

from sqlalchemy import create_engine



def test_current():
    print(os.getenv('PYTEST_CURRENT_TEST'))
    

@pytest.fixture(scope='function')
def db():
    file = os.path.join(os.getcwd(), "test.db")
    print(file)
    connection = sqlite3.connect(file)
    print(connection.total_changes)
    connection.execute("CREATE TABLE IF NOT EXISTS test_tbl (id INTEGER, language TEXT, text TEXT)")
    connection.commit()
    yield connection
    connection.close()
    
def test_entry_creation(db):
    query = ("INSERT INTO test_tbl VALUES (?, ?, ?)")
    values = (1, "PyTest", "This is a testing language")    
    db.execute(query, values)

def test_input_entry(db):
    query = ("INSERT INTO test_tbl VALUES (2, 'Python', 'This is a programming language')")
    db.execute(query)

@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


@pytest.fixture(scope='function')
def db_():
    file = os.path.join(os.getcwd(), "test.db")
    print(file)
    connection = sqlite3.connect(file)
    print(connection.total_changes)
    yield connection
    connection.close()


@pytest.fixture(scope='function')
def db_a():
    try:
        with sqlite3.connect("my.db") as conn:
            print(f"Opened SQLite database with version {sqlite3.sqlite_version} successfully.")

    except sqlite3.OperationalError as e:
        print("Failed to open database:", e)
    
    # file = os.path.join(os.getcwd(), "test.db")
    # print(file)
    # connection = sqlite3.connect(file)
    print(conn.total_changes)
    # connection.execute("CREATE TABLE IF NOT EXISTS test_tbl (id INTEGER, language TEXT, text TEXT)")
    # connection.commit()
    yield conn
    conn.close()
    

# try:
#     with sqlite3.connect("my.db") as conn:
#         print(f"Opened SQLite database with version {sqlite3.sqlite_version} successfully.")

# except sqlite3.OperationalError as e:
#     print("Failed to open database:", e)


def test_get_cols(spark: SparkSession):
    table = "CYP001"
    # file = os.path.join(os.getcwd(), "temp_db.db")
    conn = sqlite3.connect("test.db")
    cursor = conn.cursor()
    cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {table} (
        Person_ID_Mother STRING, EFFECTIVE_FROM TIMESTAMP , EFFECTIVE_TO TIMESTAMP)""")

    data = cursor.fetchall()
    cursor_ = cursor.execute(f'select * from  {table}')
    names = [description[0] for description in cursor_.description]
    #cols = get_cols(spark, data, table, 'EFFECTIVE_TO')
    print(names)
    assert 'Person_ID_Mother' in names
    #assert names == ['Person_ID_Mother', 'Person_ID_Mother']



def test_get_cols_(spark: SparkSession):
    table = "CYP001"
    # file = os.path.join(os.getcwd(), "temp_db.db")
    df = spark.read.format("jdbc").options(url='jdbc:sqlite:test.db',
                                       dbtable='CYP001',
                                       driver="org.sqlite.JDBC").load()
    conn = sqlite3.connect("test.db")
    cursor = conn.cursor()
    cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {table} (
        Person_ID_Mother STRING, EFFECTIVE_FROM TIMESTAMP , EFFECTIVE_TO TIMESTAMP)""")

    data = cursor.fetchall()
    cursor_ = cursor.execute(f'select * from  {table}')
    names = [description[0] for description in cursor_.description]
    #cols = get_cols(spark, data, table, 'EFFECTIVE_TO')
    print(names)
    assert 'Person_ID_Mother' in names
    #assert names == ['Person_ID_Mother', 'Person_ID_Mother']




# def test_get_cols_(spark: SparkSession, db_a):
#     table = "CYP002"
#     # file = os.path.join(os.getcwd(), "temp_db.db")
#     #conn = sqlite3.connect("test.db")
#     #cursor = conn.cursor()
#     engine = create_engine('sqlite:///data.sqlite')
#     conn = engine.connect()
#     engine.execute(
#         f"""CREATE TABLE IF NOT EXISTS {table} (
#         Person_ID_Mother STRING, EFFECTIVE_FROM TIMESTAMP , EFFECTIVE_TO TIMESTAMP)""")

#     cols = get_cols(spark, db_a, table, 'EFFECTIVE_TO')
#     assert cols == ['EFFECTIVE_FROM', 'Person_ID_Mother']
    
    

# def test_create_view(spark: SparkSession):
#     legacy_schema_name = f'create_view_test_db_{uuid4().hex}'
#     spark.sql(f"CREATE DATABASE {legacy_schema_name}")

#     table1 = f"cyp001_{uuid4().hex}"
#     spark.sql(
#         f"""CREATE TABLE {legacy_schema_name}.{table1} (
#         Person_ID_Mother STRING, EFFECTIVE_FROM TIMESTAMP , EFFECTIVE_TO TIMESTAMP)""")
#     table2 = f"cyp002_{uuid4().hex}"
#     spark.sql(
#         f"""CREATE TABLE {legacy_schema_name}.{table2} (
#         Person_ID_Mother STRING, LPI string , name string)""")

#     current_schema_name = f'create_view_test_db_{uuid4().hex}'
#     spark.sql(f"CREATE DATABASE {current_schema_name}")

#     spark.sql(
#         f"""CREATE TABLE {current_schema_name}.{table2} (
#         Person_ID_Mother STRING, LPI string , name string)""")

#     table3 = f"cyp003_{uuid4().hex}"
#     spark.sql(
#         f"""CREATE TABLE {current_schema_name}.{table3} (
#         SRV STRING, LPI string , name string)""")
#     v1to3_tables = [t[1] for t in spark.sql(f'show tables from {legacy_schema_name}').collect() if
#                     t[1] in [table1, table2]]
#     v4_tables = [t[1] for t in spark.sql(f'show tables from {current_schema_name}').collect() if
#                  t[1] in [table2, table3]]
#     ignore = ['META', 'RFAs']
#     v1to3_cols = {}
#     for t in v1to3_tables:
#         v1to3_cols[t] = get_cols(spark, legacy_schema_name, t, ignore)

#     v4_cols = {}
#     for t in v4_tables:
#         v4_cols[t] = get_cols(spark, current_schema_name, t, ignore)

#     all_tables = set(v1to3_cols.keys()) | set(v4_cols.keys())
#     target_db = f'create_view_test_db_{uuid4().hex}'
#     spark.sql(f"CREATE DATABASE {target_db}")

#     for t in all_tables:
#         create_view(spark, t, v1to3_cols, v4_cols, legacy_schema_name, current_schema_name, target_db)

#     target_db_views = spark.sql(f"show tables from {target_db}").collect()
#     views = [
#         spark.sql(f"show create table {target_db}.{row['tableName']}").collect()[0]['createtab_stmt'].replace('\n', ' ')
#         for row in target_db_views if row['tableName'] in [table1, table2, table3]]
#     expected_view_statements = [
#         f'CREATE VIEW `{target_db}`.`{table1}`(EFFECTIVE_FROM, EFFECTIVE_TO, Person_ID_Mother) AS SELECT EFFECTIVE_FROM, EFFECTIVE_TO, Person_ID_Mother FROM {legacy_schema_name}.{table1} ',
#         f'CREATE VIEW `{target_db}`.`{table2}`(LPI, Person_ID_Mother, name) AS SELECT LPI, Person_ID_Mother, name FROM {current_schema_name}.{table2}   UNION ALL   SELECT LPI, Person_ID_Mother, name FROM {legacy_schema_name}.{table2} ',
#         f'CREATE VIEW `{target_db}`.`{table3}`(LPI, SRV, name) AS SELECT LPI, SRV, name FROM {current_schema_name}.{table3} '
#     ]
#     assert views == expected_view_statements


# @pytest.mark.parametrize("deid_type", ['pseudo', 'sensitive'])
# def test_create_pseudo_sensitive_view(spark: SparkSession, pseudo_db_test: str, deid_type: str):
#     source_db = f'pseudo_sensitive_views_source_db_{uuid4().hex}'
#     spark.sql(f"CREATE DATABASE {source_db}")

#     target_db = f'pseudo_sensitive_views_target_db_{uuid4().hex}'
#     spark.sql(f"CREATE DATABASE {target_db}")

#     domain1_pseudo_fields = ['Person_ID', 'LocalPatientId', 'NHSNumber']

#     hash_fields = ['CarePersLocalId', 'ServiceRequestId', 'CareContactId', 'CareActId', 'Unique_CarePersonnelID_Local',
#                    'Unique_ServiceRequestID', 'Unique_CareContactID', 'Unique_CareActivityID']

#     redact_fields = ['PersonBirthDate', 'Postcode', 'RFAs', 'EFFECTIVE_FROM', 'EFFECTIVE_TO']

#     map_hash_fields = {
#         'CarePersLocalId': 'CareProfLocalId',
#         'Unique_CarePersonnelID_Local': 'CareProfLocalId',
#         'Unique_ServiceRequestID': 'UniqServReqID',
#         'Unique_CareContactID': 'UniqCareContID',
#         'Unique_CareActivityID': 'UniqCareActID'
#     }
#     clear_fields = ['ADSM', 'ADSM_FirstScore']

#     table1 = f"cyp001_{uuid4().hex}"
#     spark.sql(
#         f"""CREATE TABLE {source_db}.{table1} (
#         Person_ID STRING, ADSM STRING, ADSM_FirstScore STRING, ServiceRequestId STRING, Postcode STRING)""")

#     table2 = f"cyp002_{uuid4().hex}"
#     spark.sql(
#         f"""CREATE TABLE {source_db}.{table2} (
#         Person_ID STRING, ServiceRequestId STRING, Unique_CareActivityID STRING, PersonBirthDate STRING)""")

#     tables = [table1, table2]
#     concurrent_tasks(
#         (t,
#          create_pseudo_sensitive_view,
#          [spark, t, domain1_pseudo_fields, hash_fields, map_hash_fields, deid_type, clear_fields, redact_fields,
#           source_db, target_db, pseudo_db_test]) for t in tables
#     )
#     target_db_views = spark.sql(f"show tables from {target_db}").collect()
#     actual_views = [
#         spark.sql(f"show create table {target_db}.{row['tableName']}").collect()[0]['createtab_stmt'].replace('\n', ' ')
#         for row in target_db_views if row['tableName'] in [table1, table2]
#     ]
#     if deid_type == 'pseudo':
#         expected_pseudo_view_statements = \
#             [
#                 f"CREATE VIEW `{target_db}`.`{table1}`(Person_ID, ADSM, ADSM_FirstScore, ServiceRequestId) AS SELECT p0.pseudo AS Person_ID, ADSM, ADSM_FirstScore, sha2(concat(ServiceRequestId, (SELECT salt FROM {pseudo_db_test}.salt WHERE field = 'ServiceRequestId' AND domain IS NULL)), 256) as ServiceRequestId FROM {source_db}.{table1} lhs LEFT JOIN {pseudo_db_test}.pseudo p0 ON p0.domain = 'Domain-1' AND p0.pseudo_type = 'Person_ID' AND lhs.Person_ID = p0.clear ",
#                 f"CREATE VIEW `{target_db}`.`{table2}`(Person_ID, ServiceRequestId, Unique_CareActivityID) AS SELECT p0.pseudo AS Person_ID, sha2(concat(ServiceRequestId, (SELECT salt FROM {pseudo_db_test}.salt WHERE field = 'ServiceRequestId' AND domain IS NULL)), 256) as ServiceRequestId, sha2(concat(Unique_CareActivityID, (SELECT salt FROM {pseudo_db_test}.salt WHERE field = 'UniqCareActID' AND domain IS NULL)), 256) as Unique_CareActivityID FROM {source_db}.{table2} lhs LEFT JOIN {pseudo_db_test}.pseudo p0 ON p0.domain = 'Domain-1' AND p0.pseudo_type = 'Person_ID' AND lhs.Person_ID = p0.clear "

#             ]
#         assert actual_views == expected_pseudo_view_statements

#     if deid_type == 'sensitive':
#         expected_sensitive_view_statement = \
#             [
#                 f"CREATE VIEW `{target_db}`.`{table1}`(Person_ID, ADSM, ADSM_FirstScore, ServiceRequestId) AS SELECT p0.pseudo AS Person_ID, ADSM, ADSM_FirstScore, ServiceRequestId FROM {source_db}.{table1} lhs LEFT JOIN {pseudo_db_test}.pseudo p0 ON p0.domain = 'Domain-1' AND p0.pseudo_type = 'Person_ID' AND lhs.Person_ID = p0.clear ",
#                 f"CREATE VIEW `{target_db}`.`{table2}`(Person_ID, ServiceRequestId, Unique_CareActivityID) AS SELECT p0.pseudo AS Person_ID, ServiceRequestId, Unique_CareActivityID FROM {source_db}.{table2} lhs LEFT JOIN {pseudo_db_test}.pseudo p0 ON p0.domain = 'Domain-1' AND p0.pseudo_type = 'Person_ID' AND lhs.Person_ID = p0.clear "
#             ]
#         assert actual_views == expected_sensitive_view_statement
