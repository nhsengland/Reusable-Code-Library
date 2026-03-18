# pylint: disable=redefined-outer-name
import logging
import os
import shutil
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING, Generator, Any
from typing import Tuple
from uuid import uuid4

import boto3
import petname
import pytest
import requests_pkcs12
from mock import Mock
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, TimestampType, StructType
from pytest_mock import MockFixture

from dsp.common.spark import spark_dev
from dsp.pipeline.spark_state import set_broadcast_state, standard_broadcast_state
from dsp.shared import local_path
from dsp.shared.aws import s3_bucket, LOCAL_MODE, dynamodb, ddb_table
from dsp.shared.common.test_helpers import LOCAL_TESTING_BUCKET
# noinspection PyUnresolvedReferences
from dsp.shared.conftest import *
# noinspection PyUnresolvedReferences
from dsp.shared.conftest import initialise_logging
from dsp.shared.logger import LogLevel

if TYPE_CHECKING:
    from pyspark.streaming import StreamingContext


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(LogLevel.WARN)


@pytest.fixture()
def sc(spark: SparkSession) -> SparkContext:
    """ create a spark context for unit tests

    Returns:
        SparkContext: a locally configured spark context
    """

    return spark.sparkContext


@pytest.fixture(scope='session')
def spark() -> Generator[SparkSession, None, None]:
    yield from _spark_factory()


def _spark_factory() -> Generator[SparkSession, None, None]:
    """ create a spark session for unit tests

    Returns:
        SparkSession: a locally configured spark session
    """

    with tempfile.TemporaryDirectory('_dsp', prefix='metastore_') as metastore_home:
        print("")
        print("--- start ---")
        print("")

        session = spark_dev(metastore_home)
        yield session

        print("")
        print("--- fin ---")
        print("")

        context = session.sparkContext  # type: SparkContext

        try:
            context.cancelAllJobs()
            session.catalog.clearCache()

            tables = session.catalog.listTables()

            if tables:
                print("")
                print("remaining tables:")
                print("")
                print("name:              temporary:")

                for table in tables:
                    print("{}              {}".format(table.name, table.isTemporary))

            context.stop()

            shutil.rmtree('metastore_db', ignore_errors=True)

            shutil.rmtree('spark-warehouse', ignore_errors=True)
            shutil.rmtree('etl_output', ignore_errors=True)
        except Exception as e:
            print(e)


@pytest.fixture(scope='module', autouse=True)
def clean_up_spark_context(spark: SparkSession):
    yield

    spark.catalog.clearCache()
    for rdd in spark.sparkContext._jsc.getPersistentRDDs().values():
        rdd.unpersist()


@pytest.fixture()
def streaming_context(sc: SparkContext) -> Generator['StreamingContext', None, None]:
    """ create a spark streaming context for unit tests

    Returns:
        StreamingContext: a locally configured spark streaming context
    """
    from pyspark.streaming import StreamingContext
    context = StreamingContext(sc, 1)
    yield context


@pytest.fixture()
def temp_dir() -> Generator[str, None, None]:
    """ create a temporary folder and cleanup afterwards
    """
    with tempfile.TemporaryDirectory('_dsp', prefix='pytest_') as temp_dir_name:
        yield temp_dir_name


@pytest.fixture()
def s3_temp() -> Generator[Tuple[Any, str], None, None]:
    bucket = s3_bucket(LOCAL_TESTING_BUCKET)

    bucket.create()

    bucket.Acl().put(ACL='public-read')

    temp_folder = uuid4().hex

    yield bucket, 's3://{}/{}'.format(bucket.name, temp_folder)

    for obj in bucket.objects.filter(Prefix=temp_folder):
        obj.delete()


@pytest.fixture()
def temp_db(spark: SparkSession) -> Generator[str, None, None]:
    db_name = uuid4().hex

    spark.sql('CREATE DATABASE {}'.format(db_name))

    yield db_name

    for table in spark.catalog.listTables(db_name):
        if table.database != db_name:
            continue

        ttype = 'VIEW' if table.tableType == 'VIEW' else 'TABLE'
        spark.sql('DROP {} {}.{}'.format(ttype, db_name, table.name))

    spark.sql('DROP DATABASE {}'.format(db_name))

    spark.sql('USE default')


@pytest.fixture(scope='session', autouse=True)
def global_setup():
    os.environ.setdefault(LOCAL_MODE, 'True')
    os.environ['env'] = 'local'
    assert os.environ.get('env') == 'local'

    initialise_logging()

    metastore_path = local_path('metastore_db')
    if os.path.exists(metastore_path):
        shutil.rmtree(metastore_path, ignore_errors=True)

    set_broadcast_state(standard_broadcast_state({}))


def clone_schema(table):
    key_schema = table.key_schema

    attributes = table.attribute_definitions

    indexes = table.global_secondary_indexes

    for index in indexes:
        del index['IndexStatus']
        del index['IndexSizeBytes']
        del index['ItemCount']
        del index['IndexArn']

    clone = {
        'KeySchema': key_schema,
        'AttributeDefinitions': attributes,
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1000,
            'WriteCapacityUnits': 1000
        }
    }

    if indexes:
        for index in indexes:
            index['ProvisionedThroughput']['ReadCapacityUnits'] = 1000
            index['ProvisionedThroughput']['WriteCapacityUnits'] = 1000
        clone['GlobalSecondaryIndexes'] = indexes

    return clone


@pytest.fixture(scope='function')
def temp_submissions() -> Generator[boto3.session.Session.resource, None, None]:
    ddb = dynamodb()

    table_name = petname.Generate(words=4, separator='_')

    submissions = ddb_table('submissions')

    cloned = clone_schema(submissions)

    table = ddb.create_table(TableName=table_name, **cloned)

    yield table

    table.delete()


@pytest.fixture(scope='function')
def requests_pkcs12_get_mock(mocker: MockFixture) -> Mock:
    mock_get = mocker.patch.object(requests_pkcs12, "get", autospec=True)

    return mock_get


@pytest.fixture()
def pseudo_db_test(spark: SparkSession):
    db_name = 'pseudo' + uuid4().hex

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    schema = StructType(
        [
            StructField('id', StringType()),
            StructField('domain', StringType()),
            StructField('pseudo_type', StringType()),
            StructField('pseudo', StringType()),
            StructField('clear', StringType()),
            StructField('created', TimestampType()),
            StructField('job_id', StringType())
        ]
    )

    rows = [
        ("1", 'Domain-1', 'Person_ID', 'LD2ND9ZN3C2DY2Q', '1234567890', datetime.utcnow(), "1"),
        ("1", 'COMM-CSDS', 'Person_ID', 'DSHJSJJLSDJFLKF', '1234567890', datetime.utcnow(), "1"),
        ("1", 'Domain-1', 'Person_ID', 'JE2ND9FH8H7DW8F', '0987654321', datetime.utcnow(), "1"),
        ("1", 'Domain-1', 'Person_ID', 'FHUE7HFLE8EFL8H', '3847837636', datetime.utcnow(), "1"),
        ("1", 'COMM-CSDS', 'Person_ID', 'FHUE7HFLE8EFL6J', '5758490385', datetime.utcnow(), "1"),
        ("1", 'COMM-CSDS', 'Person_ID', 'ASSE7HFLE8EFL6K', '4567438905', datetime.utcnow(), "1"),
        ("1", 'COMM-CSDS', 'Person_ID', 'GHENNBUMDHSHE6H', '5749494733', datetime.utcnow(), "1"),
        ("1", 'COMM-CSDS', 'Person_ID', 'KJLDKLKEHKDLKIE', '4858729709', datetime.utcnow(), "1"),
        ("1", 'COMM-IAPT', 'Person_ID', 'XYZ6JOZPS1ITTTT', '9018978916', datetime.utcnow(), "1"),
        ("1", 'COMM-IAPT', 'Person_ID', 'XYZ6JOZPS1IO6U4', '9254945124', datetime.utcnow(), "1"),
        ("1", 'COMM-IAPT', 'Person_ID', 'XYZ6JOZPS1IO6U5', '9254945125', datetime.utcnow(), "1"),
        ("1", 'COMM-IAPT', 'Person_ID', 'XYZ6JOZPS1IO6U6', '9254945126', datetime.utcnow(), "1"),
        ("1", 'COMM-MHSDS', 'Person_ID', 'LEI6JOZPS1IO6U8', '3338978892', datetime.utcnow(), "1"),
        ("1", 'COMM-MHSDS', 'Person_ID', 'MEI6JOZPS1IO6U9', '4448798109', datetime.utcnow(), "1"),
    ]

    df = spark.createDataFrame(rows, schema)
    df.write.saveAsTable(f"{db_name}.pseudo")

    salt_table_schema = StructType(
        [
            StructField('field', StringType()),
            StructField('domain', StringType()),
            StructField('salt', StringType()),
            StructField('created', TimestampType())
        ]
    )

    salt_table_rows = [
        ('ServiceRequestId', None, '2407135900', datetime.utcnow()),
        ('UniqCareActID', None, '5199226389', datetime.utcnow())
    ]
    salt_df = spark.createDataFrame(salt_table_rows, salt_table_schema)
    salt_df.write.saveAsTable(f"{db_name}.salt")

    yield db_name

    spark.sql(f"DROP DATABASE {db_name} CASCADE")