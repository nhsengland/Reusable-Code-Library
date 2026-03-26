import uuid

from pyspark import Row
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from dsp.common.relational import Table, Field
from dsp.datasets.common import Fields as Common
from dsp.validations.iapt.person_score_validations import AssessmentToolName
from dsp.validations.iapt.validation_functions import is_valid_assessment_scale_score_rule, \
    is_valid_score_format_for_snomed_code, is_valid_assessment_scale_code_rule
from dsp.validations.common_validation_functions import apply_rules
import pytest 

@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark

def _test_data_table(spark: SparkSession):
    records = [
        ('0', '1128221000000100', '1'),  # valid row range is 0-72& valid format n2
        ('1', '1128221000000100', '100'),  # invalid row range is 0-72 & invalid format n2
        ('2', 'XXXXXXXXXXXXXXX', '10'),  # no mtaching SNOMED
        ('3', '1128221000000100', None),  # allowing null person score
        ('4', None, '10'),  # allowing null SNOMED
        ('5', '747871000000107', 'Y'),  # valid record allowed values Y or N & valid format a1
        ('6', '747871000000107', 'NA'),  # invalid record allowed values Y or N & valid format a1
        ('7', '747871000000107', 'p'),  # invalid record allowed values Y or N & valid format a1
        ('8', '473355002', '4.9'),  # valid score range is 0-5.00 & valid format n1.max n2
        ('9', '473355002', '4.999'),  # valid score range is 0-5.00 & invalid format n1.max n2
        ('10', '904691000000103', 'pq'),  # invalid score & valid format max an2
        ('11', '904691000000103', 'NAY'),  # invalid score & invalid format max an2
        ('12', '910931000000101', '20')
    ]
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("SNOMED", StringType(), True),
        StructField("Score", StringType(), True)
    ])

    spark.createDataFrame(records, schema).createOrReplaceTempView("testTable")
    return Table('testTable',
                  Field('SNOMED', str),
                  Field('Score', str))


def tests_is_valid_assessment_scale_code_rule(spark: SparkSession):
    table_name = f"{uuid.uuid4().hex}_testTable"
    records = [
            ('0', '1128221000000100'),
            ('1', '747941000000105'),
            ('2', '747871000000107'),
            ('3', '748161000000109')
        ]
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("SNOMED", StringType(), True)
    ])
    spark.createDataFrame(records, schema).createOrReplaceTempView(table_name)

    table = Table(table_name, Field('SNOMED', str), Field('Score', str))

    apply_rules(spark, is_valid_assessment_scale_code_rule(
        'code', table['SNOMED'], [AssessmentToolName.T_PEQ, AssessmentToolName.A_PEQ]))
    validate_row = Row("ID", "SNOMED", Common.DQ)
    assert sorted(spark.table(table_name).select('ID', 'SNOMED', Common.DQ).collect(), key=lambda r: r[0]) == [
        validate_row('0', '1128221000000100', ['code']),
        validate_row('1', '747941000000105', []),
        validate_row('2', '747871000000107', []),
        validate_row('3', '748161000000109', ['code'])
    ]


def test_is_valid_assessment_scale_score(spark: SparkSession):
    table = _test_data_table(spark)
    apply_rules(spark, is_valid_assessment_scale_score_rule('code', table['SNOMED'], table['Score']))
    validate_row = Row("ID", "SNOMED", "Score", Common.DQ)
    assert (spark.table("testTable").collect()) == [
        validate_row('0', '1128221000000100', '1', []),
        validate_row('1', '1128221000000100', '100', ['code']),
        validate_row('2', 'XXXXXXXXXXXXXXX', '10', []),
        validate_row('3', '1128221000000100', None, []),
        validate_row('4', None, '10', []),
        validate_row('5', '747871000000107', 'Y', []),
        validate_row('6', '747871000000107', 'NA', ['code']),
        validate_row('7', '747871000000107', 'p', ['code']),
        validate_row('8', '473355002', '4.9', []),
        validate_row('9', '473355002', '4.999', []),
        validate_row('10', '904691000000103', 'pq', ['code']),
        validate_row('11', '904691000000103', 'NAY', ['code']),
        validate_row('12', '910931000000101', '20', [])
    ]


def test_is_valid_score_format_for_snomed_code(spark: SparkSession):
    table = _test_data_table(spark)
    apply_rules(spark, is_valid_score_format_for_snomed_code('code', table['SNOMED'], table['Score']))
    validate_row = Row("ID", "SNOMED", "Score", Common.DQ)
    assert (spark.table("testTable").collect()) == [
        validate_row('0', '1128221000000100', '1', []),
        validate_row('1', '1128221000000100', '100', ['code']),
        validate_row('2', 'XXXXXXXXXXXXXXX', '10', []),
        validate_row('3', '1128221000000100', None, []),
        validate_row('4', None, '10', []),
        validate_row('5', '747871000000107', 'Y', []),
        validate_row('6', '747871000000107', 'NA', ['code']),
        validate_row('7', '747871000000107', 'p', []),
        validate_row('8', '473355002', '4.9', []),
        validate_row('9', '473355002', '4.999', ['code']),
        validate_row('10', '904691000000103', 'pq', []),
        validate_row('11', '904691000000103', 'NAY', ['code']),
        validate_row('12', '910931000000101', '20', [])
    ]
