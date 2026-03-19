import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DecimalType, DateType, IntegerType

from dsp.datasets.sgss.ingestion.sgss.config import Fields, PostprocessingFields, POSITIVE_ORGANISM_SPECIES_NAME, INDETERMINATE_ORGANISM_SPECIES_NAME
from dsp.validations.validator import compare_results
from .sgss_delta_add_keystone_flag import SGSSDeltaAddKeystoneFlagStage

input_schema = StructType([
    StructField('id', StringType()),
    StructField(Fields.Lab_Pillar, StringType()),
    StructField(Fields.test_type, StringType()),
    StructField(Fields.Reporting_Lab, StringType()),
    StructField(Fields.gp_code, StringType()),
    StructField(Fields.MPSConfidence, StructType([
        StructField('MATCHED_ALGORITHM_INDICATOR', StringType()),
        StructField('MATCHED_CONFIDENCE_PERCENTAGE', StringType())
    ])),
    StructField(Fields.PdsCrossCheckCondition, StringType()),
    StructField(Fields.PERSON_ID, StringType()),
    StructField(Fields.Organism_Species_Name, StringType()),
    StructField(Fields.test_result, StringType())
])

expected_schema = StructType([
    StructField('id', StringType()),
    StructField(Fields.Lab_Pillar, StringType()),
    StructField(Fields.test_type, StringType()),
    StructField(Fields.Reporting_Lab, StringType()),
    StructField(Fields.gp_code, StringType()),
    StructField(Fields.MPSConfidence, StructType([
        StructField('MATCHED_ALGORITHM_INDICATOR', StringType()),
        StructField('MATCHED_CONFIDENCE_PERCENTAGE', StringType())
    ])),
    StructField(Fields.PdsCrossCheckCondition, StringType()),
    StructField(Fields.PERSON_ID, StringType()),
    StructField(Fields.Organism_Species_Name, StringType()),
    StructField(Fields.test_result, StringType()),
    StructField(Fields.SendToKeystone, BooleanType())
])

mps_good_quality_match = ['4', '100.00']
mps_bad_quality_match = ['4', '0.00']


def test_sgss_delta_add_keystone_flag_filter_non_pillar_1(spark: SparkSession):
    input_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('2', 'Pillar 4', '12', 'PILLAR 4 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
    ]

    expected_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('2', 'Pillar 4', '12', 'PILLAR 4 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False)
    ]

    input_df = spark.createDataFrame(input_data, input_schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._add_keystone_flag(input_df)

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert compare_results(actual_df, expected_df, join_columns=['id'])


def test_sgss_delta_add_keystone_flag_filter_non_allowed_test_type(spark: SparkSession):
    input_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('2', 'Pillar 1', '42424242', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('3', 'Pillar 1', '11', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('4', 'Pillar 1', '25', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGU'),
        ('5', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', INDETERMINATE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ORGU'),
        ('6', 'Pillar 1', '11', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', INDETERMINATE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGQ'),
    ]

    expected_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('2', 'Pillar 1', '42424242', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False),
        ('3', 'Pillar 1', '11', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('4', 'Pillar 1', '25', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGU', False),
        ('5', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', INDETERMINATE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ORGU', True),
        ('6', 'Pillar 1', '11', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', INDETERMINATE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGQ', False),
    ]

    input_df = spark.createDataFrame(input_data, input_schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._add_keystone_flag(input_df)

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert compare_results(actual_df, expected_df, join_columns=['id'])


def test_sgss_delta_add_keystone_flag_filter_react_ons_records(spark: SparkSession):
    input_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('2', 'Pillar 1', '12', 'REACT', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('3', 'Pillar 1', '12', 'IQVIA', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
    ]

    expected_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('2', 'Pillar 1', '12', 'REACT', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False),
        ('3', 'Pillar 1', '12', 'IQVIA', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False),
    ]

    input_df = spark.createDataFrame(input_data, input_schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._add_keystone_flag(input_df)

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert compare_results(actual_df, expected_df, join_columns=['id'])


def test_sgss_delta_add_keystone_flag_filter_null_gp_codes(spark: SparkSession):
    input_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', None, mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY')
    ]

    expected_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', None, mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False),
    ]

    input_df = spark.createDataFrame(input_data, input_schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._add_keystone_flag(input_df)

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert compare_results(actual_df, expected_df, join_columns=['id'])


def test_sgss_delta_add_keystone_flag_filter_welsh_gp_codes(spark: SparkSession):
    input_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'W1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY')
    ]

    expected_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'W1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False),
    ]

    input_df = spark.createDataFrame(input_data, input_schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._add_keystone_flag(input_df)

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert compare_results(actual_df, expected_df, join_columns=['id'])


def test_sgss_delta_add_keystone_flag_filter_mps_quality_match(spark: SparkSession):
    input_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_bad_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('3', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_bad_quality_match, None, 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('4', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'MPS', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('5', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_bad_quality_match, 'MPS', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY')
    ]

    expected_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_bad_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('3', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_bad_quality_match, None, 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False),
        ('4', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'MPS', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('5', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_bad_quality_match, 'MPS', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False)
    ]

    input_df = spark.createDataFrame(input_data, input_schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._add_keystone_flag(input_df)

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert compare_results(actual_df, expected_df, join_columns=['id'])


def test_sgss_delta_add_keystone_flag_filter_null_person_id(spark: SparkSession):
    input_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_bad_quality_match, 'a', None, POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY'),
    ]

    expected_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', True),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_bad_quality_match, 'a', None, POSITIVE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGY', False),
    ]

    input_df = spark.createDataFrame(input_data, input_schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._add_keystone_flag(input_df)

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert compare_results(actual_df, expected_df, join_columns=['id'])


def test_sgss_delta_add_keystone_flag_exclude_null_test_result(spark: SparkSession):
    input_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', None, None),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', INDETERMINATE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGQ'),
    ]

    expected_data = [
        ('1', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', None, None, False),
        ('2', 'Pillar 1', '12', 'PILLAR 1 TESTING', 'A1234', mps_good_quality_match, 'a', 'AAAA1', INDETERMINATE_ORGANISM_SPECIES_NAME, 'SARS-CoV-2-ANGQ', True),
    ]

    input_df = spark.createDataFrame(input_data, input_schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._add_keystone_flag(input_df)

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert compare_results(actual_df, expected_df, join_columns=['id'])


schema = StructType([
    StructField('id', StringType(), True),
    StructField(Fields.MPSConfidence, StructType([
        StructField('MATCHED_ALGORITHM_INDICATOR', StringType(), True),
        StructField('ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC', StringType()),
        StructField('MATCHED_CONFIDENCE_PERCENTAGE', StringType())
    ]), True),
    StructField(Fields.Patient_Forename, StringType(), True),
    StructField("MPSGIVEN_NAME", StringType(), True),
    StructField(Fields.Patient_Surname, StringType(), True),
    StructField("MPSFAMILY_NAME", StringType(), True),
    StructField(Fields.Patient_Sex, StringType(), True),
    StructField("MPSGENDER", StringType(), True),
    StructField(Fields.Patient_Date_Of_Birth, DateType(), True),
    StructField("MPSDATE_OF_BIRTH", IntegerType(), True),
    StructField(PostprocessingFields.NormalisedPostcode, StringType(), True),
    StructField("MPSPOSTCODE", StringType(), True),
    StructField(Fields.P2_email, StringType(), True),
    StructField("MPSEMAIL_ADDRESS", StringType(), True),
    StructField(Fields.P2_mobile, StringType(), True),
    StructField("MPSMOBILE_NUMBER", StringType(), True),
    StructField(Fields.SendToKeystone, BooleanType())
])


def test_join_on_keystone_record(spark: SparkSession):
    input_data = [
        ('1', None, 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - MPSConfidence
        ('2', ['3', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),
        # match on MATCHED_ALGORITHM_INDICATOR and POSTCODE POPULATED and ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC
        ('3', ['3', '80.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),
        # No match - ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC
        ('4', ['3', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, '', 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),  # No match - POSTCODE
        ('5', ['3', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, None, 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),  # No match - POSTCODE
        ('6', ['3', '80.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'AB12BT', 'email@test.co.uk', '', '0777777777', '', True),  # No match - POSTCODE
        ('7', ['1', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # Match on all - MATCHED_ALGORITHM_INDICATOR 1
        ('8', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # Match on all - MATCHED_ALGORITHM_INDICATOR 4
        ('9', ['2', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - MATCHED_ALGORITHM_INDICATOR 2
        ('10', ['4', '100.00', '100.00'], None, 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - patient forename
        ('11', ['4', '100.00', '100.00'], '', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - patient forename
        ('12', ['4', '100.00', '100.00'], 'Brian', 'ian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - patient forename
        ('13', ['4', '100.00', '100.00'], 'Brian', 'Brian', None, 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - patient surname
        ('14', ['4', '100.00', '100.00'], 'Brian', 'Brian', '', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - patient surname
        ('15', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'oru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - patient surname
        ('16', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Unknow', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - gender not male/female
        ('17', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '2', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - gender does not match
        ('18', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', None,
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - no dob
        ('19', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19910217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # No match - dob does not match
        ('20', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),
        # Match - Postcode good
        ('21', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, None, 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),
        # No match - Postcode
        ('22', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, '', 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),
        # No match - Postcode
        ('23', ['4', '80.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'AB12YZ', 'email@test.co.uk', '', '0777777777', '', True),
        # No match - Postcode does not match
        ('24', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),
        # No match - Postcode indicator <> 100
        ('25', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '', True),
        # Match - Email good
        ('26', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', None, '0777777777', '', True),
        # No match - Email
        ('27', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', '', '0777777777', '', True),
        # No match - Email
        ('28', ['4', '80.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', 'test@email.co.uk', '0777777777', '', True),
        # No match - Email does not match
        ('29', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', '', '0777777777', '0777777777', True),
        # Match - mobile good
        ('30', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', '', '0777777777', None, True),
        # No match - mobile
        ('31', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', '', '0777777777', '', True),
        # No match - mobile
        ('32', ['4', '80.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', '', '0777777777', '0888888888', True),
        # No match - mobile does not match
        ('33', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '', True),
        # Match - gender in lower case

    ]

    expected_data = [
        ('2', ['3', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),
        # match on MATCHED_ALGORITHM_INDICATOR and POSTCODE POPULATED and ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC
        ('7', ['1', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # Match on all - MATCHED_ALGORITHM_INDICATOR 1
        ('8', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '0777777777', True),
        # Match on all - MATCHED_ALGORITHM_INDICATOR 4
        ('20', ['4', '100.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', 'LS14BT', 'email@test.co.uk', '', '0777777777', '', True),  # Match - Postcode good
        ('25', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '', True),  # Match - Email good
        ('29', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'Male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', '', '0777777777', '0777777777', True),  # Match - mobile good
        ('33', ['4', '0.00', '100.00'], 'Brian', 'Brian', 'Boru', 'Boru', 'male', '1', datetime.date(1990, 2, 17),
         19900217, 'LS14BT', '', 'email@test.co.uk', 'email@test.co.uk', '0777777777', '', True), # Match - gender in lower case
    ]

    input_df = spark.createDataFrame(input_data, schema)

    actual_df = SGSSDeltaAddKeystoneFlagStage._remove_invalid_mps_records(input_df)
    expected_df = spark.createDataFrame(expected_data, schema)

    assert compare_results(actual_df, expected_df, join_columns=['id'])
