from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from dsp.validation.validator import compare_results
from .sgss_map_result_to_description import SGSSMapResultToDescriptionStage


def test_sgss_map_result_to_description(spark: SparkSession):
    input_schema = StructType([
        StructField('Organism_Species_Name', StringType()),
        StructField('test_type', StringType())
    ])

    input_data = [
        ('SARS-CoV-2 CORONAVIRUS (Covid-19)', '25'),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19) NEGATIVE', '12'),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19) VOID', '13'),
        ('NO SUCH ORGANISM SPECIES NAME', '25'),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19)', '42424242'),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19) INDETERMINATE', '25'),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19) INDETERMINATE', '6')
    ]

    expected_schema = StructType([
        StructField('Organism_Species_Name', StringType()),
        StructField('test_type', StringType()),
        StructField('test_result_sct', StringType()),
        StructField('test_result', StringType()),
        StructField('method_of_detection', StringType()),
        StructField('test_kit', StringType()),
        StructField('specimen_type', StringType()),
    ])

    expected_data = [
        ('SARS-CoV-2 CORONAVIRUS (Covid-19)', '25', '1322781000000102', 'SARS-CoV-2-ANGY', 'antigen', 'lft',
         'Nasopharyngeal swab'),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19) NEGATIVE', '12', '1324581000000102', 'SARS-CoV-2-ORGN', 'rna',
         'pcr', 'Nasopharyngeal swab'),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19) VOID', '13', '1321691000000102', 'SARS-CoV-2-ORGU', 'rna',
         'pcr', 'Nasopharyngeal swab'),
        ('NO SUCH ORGANISM SPECIES NAME', '25', None, None, None, None, None),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19)', '42424242', None, None, None, None, None),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19) INDETERMINATE', '25', '1322801000000101', 'SARS-CoV-2-ANGQ', 'antigen',
         'lft', 'Nasopharyngeal swab'),
        ('SARS-CoV-2 CORONAVIRUS (Covid-19) INDETERMINATE', '6', '1321691000000102', 'SARS-CoV-2-ORGU', 'rna',
         'pcr', 'Nasopharyngeal swab')
    ]

    input_df = spark.createDataFrame(input_data, input_schema)
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    actual_df = SGSSMapResultToDescriptionStage._map_result_to_description(spark, input_df)
    assert compare_results(actual_df, expected_df, join_columns=['Organism_Species_Name', 'test_type'])
