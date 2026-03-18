from pyspark.sql.functions import col, trim


def strip_field(column_name):
    return trim(col(column_name))
