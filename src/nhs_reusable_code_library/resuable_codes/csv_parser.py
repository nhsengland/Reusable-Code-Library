from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class CsvParser:
    @staticmethod
    def parse(spark: SparkSession, file_path: str, schema: StructType, has_header: bool) -> DataFrame:
        reader = spark.read.format("csv").option("header", has_header).schema(schema)

        # We need to escape backslashes for the reader.
        return reader.load(file_path.replace("\\", "\\\\"))
