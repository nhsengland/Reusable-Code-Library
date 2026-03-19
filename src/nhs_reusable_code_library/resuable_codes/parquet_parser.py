import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from dsp.common import canonical_name


class ParquetParser:
    @staticmethod
    def parse(spark: SparkSession, file_path: str, schema: StructType) -> DataFrame:
        return spark.read.schema(schema).parquet(file_path)

    @staticmethod
    def get_dms_table_name(file_path: str) -> str:
        """Returns the canonical name of the SQL table from which a DMS-migrated
        parquet file originated
        """
        dms_pattern = "^.+/([^/]+)/[^/]+\\.parquet$"

        path_match = re.match(dms_pattern, file_path)
        if not path_match:
            raise Exception(f"Unexpected DMS parquet file path: {file_path}")

        return canonical_name(path_match.groups()[0]).lower()
