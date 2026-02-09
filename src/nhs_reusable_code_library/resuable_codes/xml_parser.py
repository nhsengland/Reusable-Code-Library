from functools import reduce
from typing import List, NamedTuple, Union

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType

from nhs_reusable_code_library.resuable_codes.dataframe_renamer import (
    strip_xml_namespaces_in_dataframe,
)


class XmlParser:
    # The schema is defined in these Extract classes. The schema is required as
    # there are situations where the parser does not infer array types correctly
    # so it must be specified when parsing. The schema must be defined manually
    # as the corresponding table schema will have canonicalised column names so
    # passing in the table schema would cause a schema mismatch error.
    class MultiRowExtracts(NamedTuple): #Extracting multiple rows 
        row_tags: List[str]
        schema: StructType

    class SingleRowExtract(NamedTuple): #Extracting single rows 
        row_tag: str
        dest_column: str
        schema: StructType

    @staticmethod
    def _parse_rows( #function to parse rows within an xml file using a defined schema
        spark: SparkSession, file_path: str, row_tag: str, schema: StructType
    ) -> DataFrame:
        reader = (
            spark.read.format("com.databricks.spark.xml")
            .option("rowTag", row_tag)
            .option("inferSchema", False)
            .option("ignoreNamespace", True)
            .schema(schema)
        )

        # We need to escape backslashes for the reader.
        return reader.load(file_path.replace("\\", "\\\\"))

    @staticmethod
    def _parse_single_row_extract(  #function to extract single row within an xml file
        spark, file_path: str, row_tag: str, schema: StructType
    ) -> Row:
        df = XmlParser._parse_rows(spark, file_path, row_tag, schema)
        row_count = df.count()

        if row_count != 1:
            raise Exception(
                f"Unexpected number of rows extracted ({row_count})"
                f" for single-row extract '{row_tag}'."
            )

        return df.head()

    @staticmethod
    def _parse_single_rows_df(  #function to parse single row to a dataframe 
        spark: SparkSession, file_path: str, single_row_extracts: List[SingleRowExtract]
    ) -> DataFrame:
        single_row_values = []
        single_row_cols = []

        for extract in single_row_extracts:
            extract_value = XmlParser._parse_single_row_extract(
                spark, file_path, extract.row_tag, extract.schema
            )

            single_row_values.append(extract_value)
            single_row_cols.append(extract.dest_column)

        single_rows_data = [tuple(single_row_values)]
        single_rows_df = spark.createDataFrame(single_rows_data, single_row_cols)

        return broadcast(single_rows_df)

    @staticmethod
    def _parse_multi_row_df( #function to parse multiples rows to a dataframe 
        spark: SparkSession, file_path: str, multi_row_extracts: MultiRowExtracts
    ) -> DataFrame:
        extract_dataframes = []

        for row_tag in multi_row_extracts.row_tags:
            df = XmlParser._parse_rows(
                spark, file_path, row_tag, multi_row_extracts.schema
            )
            extract_dataframes.append(df)

        return reduce(DataFrame.union, extract_dataframes)

    @staticmethod
    def _verify_extract_definitions(
        multi_row_extracts: MultiRowExtracts,
        single_row_extracts: List[SingleRowExtract],
    ) -> None:
        if len(multi_row_extracts.row_tags) == 0:
            raise Exception("Insufficient number of multi-row tags provided (0).")

        single_row_columns = [extract.dest_column for extract in single_row_extracts]

        if len(single_row_columns) != len(set(single_row_columns)):
            raise Exception(
                f"Found clashes between single-row columns: {single_row_columns}"
            )

        multi_row_columns = [field.name for field in multi_row_extracts.schema]
        clashing_cols = set(multi_row_columns).intersection(single_row_columns)

        if len(clashing_cols) != 0:
            raise Exception(
                f"Found clashes between multi-row and single-row columns:"
                f" {clashing_cols}"
            )

    @staticmethod
    def _parse_multiple_row_extract_list(
        spark: SparkSession,
        file_path: str,
        multi_row_extracts: List[MultiRowExtracts],
        single_row_extracts: SingleRowExtract,
    ) -> DataFrame:
        [
            XmlParser._verify_extract_definitions(
                multi_row_extract, single_row_extracts
            )
            for multi_row_extract in multi_row_extracts
        ]

        base_multi_row_extact = XmlParser._parse_multi_row_df(
            spark, file_path, multi_row_extracts
        )
        for multi_row_extract in multi_row_extracts[1:]:
            extracted = XmlParser._parse_multi_row_df(
                spark, file_path, multi_row_extract
            )
            base_multi_row_extact = base_multi_row_extact.crossJoin(extracted)

        return base_multi_row_extact

    @staticmethod
    def parse(
        spark: SparkSession,
        file_path: str,
        multi_row_extracts: Union[MultiRowExtracts, List[MultiRowExtracts]],
        single_row_extracts: List[SingleRowExtract],
    ) -> DataFrame:
        """
        Extracts rows for a XML file to a single data frame. Rows from multi-row
        extracts will be appended with a single, unified schema (where irrelevant
        columns from other extracts are simply left null), while rows from single-row
        extracts are appended to each resulting row as a struct column.
        """
        if isinstance(multi_row_extracts, list) and len(multi_row_extracts) > 1:
            multi_row_df = XmlParser._parse_multiple_row_extract_list(
                spark, file_path, multi_row_extracts, single_row_extracts
            )
        else:
            if isinstance(multi_row_extracts, list):
                multi_row_extracts = multi_row_extracts[0] 
            XmlParser._verify_extract_definitions(
                multi_row_extracts, single_row_extracts
            )
            multi_row_df = XmlParser._parse_multi_row_df(
                spark, file_path, multi_row_extracts
            )
            
        single_rows_df = XmlParser._parse_single_rows_df(
            spark, file_path, single_row_extracts
        )

        df = multi_row_df.crossJoin(single_rows_df)
        return strip_xml_namespaces_in_dataframe(df)
