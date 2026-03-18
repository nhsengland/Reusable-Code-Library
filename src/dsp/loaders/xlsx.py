from pyspark.sql import SparkSession, DataFrame


# pylint:disable=too-many-arguments
def load_sheet(
        spark: SparkSession, location: str, sheet: str, start_cell: str = 'A1',
        end_cell: str = None, use_header: bool = False, infer_schema: bool = False,
        timestamp_format: str = "MM-dd-yyyy HH:mm:ss"
) -> DataFrame:
    sheet = "'{}'!".format(sheet) if sheet else ''
    data_address = "{}{}{}{}".format(sheet, start_cell, ':' if end_cell else '', end_cell or '')
    return (
        spark
        .read
        .format("com.crealytics.spark.excel")
        .option("dataAddress", data_address)  # Required
        .option("useHeader", str(use_header).lower())
        .option("treatEmptyValuesAsNulls", "true")  # Optional, default: true
        .option("inferSchema", str(infer_schema).lower())  # Optional, default: false
        .option("addColorColumns", "false")  # Optional, default: false
        .option("timestampFormat", timestamp_format)  # Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
        # .option("maxRowsInMemory", 20)  # Optional, default None.
        # .option("excerptSize", 10)  # Optional, default: 10. If set and if schema inferred, rows to infer from
        .load(location)
    )
