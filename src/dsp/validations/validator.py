# pylint: disable=R0912
# pylint: disable=R1702
import os
import pprint
import re
from collections import defaultdict
from itertools import chain
from typing import Iterable, List, Tuple, DefaultDict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit, hash as pyspark_hash
from pyspark.sql.types import ArrayType, StructType

from dsp.dq_files.output import Fields as DQFields
from dsp.dq_files.coerce_types import coerce_data_types
from dsp.udfs import SortedJsonString
from dsp.validations.loader import load_files
from dsp.validations.validator_config import expected_type_coercions, ignored_columns, ignored_column_values, \
    ignored_nested_columns

NUM_REC_LOGGED = 50


def return_dq_result(spark: SparkSession, actual: str, metadata: dict) -> bool:
    """
        validate whether the records in one data file matches in another file
        Args:
            spark (SparkSession): the spark session
            actual (str): uri of the dq output
            expected (str): uri of the expected dq output csv
            metadata (dict): metadata dictionary

        Returns:
            bool: True when the contents in files matches
    """
    if os.path.isdir(actual):
        dq_df = load_files(spark=spark, metadata={
            "file_format": "parquet",
            "file_location": actual,
            "submission_id": metadata["submission_id"],
            "submitted_timestamp": metadata["submitted_timestamp"],
            "dataset_id": metadata["dataset_id"]
        }, add_metadata=False).drop(DQFields.DQ_TS)

        return dq_df

    else:
        return False


def validate_dq_result(spark: SparkSession, actual: str, expected: str, metadata: dict) -> bool:
    """
        validate whether the records in one data file matches in another file
        Args:
            spark (SparkSession): the spark session
            actual (str): uri of the dq output
            expected (str): uri of the expected dq output csv
            metadata (dict): metadata dictionary

        Returns:
            bool: True when the contents in files matches
    """

    output_df = load_files(spark=spark, metadata={
        "file_format": "parquet",
        "file_location": actual,
        "submission_id": metadata["submission_id"],
        "submitted_timestamp": metadata["submitted_timestamp"],
        "dataset_id": metadata["dataset_id"]
    }, add_metadata=False).drop(DQFields.DQ_TS)

    expected_df = load_files(spark=spark, metadata={
        "file_format": "dq_xml",
        "file_location": expected,
        "submission_id": metadata["submission_id"],
        "submitted_timestamp": metadata["submitted_timestamp"],
        "dataset_id": metadata["dataset_id"]
    }, add_metadata=False)

    return compare_results(output_df, expected_df, [DQFields.ROW_INDEX, DQFields.CODE],
                           ignore_column_values=[DQFields.DQ_TS])


def validate_expected_result(spark: SparkSession, actual: str, expected: str, metadata: dict) -> bool:
    """
        validate whether the records in one data file matches in another file
        Args:
            spark (SparkSession): the spark session
            actual (str): uri of the processed parquet file
            expected (str): uri of the expected result csv
            metadata (dict): metadata dictionary

        Returns:
            bool: True when the contents in files matches
    """

    input_dict = {
        "file_format": "parquet",
        "file_location": actual,
        "submission_id": metadata["submission_id"],
        "submitted_timestamp": metadata["submitted_timestamp"],
        "dataset_id": metadata["dataset_id"]
    }

    output_df = load_files(spark=spark, metadata=input_dict, add_metadata=False)

    file_format = '.'.join(os.path.basename(expected).split('.')[1:])
    input_dict = {
        "file_format": file_format,
        "file_location": expected,
        "submission_id": metadata["submission_id"],
        "submitted_timestamp": metadata["submitted_timestamp"],
        "dataset_id": metadata["dataset_id"]
    }

    expected_df = load_files(spark=spark, metadata=input_dict, add_metadata=True)

    dataset_id = metadata["dataset_id"]

    # coerce the expected df to update the schema
    coercions = expected_type_coercions.get(metadata["dataset_id"], [])
    typed_expected_df = coerce_data_types(expected_df, coercions)

    ignore_columns = ignored_columns.get(dataset_id, [])
    ignore_column_values = ignored_column_values.get(dataset_id, [])
    ignore_nested_columns = ignored_nested_columns.get(dataset_id, [])

    return compare_results(
        output_df, typed_expected_df, ['META.RECORD_INDEX'],
        ignore_columns=ignore_columns, ignore_column_values=ignore_column_values,
        ignore_nested_columns=ignore_nested_columns
    )


def get_df_show_string(df: DataFrame, n: int = 20, truncate: bool = False, message: str = '') -> List[str]:
    message = (message or '').strip()

    if isinstance(truncate, bool) and truncate:
        show_string = df._jdf.showString(n, 20, False)
    else:
        show_string = df._jdf.showString(n, int(truncate), False)

    return [show_string] if not message else [message, show_string]


def compare_results(
        actual_df: DataFrame, expected_df: DataFrame,
        join_columns: Iterable[str], ignore_columns: Iterable[str] = None, ignore_column_values: Iterable[str] = None,
        ignore_specific_values: Iterable[str] = None, ignore_nested_columns: Iterable[str] = None,
        test_name: str = None
) -> bool:
    all_messages = []

    def print_messages(summary_message: str):
        print('\n.'.join(
            ["----------{}----------".format(test_name), summary_message]
            + all_messages +
            ["----^^----{}----^^----".format(test_name)]
        ))

    actual_df.cache()
    expected_df.cache()

    try:

        test_name = '{}: '.format(test_name) if test_name else ''

        join_columns = [c for c in join_columns]

        ignore_columns = [c for c in (ignore_columns or [])]

        ignore_nested_columns = [c for c in (ignore_nested_columns or [])]

        ignore_column_values = [c for c in chain(ignore_columns, (ignore_column_values or []))]

        schema_match, messages = match_schema(expected_df, actual_df, ignore_columns)
        all_messages.extend(messages)
        if not schema_match:
            print_messages("actual output schema does not match the expected schema")
            return False

        # total rows validation
        row_count_match, messages = match_row_count(expected_df, actual_df)
        all_messages.extend(messages)

        # records match
        expected_match, messages = validate_all_records_present(
            expected_df, actual_df, join_columns, "missing expected records"
        )
        all_messages.extend(messages)

        actual_match, messages = validate_all_records_present(
            actual_df, expected_df, join_columns, "additional records that weren't expected"
        )
        all_messages.extend(messages)

        # records content match
        all_values_match, messages = compare_column_values(
            expected_df, actual_df, join_columns, ignore_column_values, ignore_specific_values, ignore_nested_columns
        )
        all_messages.extend(messages)

        if all([row_count_match, expected_match, actual_match, all_values_match]):
            print_messages("compare dataframes successful")
            return True

        print_messages("compare dataframes failed")
        return False

    except Exception:
        print_messages("compare dataframes failed")
        raise
    finally:
        actual_df.unpersist()
        expected_df.unpersist()


def compare_results_hash(
        actual_df: DataFrame, expected_df: DataFrame, test_name: str = None
) -> bool:
    all_messages = []

    def print_messages(summary_message: str):
        print('\n.'.join(
            ["----------{}----------".format(test_name), summary_message]
            + all_messages +
            ["----^^----{}----^^----".format(test_name)]
        ))

    actual_df.cache()
    expected_df.cache()

    try:

        test_name = '{}: '.format(test_name) if test_name else ''

        schema_match, messages = match_schema(expected_df, actual_df, [])
        all_messages.extend(messages)
        if not schema_match:
            print_messages("actual output schema does not match the expected schema")
            return False

        # total rows validation
        row_count_match, messages = match_row_count(expected_df, actual_df)
        all_messages.extend(messages)

        # records match
        expected_match, messages = validate_all_records_present_by_hash(
            expected_df, actual_df, "unmatched expected records"
        )
        all_messages.extend(messages)

        actual_match, messages = validate_all_records_present_by_hash(
            actual_df, expected_df, "unmatched additional records"
        )
        all_messages.extend(messages)

        if all([row_count_match, expected_match, actual_match]):
            print_messages("compare dataframe hashes successful")
            return True

        print_messages("compare dataframes hashes failed")
        return False

    except Exception:
        print_messages("compare dataframes hashes failed")
        raise
    finally:
        actual_df.unpersist()
        expected_df.unpersist()


def match_schema(
        expected_df: DataFrame, actual_df: DataFrame, ignore_columns: List[str]
) -> Tuple[bool, List[str]]:
    """
    check whether the dataframe schemas match
    Args:
        expected_df (DataFrame): the spark dataframe
        actual_df (DataFrame): the spark dataframe
        ignore_columns (List[str]): list of columns to ignore

    Returns:
        bool: True when the schema matches
    """

    schema_match = True
    expected_schema = expected_df.dtypes
    actual_schema = actual_df.dtypes
    mismatched_data_types = []
    unexpected_columns = []
    missing_columns = []
    for column_tuple in actual_schema:

        column = column_tuple[0]
        if column in ignore_columns:
            continue

        if column_tuple in expected_schema:
            continue

        column_found = False
        for expected_tuple in expected_schema:
            if expected_tuple[0] != column_tuple[0]:
                continue

            column_found = True
            mismatched_data_types.append((column_tuple[0], expected_tuple[1], column_tuple[1]))
            break

        if not column_found:
            unexpected_columns.append(column_tuple[0])

        schema_match = False

    for column_tuple in expected_schema:

        column = column_tuple[0]
        if column in ignore_columns:
            continue

        if column_tuple in actual_schema:
            continue

        column_found = False
        for output_tuple in actual_schema:
            if output_tuple[0] != column_tuple[0]:
                continue
            column_found = True
            break

        if column_found:
            continue

        missing_columns.append(column_tuple[0])
        schema_match = False

    if schema_match:
        return True, []

    messages = []

    if unexpected_columns:
        messages.append("unexpected columns (present in actual but not in expected): {}".format(unexpected_columns))

    if missing_columns:
        messages.append("missing columns (present in expected but not in actual): {}".format(missing_columns))

    if mismatched_data_types:
        messages.append("column datatype mismatch(es) ({}):".format(len(mismatched_data_types)))
        for col_name, expected_dt, actual_dt in mismatched_data_types:
            messages.append(
                "column_name={}, expected_datatype={}, actual_datatype={}".format(col_name, expected_dt, actual_dt)
            )

    return False, messages


def match_row_count(expected_df: DataFrame, actual_df: DataFrame) -> Tuple[bool, List[str]]:
    """
        check whether the dataframe row count match
        Args:
            expected_df (DataFrame): the spark dataframe
            actual_df (DataFrame): the spark dataframe

        Returns:
            bool: True when the total rows in dataset matches
    """
    expected_total_rows = expected_df.count()
    actual_total_rows = actual_df.count()

    if expected_total_rows == actual_total_rows:
        return True, []

    return False, [
        "output and expected file row count mismatch. expected {} rows, found {} rows".format(
            expected_total_rows,
            actual_total_rows
        )
    ]


def validate_all_records_present(
        df_1: DataFrame, df_2: DataFrame, join_columns: List[str], message: str
) -> Tuple[bool, List[str]]:
    """
        check whether all the records in one dataset is present in other dataset
        Args:
            df_1 (DataFrame): the spark dataframe
            df_2 (DataFrame): the spark dataframe
            join_columns (list): list of columns to join on the dataset
            message (str): message

        Returns:
            bool: True when the contents in dataset matches
    """
    df1_alias = df_1.alias("df1_a")
    df2_alias = df_2.alias("df2_a")
    join_conditions = []
    for join_column in join_columns:
        join_conditions.append(col("df1_a." + join_column).eqNullSafe(col("df2_a." + join_column)))

    anti_joined_df = df1_alias.join(df2_alias, join_conditions, "left_anti")
    if anti_joined_df.count() == 0:
        return True, []

    return False, get_df_show_string(anti_joined_df, n=NUM_REC_LOGGED, message=message)


def validate_all_records_present_by_hash(
        df_1: DataFrame, df_2: DataFrame, message: str
) -> Tuple[bool, List[str]]:
    """
    Check whether all the records in one dataset are present in the other dataset by hashing each record, counting the
    occurrences and then joining on the hash.
    Args:
        df_1: the expected records dataframe
        df_2: the actual records dataframe
        message: message
    Returns: (bool) True when the contents in the dataset matches
    """
    df1_alias = df_1.alias("df1_a")
    df2_alias = df_2.alias("df2_a")
    df1_with_hashes = df1_alias.withColumn("hash", pyspark_hash(*df1_alias.columns).alias('hash'))
    df2_with_hashes = df2_alias.withColumn("hash", pyspark_hash(*df1_alias.columns).alias('hash'))
    df1_hash_occurrences = df1_with_hashes.groupBy("hash").agg(count(lit(1)).alias("occurrences"))
    df2_hash_occurrences = df2_with_hashes.groupBy("hash").agg(count(lit(1)).alias("occurrences"))
    # find records that can't be matched based on hash
    anti = df1_hash_occurrences.join(df2_hash_occurrences,
                                     [df1_hash_occurrences['hash'] == df2_hash_occurrences['hash']],
                                     how="left_anti")
    issues = []

    if anti.count() != 0:
        original_records = anti.join(df1_with_hashes, [df1_with_hashes['hash'] == anti['hash']], how='left')\
            .drop('hash').drop('occurrences')
        issues.extend(get_df_show_string(original_records, n=NUM_REC_LOGGED, message="Unmatched - {}".format(message)))
    # find records that match on hash but not on occurrences count
    anti_wrong_count = df1_hash_occurrences.join(df2_hash_occurrences,
                                                 [df1_hash_occurrences['hash'] == df2_hash_occurrences['hash'],
                                                  df1_hash_occurrences['occurrences'] == df2_hash_occurrences['occurrences']],
                                                 how="left_anti")\
        .join(anti, df1_hash_occurrences['hash'] == anti['hash'], how='left_anti')

    if anti_wrong_count.count() != 0:
        original_records_wrong_count = anti_wrong_count.join(df1_with_hashes,
                                                            [df1_with_hashes['hash'] == anti_wrong_count['hash']],
                                                            how='left').drop('hash').drop('occurrences')
        issues.extend(get_df_show_string(original_records_wrong_count, n=NUM_REC_LOGGED,
                                         message="Incorrect count - {}".format(message)))

    return len(issues) == 0, issues


def compare_column_values(
        expected_df: DataFrame, actual_df: DataFrame, join_columns: List[str],
        ignore_columns: List[str], ignore_specific_values: List[str], ignored_nested_cols: List[str]
) -> Tuple[bool, List[str]]:
    import deepdiff  # only present in local and dev

    messages = []
    all_values_match = True

    # Replace NULL/None fields with " ", to ensure all records are included in the JOIN.
    # This prevents rows with empty key fields from being silently dropped from the tests.
    df1_alias = expected_df.na.fill(" ").alias("df1_a")
    df2_alias = actual_df.na.fill(" ").alias("df2_a")

    all_columns = expected_df.dtypes
    join_conditions = []
    for join_column in join_columns:
        join_conditions.append(col("df1_a." + join_column) == col("df2_a." + join_column))

    ignore_columns = ignore_columns or []

    # group ignored nested columns by the root column
    regex_ignored_nested_cols = defaultdict(list)  # type: DefaultDict[str, List[str]]
    for c in ignored_nested_cols:
        if "." in c:
            root_col, nested_col = c.split(".")
        else:
            # if root column was not provided, include the nested column in all top-level columns
            root_col = "*"
            nested_col = c
        # the name should include a regex path for the nested column, so square brackets have to be escaped
        regex_ignored_nested_cols[root_col].append(fr"\['{nested_col}'\]")

    joined_df = df1_alias.join(df2_alias, join_conditions)

    for col_name, col_type in all_columns:

        if col_name in join_columns or col_name in ignore_columns:
            continue

        df1_col_name = 'df1_a.' + col_name
        df2_col_name = 'df2_a.' + col_name
        column_list = []
        for join_column in join_columns:
            column_list.append(col("df1_a." + join_column))

        if re.fullmatch(r"map<string,.+>", col_type):
            column_list.append(SortedJsonString(df1_col_name).alias('expected_value'))
            column_list.append(SortedJsonString(df2_col_name).alias('actual_value'))
        else:
            column_list.append(col(df1_col_name).alias('expected_value'))
            column_list.append(col(df2_col_name).alias('actual_value'))

        compare_df = joined_df.select(column_list)

        where_clause = 'expected_value is distinct from actual_value'
        if ignore_specific_values:
            where_clause += ' AND expected_value not in ("{}")'.format('","'.join(ignore_specific_values))

        invalid_df = compare_df.where(where_clause)

        invalid_df.cache()

        if invalid_df.count() == 0:
            invalid_df.unpersist()
            continue

        if all_values_match:
            messages.append('column values do not match:')

        col_message = 'Column = {}'.format(col_name)

        if not isinstance(actual_df.schema[col_name].dataType, (ArrayType, StructType)):
            all_values_match = False
            messages.extend(get_df_show_string(invalid_df, n=NUM_REC_LOGGED, message=col_message))
            invalid_df.unpersist()
            continue

        messages.append(col_message + ' - diff:')

        for row in invalid_df.collect():
            row = row.asDict(recursive=True)
            exclude_regex_paths = regex_ignored_nested_cols.get("*", []) + regex_ignored_nested_cols.get(col_name, [])
            diff = deepdiff.DeepDiff(
                row['expected_value'],
                row['actual_value'],
                ignore_order=True,
                report_repetition=True,
                exclude_regex_paths=exclude_regex_paths
            )
            for diff_type, _ in diff.items():
                for location, mismatches in diff[diff_type].items():
                    all_values_match = False
                    messages.append(
                        '{}: {} @ {} : {}'.format(
                            col_name,
                            ','.join(
                                '{}: {}'.format(c, row[c]) for c in invalid_df.schema.names[:-2]
                            ), location.replace('root', col_name), diff_type
                        )
                    )
                    messages.append(pprint.pformat(mismatches))

        invalid_df.unpersist()

    return all_values_match, messages
