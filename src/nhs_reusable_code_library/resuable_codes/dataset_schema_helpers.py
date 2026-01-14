from typing import Dict, List
from pyspark.sql import SparkSession


def get_cols(spark: SparkSession, schema: str, table: str, ignore_columns: List[str]) -> List[str]:
    """
    retrieves list of columns from the table , columns present in ignore list will be excluded.
    :param spark: SparkSession object of current execution context
    :param schema: database name
    :param table: table for which column information is required
    :param ignore_columns: columns to be ignored
    :return: list of columns from table
    """
    cols = [c[0] for c in spark.sql(f'show columns from {schema}.{table}').collect() if c[0] not in ignore_columns]
    cols.sort()
    return cols


def get_sql_select_str_with_col_mappings(table_columns: iter, all_columns: iter, column_map: Dict[str, str]) -> str:
    """
    Args:
        table_columns: Metadata containing the column information for the table we're selecting from
        all_columns: Metadata containing the column information for all tables in the dataset schema versions
        we're mapping for.
        column_map: A dictionary containing original column names to be mapped (key), and what they will be
        renamed to (value).

    Returns:
        A string containing column information to be used in a SQL SELECT, for use in view creation statements
        involving union of one or more tables. Includes handling for columns renamed AS something
        else, and NULL AS columns which exist in one schema version but not in the next.
    """
    mapped_columns = []
    for col in all_columns:
        if col in table_columns and col in column_map:
            mapped_columns.append(f'{col} as {column_map[col]}')
        elif col in table_columns:
            mapped_columns.append(col)
        elif col in column_map and column_map[col] not in table_columns:
            mapped_columns.append(f'null as {column_map[col]}')
        elif col not in column_map and col not in column_map.values():
            mapped_columns.append(f'null as {col}')
    sql_select_str = ', '.join(mapped_columns)
    return sql_select_str


def create_view(spark: SparkSession, table: str, legacy_schema_table_meta: Dict[str, str],
                current_schema_table_meta: Dict[str, str], legacy_schema: str, current_schema: str, target_db: str):
    """
    creates a combined view of legacy and current database tables.

    :param spark: SparkSession object of current execution context
    :param table:
    :param legacy_schema_table_meta:
    :param current_schema_table_meta:
    :param legacy_schema: name of legacy schema
    :param current_schema: name of current version schema
    :param target_db: target database name where view will be created
    :return: True if successful.
    """
    in_legacy = table in legacy_schema_table_meta
    in_current = table in current_schema_table_meta
    in_both = in_legacy and in_current

    if not in_both:
        cols = ', '.join(legacy_schema_table_meta[table] if in_legacy else current_schema_table_meta[table])
        schema = legacy_schema if in_legacy else current_schema
        spark.sql(
            f"""
      CREATE OR REPLACE VIEW {target_db}.{table}
      AS SELECT {cols} FROM {schema}.{table}
          """
        )
        return True

    all_cols = list(set(current_schema_table_meta[table]) | set(legacy_schema_table_meta[table]))
    all_cols.sort()

    v3_cols_map = ', '.join(
        [col if col in legacy_schema_table_meta[table] else "null as {}".format(col) for col in all_cols])
    v4_cols_map = ', '.join(
        [col if col in current_schema_table_meta[table] else "null as {}".format(col) for col in all_cols])

    spark.sql(
        f"""
  CREATE OR REPLACE VIEW {target_db}.{table}
  AS 
  SELECT {v4_cols_map} FROM {current_schema}.{table}
  UNION ALL
  SELECT {v3_cols_map} FROM {legacy_schema}.{table}
      """
    )
    return True


def create_pseudo_sensitive_view(spark: SparkSession, table: str, domain1_pseudo_fields: List[str],
                                 hash_fields: List[str],
                                 map_hash_fields: Dict[str, str], deid_type: str, clear_fields: List[str], redact_fields: List[str], source_db: str, target_db: str, pseudo_db: str):
    """
    creates a view in target database, if column is part of domain1_pseudo_fields then column will be fetched from
    pseudo.pseudo table, if column is present in hash_fields then column will be fetched from pseudo.salt table
    otherwise from source db.

    :param pseudo_db: name of pseudo database
    :param spark: SparkSession object of current execution context
    :param table: taable name for which view needs to be created
    :param domain1_pseudo_fields: field names which should be tokenized with privatar
    :param hash_fields: list of columns to be hashed
    :param map_hash_fields: dict of column mapping with pseudo.salt table
    :param deid_type: type of deid view
    :param clear_fields: columns to be displayed in clear
    :param redact_fields: columns to be excluded from the view
    :param source_db: source database name
    :param target_db: target database where views will be created
    """
    joins = []

    def get_col(col):

        if col in domain1_pseudo_fields:
            join_num = len(joins)
            joins.append(f"LEFT JOIN {pseudo_db}.pseudo p{join_num} ON p{join_num}.domain = 'Domain-1' "
                         f"AND p{join_num}.pseudo_type = '{col}' AND lhs.{col} = p{join_num}.clear")

            return f"p{join_num}.pseudo AS {col}"

        if deid_type == 'pseudo' and col in hash_fields:
            hash_field = map_hash_fields.get(col) or col
            salt = f"SELECT salt FROM {pseudo_db}.salt WHERE field = '{hash_field}' AND domain IS NULL"
            return f"sha2(concat({col}, ({salt})), 256) as {col}"

        assert col in (clear_fields + hash_fields + domain1_pseudo_fields), f'{col} not present in any list'
        return col

    columns = [c[0] for c in
               spark.sql("show columns from {source_db}.{table}".format(source_db=source_db, table=table)).collect() if
               c[0] not in redact_fields]

    columns = ', '.join(get_col(col) for col in columns)
    joins = '\n'.join(joins)
    view = f"""
CREATE OR REPLACE VIEW {target_db}.{table}
AS SELECT {columns}
FROM {source_db}.{table} lhs
{joins}
"""
    spark.sql(view)
