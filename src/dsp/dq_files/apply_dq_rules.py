from typing import Iterable, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import when, array, col, count, size, lit, struct, \
    current_timestamp, create_map, concat, coalesce  # pylint: disable=no-name-in-module
from pyspark.sql.types import ArrayType, StringType

from dsp.common import canonical_name
from nhs_reusable_code_library.resuable_codes.spark_helpers import empty_array
from dsp.dam.dq_errors import DQErrs
from dsp.datasets.common import DQRule, Fields as Common, DQMessageType, UniquenessRule
from dsp.dq_files.dq.schema import DQ_MESSAGE_SCHEMA
from dsp.dq_files.output import Fields as DQFields
from dsp.shared.logger import log_action


@log_action(log_args=['dq_rules'])
def data_dq(df: DataFrame, dq_rules: Iterable[DQRule], dq_field_values: bool = False,
            dq_message_meta_fields: list = None) -> Tuple[DataFrame, DataFrame, DataFrame]:
    df = df if Common.DQ_ERR in df.schema.names else df.withColumn(Common.DQ_ERR, empty_array(DQ_MESSAGE_SCHEMA))

    df = df if Common.DQ_WARN in df.schema.names else df.withColumn(Common.DQ_WARN, empty_array(DQ_MESSAGE_SCHEMA))

    def prepare_rules(errors: bool):
        prepared = []
        for rule in dq_rules:
            if not rule.is_error == errors:
                continue
            names = [canonical_name(n) for n in rule.fields]
            err_message = rule.code.label.format(*names)
            err_type = DQMessageType.Error if errors else DQMessageType.Warning

            # Reject any error to retain backwards compatibility with older datasets
            action = 'reject_file' if errors else None

            if dq_field_values:
                fields_ext = rule.fields.copy()
            else:
                fields_ext = []

            if dq_message_meta_fields:
                fields_ext.extend(dq_message_meta_fields)
                fields_ext = list(set(fields_ext))

            message = struct(
                lit(None).alias(DQFields.TABLE),
                col(Common.META + "." + Common.RECORD_INDEX).astype(StringType()).alias(DQFields.ROW_INDEX),
                lit(canonical_name(rule.attribute)).alias(DQFields.ATTRIBUTE),
                array(*[lit(n) for n in names]).alias(DQFields.FIELDS),
                lit(rule.code.value).alias(DQFields.CODE),
                lit(err_message).alias(DQFields.MESSAGE),
                create_map([column
                            for field in fields_ext
                            for column in (lit(field), coalesce(df[field], lit('')).astype(StringType()))]).alias(
                    DQFields.FIELD_VALUES),

                lit(err_type).alias(DQFields.TYPE),
                lit(action).alias(DQFields.ACTION)
            )

            prepared.append((rule.condition(*names), message))

        return prepared

    error_rules = prepare_rules(True)
    warning_rules = prepare_rules(False)


    df_dq_errs = df if not error_rules else df.withColumn(
        Common.DQ_ERR,
        concat(
            col(Common.DQ_ERR),
            *(when(~c, array(m)).otherwise(empty_array(DQ_MESSAGE_SCHEMA)) for c, m in error_rules),
        )
    )

    df_dq = df_dq_errs if not warning_rules else df_dq_errs.withColumn(
        Common.DQ_WARN,
        concat(
            col(Common.DQ_WARN),
            *(when(~c, array(m)).otherwise(empty_array(DQ_MESSAGE_SCHEMA)) for c, m in warning_rules),
        )
    )

    df_dq = df_dq.withColumn(
        Common.DQ, concat(col(Common.DQ_ERR), col(Common.DQ_WARN))
    ).withColumn(Common.DQ_TS, current_timestamp())

    dq_errs_df = df_dq.where(size(col(Common.DQ_ERR)) > 0).select([Common.META, Common.DQ, Common.DQ_TS])

    dq_warns_df = df_dq.where(
        (size(col(Common.DQ_ERR)) == 0) & (size(col(Common.DQ_WARN)) > 0)
    ).select([Common.META, Common.DQ, Common.DQ_TS])

    dq_pass_df = df_dq.where(size(col(Common.DQ_ERR)) == 0)

    return dq_errs_df, dq_warns_df, dq_pass_df.drop(Common.DQ).drop(Common.DQ_WARN).drop(Common.DQ_ERR).drop(
        Common.DQ_TS
    )


@log_action(log_args=['unique_rules'])
def uniqueness_dq(df_dq: DataFrame, unique_rules: Iterable[UniquenessRule] = None, dq_field_values: bool = False) \
        -> Tuple[DataFrame, DataFrame]:
    if unique_rules:
        unique_columns, dq_code = unique_rules.unique_columns, unique_rules.code
    else:
        dq_code = DQErrs.DQ_NOT_UNIQUE

        # pylint: disable=E1101
        dq_error = struct(
            lit(None).alias(DQFields.TABLE),
            col(Common.META + "." + Common.RECORD_INDEX).astype(StringType()).alias(DQFields.ROW_INDEX),
            lit('FAKE').alias(DQFields.ATTRIBUTE),
            array(*[lit('FAKE')]).alias(DQFields.FIELDS),
            lit(dq_code.value).alias(DQFields.CODE),
            lit(dq_code.label.format('FAKE')).alias(DQFields.MESSAGE),
            create_map().alias(DQFields.FIELD_VALUES),
            lit(DQMessageType.Error).alias(DQFields.TYPE),
            lit('reject_file').alias(DQFields.ACTION),
        )

        df_dq_errs = df_dq.where(
            lit(False) == lit(True)
        ).withColumn(Common.DQ, array([dq_error])).withColumn(
            Common.DQ_TS, current_timestamp()
        ).withColumn(
            Common.DQ,
            col(Common.DQ).cast(ArrayType(DQ_MESSAGE_SCHEMA))
        ).select([Common.META, Common.DQ, Common.DQ_TS])
        return df_dq_errs, df_dq

    partition_window = Window.partitionBy(*unique_columns)

    # Remove nulls from uniqueness constraint
    qry1 = " and ".join(a for a in [f"{c} is not null" for c in unique_columns])
    df_dq_temp = df_dq.where(qry1)

    df_dq_temp = df_dq_temp.withColumn(
        Common.DQ_UNIQ,
        count('*').over(partition_window)
    )

    df_dq_temp.cache()

    if dq_field_values:
        field_value_map = create_map([column
                    for field in unique_columns
                    for column in (lit(field), df_dq[field].astype(StringType()))])
    else:
        field_value_map = create_map()

    # pylint: disable=E1101
    dq_error = struct(
        lit(None).alias(DQFields.TABLE),
        col(Common.META + "." + Common.RECORD_INDEX).astype(StringType()).alias(DQFields.ROW_INDEX),
        lit(unique_columns[0]).alias(DQFields.ATTRIBUTE),
        array(*[lit(n) for n in unique_columns]).alias(DQFields.FIELDS),
        lit(dq_code.value).alias(DQFields.CODE),
        lit(dq_code.label.format(*unique_columns)).alias(DQFields.MESSAGE),
        field_value_map.alias(DQFields.FIELD_VALUES),
        lit(DQMessageType.Error).alias(DQFields.TYPE),
        lit('reject_file').alias(DQFields.ACTION),
    )

    dq_errs_df = df_dq_temp.where(col(Common.DQ_UNIQ) > 1).withColumn(
        Common.DQ, array([dq_error])
    ).withColumn(
        Common.DQ,
        col(Common.DQ).cast(ArrayType(DQ_MESSAGE_SCHEMA))
    ).withColumn(
        Common.DQ_TS,
        current_timestamp()
    ).select([Common.META, Common.DQ, Common.DQ_TS])

    df_dq_temp.unpersist()

    return dq_errs_df, df_dq
