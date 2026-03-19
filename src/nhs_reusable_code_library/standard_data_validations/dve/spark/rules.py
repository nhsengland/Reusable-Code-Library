"""Step implementations in Spark."""

from collections.abc import Callable
from typing import Optional
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, lit

from dve.core_engine.backends.base.rules import BaseStepImplementations
from dve.core_engine.backends.exceptions import ConstraintError
from dve.core_engine.backends.implementations.spark.spark_helpers import (
    create_udf,
    get_all_registered_udfs,
    object_to_spark_literal,
    spark_read_parquet,
    spark_write_parquet,
)
from dve.core_engine.backends.implementations.spark.types import (
    Joined,
    Source,
    SparkEntities,
    Target,
)
from dve.core_engine.backends.implementations.spark.utilities import parse_multiple_expressions
from dve.core_engine.backends.metadata.rules import (
    AbstractConditionalJoin,
    AbstractNewColumnConditionalJoin,
    Aggregation,
    AntiJoin,
    ColumnAddition,
    ColumnRemoval,
    ConfirmJoinHasMatch,
    HeaderJoin,
    ImmediateFilter,
    InnerJoin,
    LeftJoin,
    Notification,
    OneToOneJoin,
    OrphanIdentification,
    SelectColumns,
    SemiJoin,
    TableUnion,
)
from dve.core_engine.constants import ROWID_COLUMN_NAME
from dve.core_engine.functions import implementations as functions
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.templating import template_object
from dve.core_engine.type_hints import Messages


@spark_write_parquet
@spark_read_parquet
class SparkStepImplementations(BaseStepImplementations[DataFrame]):
    """An implementation of transformation steps in Apache Spark."""

    def __init__(self, spark_session: Optional[SparkSession] = None, **kwargs):
        self._spark_session = spark_session
        self._registered_functions: list[str] = []
        super().__init__(**kwargs)

    @property
    def spark_session(self) -> SparkSession:
        """The current spark session"""
        if not self._spark_session:
            self._spark_session = SparkSession.builder.getOrCreate()
        return self._spark_session

    @property
    def registered_functions(self):
        """Set of all registered functions"""
        if not self._registered_functions:
            self._registered_functions = get_all_registered_udfs(self.spark_session)
        return self._registered_functions

    @classmethod
    def register_udfs(
        cls, spark_session: Optional[SparkSession] = None, **kwargs
    ):  # pylint: disable=arguments-differ
        """Register all function implementations as Spark UDFs."""
        spark_session = spark_session or SparkSession.builder.getOrCreate()

        _registered_functions: set[str] = get_all_registered_udfs(spark_session)
        _available_functions: dict[str, Callable] = {
            func_name: func
            for func_name, func in vars(functions).items()
            if callable(func) and func.__module__ == "dve.core_engine.functions.implementations"
        }

        _unregistered_functions: set[str] = set(_available_functions).difference(
            _registered_functions
        )

        for function_name in _unregistered_functions:
            func = _available_functions.get(function_name)

            udf_func = create_udf(func)  # type: ignore
            spark_session.udf.register(function_name, udf_func)

        return cls(spark_session=spark_session, **kwargs)

    @staticmethod
    def add_row_id(entity: DataFrame) -> DataFrame:
        if ROWID_COLUMN_NAME not in entity.columns:
            entity = entity.withColumn(ROWID_COLUMN_NAME, sf.expr("uuid()"))
        return entity

    @staticmethod
    def drop_row_id(entity: DataFrame) -> DataFrame:
        if ROWID_COLUMN_NAME in entity.columns:
            entity = entity.drop(ROWID_COLUMN_NAME)
        return entity

    def add(self, entities: SparkEntities, *, config: ColumnAddition) -> Messages:
        entity: DataFrame = entities[config.entity_name]
        entity = entity.withColumn(config.column_name, sf.expr(config.expression))
        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def remove(self, entities: SparkEntities, *, config: ColumnRemoval) -> Messages:
        entity: DataFrame = entities[config.entity_name]
        entity = entity.drop(config.column_name)
        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def select(self, entities: SparkEntities, *, config: SelectColumns) -> Messages:
        entity: DataFrame = entities[config.entity_name]
        entity = entity.select(*parse_multiple_expressions(config.columns))
        if config.distinct:
            entity = entity.distinct()
        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def group_by(self, entities: SparkEntities, *, config: Aggregation) -> Messages:
        entity: DataFrame = entities[config.entity_name]

        group_cols = parse_multiple_expressions(config.group_by)
        agg_cols = parse_multiple_expressions(config.agg_columns)  # type: ignore

        group = entity.groupBy(*group_cols)
        if config.pivot_column:
            group = group.pivot(config.pivot_column, config.pivot_values)

        if agg_cols:
            entity = group.agg(*agg_cols)
        else:
            entity = group.agg({})

        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def _perform_join(
        self, entities: SparkEntities, config: AbstractConditionalJoin
    ) -> tuple[Source, Target, Joined]:
        """Perform a conditional join between source and target, returning the
        source, target and joined DataFrames.

        """
        source_df: DataFrame = entities[config.entity_name]
        source_df = source_df.alias(config.entity_name)
        target_df: DataFrame = entities[config.target_name]
        target_df = target_df.alias(config.target_name)

        if isinstance(config, InnerJoin):
            join_type = "inner"
        elif isinstance(config, SemiJoin):
            join_type = "left_semi"
        elif isinstance(config, AntiJoin):
            join_type = "left_anti"
        else:
            join_type = "left"

        joined_df = source_df.join(target_df, on=sf.expr(config.join_condition), how=join_type)
        return source_df, target_df, joined_df

    def _resolve_join_name_conflicts(
        self, source_df: Source, joined_df: Joined, config: AbstractNewColumnConditionalJoin
    ) -> Joined:
        """Resolve name conflicts in joined DataFrames."""
        # Need to ensure we keep source columns but these can be overridden by
        # new computed keys.
        # Start with the original column names, add the new computed names.
        columns = parse_multiple_expressions(
            [f"{config.entity_name}.{column_name}" for column_name in source_df.columns]
        )
        columns.extend(parse_multiple_expressions(config.new_columns))
        # Select them from the join. There may be duplicates here for overridden fields.
        result_all_cols = joined_df.select(*columns)

        # Now need to handle the existence of dupes.
        # Use toDF to rename columns with some temp names so we can distinguish them,
        # then keep the last-specified.
        # Need to be careful with case sensitivity - Spark doesn't care.
        temp_column_names = []
        concrete_to_temp_mapping = {}
        case_mapping = {}
        for index, column_name in enumerate(result_all_cols.columns):
            temp_name = str(index)

            # Handle Spark's annoying case insensitivity by keeping track
            # of latest case preference.
            uppercase_name = column_name.upper()
            case_mapping[uppercase_name] = column_name

            # Store the temp name, and the mapping between uppercase name and temp name.
            temp_column_names.append(temp_name)
            concrete_to_temp_mapping[uppercase_name] = temp_name

        # Rename with the indices, so we can deduplicate column names.
        result_temp_names = result_all_cols.toDF(*temp_column_names)
        # Keep only the latest column for each (case insensitive) column name.
        latest_temp_names = result_temp_names.select(*concrete_to_temp_mapping.values())
        # Rename those fields to their 'proper' names (respect user-supplied case).
        return latest_temp_names.toDF(
            *[case_mapping[uppercase_name] for uppercase_name in concrete_to_temp_mapping]
        )

    def has_match(self, entities: SparkEntities, *, config: ConfirmJoinHasMatch) -> Messages:
        source_df, _, joined_df = self._perform_join(entities, config)
        entity = joined_df.select(
            f"{config.entity_name}.*",
            sf.coalesce(sf.expr(config.join_condition), lit(False)).alias(config.column_name),
        )

        if config.perform_integrity_check:
            if joined_df.count() != source_df.count():
                raise ConstraintError(
                    f"Multiple matches for some records from {config.entity_name!r} for "
                    + f"condition {config.join_condition!r}",
                    constraint=(
                        f"records in source entity ({config.entity_name!r}) must match at most "
                        + f"a single record in the target ({config.target_name})"
                    ),
                )

        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def left_join(self, entities: SparkEntities, *, config: LeftJoin) -> Messages:
        source_df, _, joined_df = self._perform_join(entities, config)
        entities[config.new_entity_name or config.entity_name] = self._resolve_join_name_conflicts(
            source_df, joined_df, config
        )
        return []

    def inner_join(self, entities: SparkEntities, *, config: InnerJoin) -> Messages:
        source_df, _, joined_df = self._perform_join(entities, config)
        entities[config.new_entity_name or config.entity_name] = self._resolve_join_name_conflicts(
            source_df, joined_df, config
        )
        return []

    def one_to_one_join(self, entities: SparkEntities, *, config: OneToOneJoin) -> Messages:
        source_df: DataFrame = entities[config.entity_name]
        messages = self.left_join(entities, config=config)
        joined_df: DataFrame = entities[config.new_entity_name or config.entity_name]

        if config.perform_integrity_check:
            if joined_df.count() != source_df.count():
                raise ConstraintError(
                    f"Multiple matches for some records from {config.entity_name!r} for "
                    + f"condition {config.join_condition!r}",
                    constraint=(
                        f"records in source entity ({config.entity_name!r}) must match at most "
                        + f"a single record in the target ({config.target_name})"
                    ),
                )
        return messages

    def anti_join(self, entities: SparkEntities, *, config: AntiJoin) -> Messages:
        _, _, joined_df = self._perform_join(entities, config)
        entities[config.new_entity_name or config.entity_name] = joined_df
        return []

    def semi_join(self, entities: SparkEntities, *, config: SemiJoin) -> Messages:
        _, _, joined_df = self._perform_join(entities, config)
        entities[config.new_entity_name or config.entity_name] = joined_df
        return []

    def join_header(self, entities: SparkEntities, *, config: HeaderJoin) -> Messages:
        source_df: DataFrame = entities[config.entity_name]
        source_df = source_df.alias(config.entity_name)
        target_df: DataFrame = entities[config.target_name]
        target_df = target_df.alias(config.target_name)

        target_rows = target_df.collect()
        n_target_rows = len(target_rows)
        if n_target_rows != 1:
            raise ConstraintError(
                f"Unable to join header {config.target_name!r} to {config.entity_name!r} "
                + f"as it contains multiple entries (expected 1, got {n_target_rows})",
                constraint=(
                    f"Header entity {config.target_name!r} must contain a single record "
                    + f"(contains {n_target_rows} records)"
                ),
            )

        header_row_dict = target_rows[0].asDict(recursive=True)
        joined_df = source_df.withColumn(
            config.header_column_name,
            object_to_spark_literal(header_row_dict).cast(target_df.schema),
        )

        entities[config.new_entity_name or config.entity_name] = joined_df
        return []

    def union(self, entities: SparkEntities, *, config: TableUnion) -> Messages:
        source_df: DataFrame = entities[config.entity_name]
        source_df = source_df.alias(config.entity_name)
        target_df: DataFrame = entities[config.target_name]
        target_df = target_df.alias(config.target_name)

        # Ensure all keys are present in both
        source_names = {column_name.upper(): column_name for column_name in source_df.columns}
        target_names = {column_name.upper(): column_name for column_name in target_df.columns}

        all_names = list(source_names.keys())
        for name in target_names:
            if name not in source_names:
                all_names.append(name)

        source_columns, target_columns = [], []
        for uppercase_name in all_names:
            source_name = source_names.get(uppercase_name)
            target_name = target_names.get(uppercase_name)

            if source_name and target_name:
                source_col = col(source_name)
                target_col = col(target_name).alias(source_name)
            elif source_name:
                source_col = col(source_name)
                target_col = lit(None).alias(source_name)
            elif target_name:
                source_col = lit(None).alias(target_name)
                target_col = col(target_name)
            else:
                continue

            source_columns.append(source_col)
            target_columns.append(target_col)

        source_df = source_df.select(*source_columns)
        target_df = target_df.select(*target_columns)
        entities[config.new_entity_name or config.entity_name] = source_df.union(target_df)
        return []

    def identify_orphans(
        self, entities: SparkEntities, *, config: OrphanIdentification
    ) -> Messages:
        source_df: DataFrame = entities[config.entity_name]
        source_df = source_df.alias(config.entity_name)
        target_df: DataFrame = entities[config.target_name]
        target_df = target_df.alias(config.target_name)

        key_name = f"key_{uuid4().hex}"
        source_df = source_df.withColumn(key_name, sf.expr("uuid()")).alias(config.entity_name)
        match_name = f"matched_{uuid4().hex}"
        target_df = target_df.withColumn(match_name, lit(1)).alias(config.target_name)

        joined_df = (
            source_df.join(target_df, on=sf.expr(config.join_condition), how="left")
            .groupBy(col(key_name))
            .agg(sf.coalesce(sf.sum(col(match_name)) == lit(0), lit(True)).alias("IsOrphaned"))
        )

        if "IsOrphaned" not in source_df.columns:
            result = source_df.join(joined_df, on=[key_name], how="left").drop(key_name)
        else:
            result = source_df.alias("source").join(
                joined_df.alias("joined"),
                on=col(f"source.{key_name}") == col(f"joined.{key_name}"),
                how="left",
            )

            columns = {name: col(f"source.{name}") for name in source_df.columns}
            columns["IsOrphaned"] = col("source.IsOrphaned") | col("joined.IsOrphaned")
            columns.pop(key_name, None)

            result = result.select(*[column.alias(name) for name, column in columns.items()])

        entities[config.new_entity_name or config.entity_name] = result
        return []

    def filter(self, entities: SparkEntities, *, config: ImmediateFilter) -> Messages:
        """Filter an entity immediately, and do not emit any messages.

        The synchronised filter stage will be implemented separately.
        """
        entity = entities[config.entity_name]
        entity = entity.where(sf.expr(config.expression))
        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def notify(self, entities: SparkEntities, *, config: Notification) -> Messages:
        """Emit a notification based on an expression. Where the expression is truthy,
        a nofication should be emitted according to the reporting config.

        This is not intended to be used directly, but is used in the
        implementation of the sync filters.
        """
        messages: Messages = []
        entity = entities[config.entity_name]

        matched = entity.filter(config.expression)
        if config.excluded_columns:
            matched = matched.drop(*config.excluded_columns)

        for record in matched.toLocalIterator():
            messages.append(
                # NOTE: only templates using values directly accessible in record - nothing nested
                # more complex extraction done in reporting module
                FeedbackMessage(
                    entity=config.reporting.reporting_entity_override or config.entity_name,
                    record=record.asDict(recursive=True),
                    error_location=config.reporting.legacy_location,
                    error_message=template_object(
                        config.reporting.message, record.asDict(recursive=True)
                    ),
                    failure_type=config.reporting.legacy_error_type,
                    error_type=config.reporting.legacy_error_type,
                    error_code=config.reporting.code,
                    reporting_field=config.reporting.legacy_reporting_field,
                    reporting_field_name=config.reporting.reporting_field_override,
                    is_informational=config.reporting.emit in ("warning", "info"),
                    category=config.reporting.category,
                )
            )
        return messages
