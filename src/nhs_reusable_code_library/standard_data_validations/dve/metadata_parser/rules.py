"""Metadata classes for rule steps."""

import warnings
from abc import ABCMeta, abstractmethod
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Optional, TypeVar, Union

from pydantic import BaseModel, Extra, Field, root_validator, validate_arguments, validator
from typing_extensions import Literal

from nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.reference_data import ReferenceConfigUnion
from nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.reporting import LegacyReportingConfig, ReportingConfig
from nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.templating import template_object
from nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.type_hints import (
    Alias,
    EntityName,
    Expression,
    JSONable,
    MultipleExpressions,
    TemplateVariables,
)

__all__ = [
    "AbstractStep",
    "Aggregation",
    "AntiJoin",
    "BaseStep",
    "ColumnAddition",
    "ColumnRemoval",
    "ConfirmJoinHasMatch",
    "CopyEntity",
    "DeferredFilter",
    "EntityRemoval",
    "HeaderJoin",
    "ImmediateFilter",
    "InnerJoin",
    "LeftJoin",
    "OneToOneJoin",
    "OneToOneJoin",
    "OrphanIdentification",
    "ParentMetadata",
    "RenameEntity",
    "Rule",
    "RuleMetadata",
    "SelectColumns",
    "SemiJoin",
    "TableUnion",
]

ASSelf = TypeVar("ASSelf", bound="AbstractStep")
RSelf = TypeVar("RSelf", bound="Rule")

Stage = Literal["Pre-sync", "Sync", "Post-sync"]
"""The stage that the rule falls under."""


# pylint: disable=too-few-public-methods
class ParentMetadata(BaseModel):
    """Data about the parent rule for a step."""

    rule: Union["Rule", str]
    """The rule the step belongs to, or a string representing the rule."""
    index: int
    """The index of the step within the parent rule."""
    stage: Stage
    """The name of the stage in the rule the step belongs to."""

    def __repr__(self) -> str:
        components: list[str] = []
        if isinstance(self.rule, Rule):
            components.append(f"rule=Rule(name={self.rule.name!r}, ...)")
        else:
            components.append(f"rule=Rule(name={self.rule!r}, ...)")

        for key, value in self.dict(exclude={"rule"}).items():
            components.append(f"{key}={value!r}")

        return f"ParentMetadata({', '.join(components)})"

    class Config:  # pylint: disable=too-few-public-methods
        """`pydantic configuration options`"""

        frozen = True
        extra = Extra.forbid


# pylint: disable=too-few-public-methods
class AbstractStep(BaseModel, metaclass=ABCMeta):
    """An abstract transformation step."""

    id: Optional[str] = None
    """An ID for the rule step. This will not be templated."""
    description: Optional[str] = None
    """An optional description for the step."""
    parent: Optional[ParentMetadata] = None
    """Data about the parent rule and the step's place within it."""

    UNTEMPLATED_KEYS: ClassVar[set[str]] = {"id", "description", "parent"}
    """A set of aliases which are exempted from templating."""

    class Config:  # pylint: disable=too-few-public-methods
        """`pydantic configuration options`"""

        frozen = True
        extra = Extra.forbid

    def __repr_args__(self) -> Sequence[tuple[Optional[str], Any]]:
        # Exclude nulls from 'repr' for conciseness.
        return [(key, value) for key, value in super().__repr_args__() if value is not None]

    @abstractmethod
    def get_required_entities(self) -> set[EntityName]:
        """Get a set of the required entity names for the transformation."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_created_entities(self) -> set[EntityName]:
        """Get a set of the entity names created by the transformation."""
        raise NotImplementedError()  # pragma: no cover

    def get_removed_entities(self) -> set[EntityName]:
        """Get a set of the entity names removed by the transformation."""
        return set()

    def with_parent(self: ASSelf, parent: ParentMetadata) -> ASSelf:
        """Return a new step with different parent metadata."""
        return type(self)(**self.dict(exclude={"parent"}), parent=parent)

    def template(
        self: ASSelf,
        local_variables: TemplateVariables,
        *,
        global_variables: Optional[TemplateVariables] = None,
    ) -> ASSelf:
        """Template the rule, given the global and local variables."""
        type_ = type(self)
        if global_variables:
            variables = global_variables.copy()
            variables.update(local_variables)
        else:
            variables = local_variables

        untemplated_data = {key: getattr(self, key) for key in self.UNTEMPLATED_KEYS}
        data_to_template = self.dict(exclude=self.UNTEMPLATED_KEYS)
        templated_data = template_object(data_to_template, variables, "jinja")

        return type_(**templated_data, **untemplated_data)

    def __str__(self):  # pydantic's default __str__ strips the model name.
        return super().__repr__()

    @root_validator(pre=True)
    @classmethod
    def _warn_for_deprecated_aliases(cls, values: dict[str, JSONable]) -> dict[str, JSONable]:
        for deprecated_name, replacement in (
            ("entity", "entity_name"),
            ("target", "target_entity_name"),
        ):
            if deprecated_name in values:
                warnings.warn(
                    f"Using deprecated name {deprecated_name!r}, should be {replacement!r}"
                )
                if replacement in values:
                    raise ValueError(
                        f"{replacement!r} and deprecated name {deprecated_name!r} both "
                        + "provided"
                    )
                values[replacement] = values.pop(deprecated_name)
        return values


class BaseStep(AbstractStep, metaclass=ABCMeta):
    """A base transformation step which performs a transformation on an entity
    and provides for a new entity being created instead of modification.

    """

    entity_name: EntityName
    """The entity to apply the step to."""
    new_entity_name: Optional[EntityName] = None
    """Optionally, a new entity to create after the operation."""

    def get_required_entities(self) -> set[EntityName]:
        """Get a set of the required entity names for the transformation."""
        return {self.entity_name}

    def get_created_entities(self) -> set[EntityName]:
        """Get a set of the entity names created by the transformation."""
        return {self.new_entity_name or self.entity_name}


class ImmediateFilter(BaseStep):
    """A filter which removes records from an entity (or creates a new entity without
    those rows) according to a provided expression. Only records for which the
    provided expression is truthy will be retained.

    """

    expression: Expression
    """
    The expression for the filter. Records for which this expression is falsey will be
    removed.

    """


class DeferredFilter(AbstractStep):
    """A filter which should be applied in a synchronised step with other filters for
    the same entity, providing feedback to users.

    Messages will be  emitted for each record that fails a filter according to the
    configured reporting, and any record-level errors will be removed from source
    entities at the end of the stage. This enables multiple issues with the same record
    to be flagged to a data provider.

    """

    entity_name: EntityName
    """The entity to apply the step to."""
    expression: Expression
    """
    The expression for the filter. Records for which this expression is falsey will be
    removed from the source entity if the reporting level is a record-level error.

    """
    reporting: Union[ReportingConfig, LegacyReportingConfig]
    """The reporting information for the filter."""

    def template(
        self: "DeferredFilter",
        local_variables: dict[Alias, Any],
        *,
        global_variables: Optional[dict[Alias, Any]] = None,
    ) -> "DeferredFilter":
        """Template the rule, given the global and local variables."""
        type_ = type(self)
        if global_variables:
            variables = global_variables.copy()
            variables.update(local_variables)
        else:
            variables = local_variables

        untemplated_data = {key: getattr(self, key) for key in self.UNTEMPLATED_KEYS}
        data_to_template = self.dict(exclude={*self.UNTEMPLATED_KEYS, "reporting"})
        templated_data = template_object(data_to_template, variables, "jinja")
        templated_data["reporting"] = self.reporting.template(
            local_variables, global_variables=global_variables
        )

        return type_(**templated_data, **untemplated_data)

    def get_required_entities(self) -> set[EntityName]:
        """Get a set of the required entity names for the transformation."""
        return {self.entity_name}

    def get_created_entities(self) -> set[EntityName]:
        """Get a set of the required entity names for the transformation."""
        return {self.entity_name}


class Notification(AbstractStep):
    """An implementation of a notification according to a particular reporting configuration.
    This will emit notifications for all records that meet the expression, and will
    not modify the source entity.

    NOTE: This is not intended to be used directly, but is used in the implementation
    of the sync filter stage.

    """

    entity_name: EntityName
    """The entity to apply the step to."""
    expression: Expression
    """
    The expression for the notification. Records for which this expression is truthy will
    emit notifications according to the reporting config.

    """
    excluded_columns: list[Alias] = Field(default_factory=list)
    """Columns to be excluded from the record in the report."""
    reporting: ReportingConfig
    """The reporting information for the filter."""

    def get_required_entities(self) -> set[EntityName]:
        return {self.entity_name}

    def get_created_entities(self) -> set[EntityName]:
        return set()


class AbstractJoin(AbstractStep, metaclass=ABCMeta):
    """An abstract table join configuration. This joins `target` into `entity`."""

    entity_name: EntityName
    """The name of the source entity to join from."""
    target_name: EntityName
    """The target dataset to join to."""
    new_entity_name: Optional[EntityName] = None
    """Optionally, a new entity to create after the operation."""

    def get_required_entities(self) -> set[EntityName]:
        return {self.entity_name, self.target_name}

    def get_created_entities(self) -> set[EntityName]:
        return {self.new_entity_name or self.entity_name}


class AbstractConditionalJoin(AbstractJoin, metaclass=ABCMeta):
    """An abstract table join configuration with a join condition.
    This joins `target` into `entity`.

    """

    join_condition: Expression
    """
    A SQL expression representing the join. Columns will be namespaced
    within the source and target dataset names.

    """

    JOIN_TYPE: ClassVar[str]
    """The type of join that the class uses in Spark."""


class AbstractNewColumnConditionalJoin(AbstractConditionalJoin, metaclass=ABCMeta):
    """A join type which adds new columns to the finished entity."""

    new_columns: MultipleExpressions = Field(default_factory=dict)
    """
    A mapping of SQL expressions to the names of the new columns to be added
    to `entity`.

    Expressions can access columns from the source and the target, and
    columns will be namespaced within the source and target dataset names.

    """


class ColumnAddition(BaseStep):
    """A transformation step which adds a new column based on an expression."""

    column_name: Alias
    """The name of the column to be created."""
    expression: Expression
    """The SQL expression for the transformation."""


class ColumnRemoval(BaseStep):
    """A transformation step which removes a column by name."""

    column_name: Alias
    """The name of the column to be dropped."""


class SelectColumns(BaseStep):
    """A transformation step which selects columns from an entity."""

    columns: MultipleExpressions
    """Multiple expressions to select from the specified entity."""
    distinct: bool = False
    """Whether to keep only distinct columns from the select statement."""


class Aggregation(BaseStep):
    """A transformation which performs aggregation."""

    group_by: MultipleExpressions
    """Multiple expressions to group by."""
    pivot_column: Optional[Alias] = None
    """An optional pivot column for the table."""
    pivot_values: Optional[list[Any]] = None
    """A list of values to translate to columns when pivoting."""
    agg_columns: Optional[MultipleExpressions] = None
    """Multiple aggregate expressions to take from the group by (for spark backend)"""
    agg_function: Optional[Alias] = None
    """The aggregate function to apply to the agg_columns (for duckdb backend)"""

    @validator("pivot_values")
    @classmethod
    def _ensure_column_if_values(
        cls,
        value: Optional[Any],
        values: dict[str, Any],
    ):
        """Ensure that `pivot_column` is not null if pivot values are provided."""
        if value and not values["pivot_column"]:
            raise ValueError("`pivot_values` specified, but no `pivot_column`")
        return value

    @validator("agg_function")
    @classmethod
    def _ensure_column_if_function(
        cls,
        agg_function: Optional[Any],
        values: dict[str, Any],
    ):
        """Ensure that `pivot_column` is not null if pivot values are provided."""
        if agg_function and not values["agg_columns"]:
            raise ValueError("`agg_function` specified, but no `agg_columns`")
        return agg_function


class EntityRemoval(AbstractStep):
    """A transformation which drops entities."""

    entity_name: Union[EntityName, list[EntityName]]
    """The entity to drop."""

    def get_required_entities(self) -> set[EntityName]:
        """Get a set of the required entity names for the transformation."""
        return set()

    def get_created_entities(self) -> set[EntityName]:
        """Get a set of the entity names created by the transformation."""
        return set()

    def get_removed_entities(self) -> set[EntityName]:
        """Get a set of the entity names created by the transformation."""
        if isinstance(self.entity_name, list):
            return set(self.entity_name)
        return {self.entity_name}


class CopyEntity(AbstractStep):
    """A transformation which copies an entity."""

    entity_name: EntityName
    """The entity to copy."""
    new_entity_name: EntityName
    """The new name for the copied entity."""

    def get_required_entities(self) -> set[EntityName]:
        """Get a set of the required entity names for the transformation."""
        return {self.entity_name}

    def get_created_entities(self) -> set[EntityName]:
        """Get a set of the entity names created by the transformation."""
        return {self.new_entity_name}

    def get_removed_entities(self) -> set[EntityName]:
        """Gets the entity which has been removed"""
        return set()


class RenameEntity(CopyEntity):
    """A transformation which renames an entity."""

    def get_removed_entities(self) -> set[EntityName]:
        """Get a set of the entity names removed by the transformation."""
        return {self.entity_name}


class LeftJoin(AbstractNewColumnConditionalJoin):
    """A table join configuration. This joins `target` into `entity`.

    The table join will result in all of the columns from `entity` being retained,
    new columns can be added based on data from `target` by adding SQL expressions
    in `new_columns`.

    """


class InnerJoin(AbstractNewColumnConditionalJoin):
    """A table join configuration. This joins `target` into `entity`, retaining only
    rows which match in both tables.

    The table join will result in all of the columns from `entity` being retained,
    new columns can be added based on data from `target` by adding SQL expressions
    in `new_columns`.

    """


class OneToOneJoin(LeftJoin):
    """A table join configuration. This joins `target` into `entity`. This
    will not alter the number of rows in `target`.

    The table join will result in all of the columns from `entity` being retained,
    new columns can be added based on data from `target` by adding SQL expressions
    in `new_columns`.

    """

    perform_integrity_check: bool = True
    """
    Whether to perform the integrity check on the number of records. If this is
    disabled, it is not guaranteed that joins will be one to one.

    """


class SemiJoin(AbstractConditionalJoin):
    """A table join configuration. This keeps the rows in `entity` where the join
    condition with `target` is true.

    The table join will result in all of the columns from `entity` being retained.
    No rows can be added from `target`.

    """


class AntiJoin(AbstractConditionalJoin):
    """A table join configuration. This keeps the rows in `entity` where the join
    condition with `target` is _not_ true.

    The table join will result in all of the columns from `entity` being retained.
    No rows can be added from `target`.

    """


class ConfirmJoinHasMatch(AbstractConditionalJoin):
    """Add a boolean column with name `column_name` to `entity`. The new column
    will indicate whether the `entity` has a corresponding row in `target`.

    """

    column_name: Alias
    """
    The name of the new column to add to `entity`. This will indicate whether
    there is a corresponding match for each row.

    """
    perform_integrity_check: bool = True
    """
    Whether to perform the integrity check on the number of records. If this is
    disabled, it is not guaranteed that rows in `entity` will not be duplicated.

    """


class HeaderJoin(AbstractJoin):
    """Add a 'header' entity (`target`) to each row in `entity`. The header entity
    must contain only a single row.

    """

    header_column_name: Alias = "_Header"
    """The name of the column to add to `entity`, which will contain the header."""


class TableUnion(AbstractJoin):
    """Union two tables together, taking the columns from each by name.

    Where columns have the same name, they must be the same type or coerceable.
    Where column casing differs, the casing from the `source` entity will be kept.

    Column order will be preserved, with columns from `source` taken first and extra
    columns in `target` added in order afterwards.

    """


class OrphanIdentification(AbstractConditionalJoin):
    """Identify records in `entity` which don't have at least one corresponding
    match in `target`. A new boolean column will be added to `entity` ('IsOrphaned')
    indicating whether the condition matched.

    If there is already an 'IsOrphaned' column in the entity, this will be set to the
    logical OR of its current value and the value it would have been set to otherwise.

    """


Step = Union[AbstractStep, Literal["sync"]]
"""A step within a rule. This is either a rule config or the literal string 'sync'."""


class Rule(BaseModel):
    """A rule, made up of multiple steps."""

    name: str
    """The name of the rule."""
    pre_sync_steps: list[AbstractStep]
    """The pre-sync steps in the rule."""
    sync_filter_steps: list[DeferredFilter]
    """The sync filter steps in the rule."""
    post_sync_steps: list[AbstractStep]
    """The post-sync steps in the rule."""

    def __str__(self):  # pydantic's default __str__ strips the model name.
        return super().__repr__()

    @classmethod
    @validate_arguments
    def from_step_list(cls, name: str, steps: list[Step]):
        """Load the rule from a single step list."""
        pre_sync_steps: list[AbstractStep] = []
        sync_filter_steps: list[DeferredFilter] = []
        post_sync_steps: list[AbstractStep] = []
        self = cls(
            name=name,
            pre_sync_steps=pre_sync_steps,
            sync_filter_steps=sync_filter_steps,
            post_sync_steps=post_sync_steps,
        )

        sync_stage_start: Optional[int] = None
        sync_stage_stop: Optional[int] = None
        for step_index, step in enumerate(steps):
            in_sync_stage = (sync_stage_start is not None) and (sync_stage_stop is None)
            sync_stage_completed = sync_stage_stop is not None

            step_is_sync_filter = isinstance(step, DeferredFilter)
            step_is_sync_keyword = isinstance(step, str) and step == "sync"
            step_is_sync = step_is_sync_filter or step_is_sync_keyword

            if step_is_sync:
                if sync_stage_completed:
                    raise ValueError(
                        "Synchronised steps must be contiguous. The step at index "
                        + f"{step_index} comes after the end of the first continuous "
                        + f"sync stage ([{sync_stage_start}:{sync_stage_stop}])"
                    )

                if sync_stage_start is None:
                    sync_stage_start = step_index

                if step_is_sync_filter:
                    sync_filter_steps.append(step)  # type: ignore

                continue

            if in_sync_stage:
                sync_stage_completed = True
                sync_stage_stop = step_index

            if not sync_stage_completed:
                pre_sync_steps.append(step)  # type: ignore
            else:
                post_sync_steps.append(step)  # type: ignore

        return self

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        global_index = 0
        for stage, step_list in (
            ("Pre-sync", self.pre_sync_steps),
            ("Sync", self.sync_filter_steps),
            ("Post-sync", self.post_sync_steps),
        ):
            for stage_step_index, step in enumerate(step_list):
                step_list[stage_step_index] = step.with_parent(
                    ParentMetadata(rule=self, index=global_index, stage=stage)
                )
                global_index += 1

    def template(
        self: RSelf,
        local_variables: TemplateVariables,
        *,
        global_variables: Optional[TemplateVariables] = None,
    ) -> RSelf:
        """Template the rule, returning the new templated rule. This is only really useful
        for 'upfront' templating, as all stages of the rule will be templated at once.

        """
        rule_lists: dict[str, Union[list[AbstractStep], list[DeferredFilter]]] = {
            "pre_sync_steps": self.pre_sync_steps,
            "sync_filter_steps": self.sync_filter_steps,
            "post_sync_steps": self.post_sync_steps,
        }
        for list_name, step_list in rule_lists.items():
            templated_rule_list = []
            for step in step_list:
                step = step.template(local_variables, global_variables=global_variables)
                templated_rule_list.append(step)
            rule_lists[list_name] = templated_rule_list

        type_ = type(self)
        return type_(name=self.name, **rule_lists)


class RuleMetadata(BaseModel):
    """Metadata about the rules."""

    rules: list[Rule]
    """A list of rules to be applied to to the entities."""
    local_variables: Optional[list[TemplateVariables]] = None
    """
    An optional list of local, rule-level template variables.

    If provided, this must be the same length as `rules`.

    """
    global_variables: TemplateVariables = Field(default_factory=dict)
    """An optional mapping of global template variables."""
    templating_strategy: Literal["upfront", "runtime"] = "upfront"
    """
    The templating strategy for the rules.

    If 'upfront', template all rules at the beginning of the rule stage
    execution.

    If 'runtime', template rules immediately before evaluating them.

    Runtime templating doesn't currently add any value, but will provide
    the ability to set variables from data items in the future. With runtime
    rules, it is not possible to check that all entities are present before
    performing work.

    """
    reference_data_config: dict[EntityName, ReferenceConfigUnion]
    """
    Per-entity configuration options for the reference data.

    Options are likely to vary by backend (e.g. some backends will have
    a database and table name, some might have a remote URI).

    """

    @root_validator()
    @classmethod
    def _ensure_locals_same_length_as_rules(cls, values: dict[str, list[Any]]):
        """Ensure that if 'local_variables' is provided, it's the same length as 'rules'."""
        local_vars = values["local_variables"]
        if local_vars is not None:
            n_local_vars = len(local_vars)
            n_rules = len(values["rules"])
            if n_local_vars != n_rules:
                raise ValueError(
                    f"Mismatch between number of local variables {n_local_vars} and number of "
                    + f"rules {n_rules}"
                )
        return values

    def __iter__(self) -> Iterator[tuple[Rule, TemplateVariables]]:  # type: ignore
        """Iterate over the rules and local variables."""
        if self.local_variables is None:
            yield from ((rule, {}) for rule in self.rules)  # type: ignore
        else:
            yield from zip(self.rules, self.local_variables)


ParentMetadata.update_forward_refs()
