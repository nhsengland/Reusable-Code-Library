"""Base implementation of the different step types."""

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from typing import Any, ClassVar, Generic, NoReturn, Optional, TypeVar
from uuid import uuid4

from typing_extensions import Literal, Protocol, get_type_hints

from nhs_reusable_code_library.standard_data_validations.dve.backends.core import get_entity_type
from nhs_reusable_code_library.standard_data_validations.dve.backends.exceptions import render_error
from nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.rules import (
    AbstractStep,
    Aggregation,
    AntiJoin,
    ColumnAddition,
    ColumnRemoval,
    ConfirmJoinHasMatch,
    CopyEntity,
    DeferredFilter,
    EntityRemoval,
    HeaderJoin,
    ImmediateFilter,
    InnerJoin,
    LeftJoin,
    Notification,
    OneToOneJoin,
    OrphanIdentification,
    ParentMetadata,
    RenameEntity,
    Rule,
    RuleMetadata,
    SelectColumns,
    SemiJoin,
    TableUnion,
)
from nhs_reusable_code_library.standard_data_validations.dve.backends.types import Entities, EntityType, StageSuccessful
from nhs_reusable_code_library.standard_data_validations.dve.backends.loggers import get_logger
from nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.type_hints import URI, EntityName, Messages, TemplateVariables

T_contra = TypeVar("T_contra", bound=AbstractStep, contravariant=True)
T = TypeVar("T", bound=AbstractStep)
# This needs to be defined outside the class since otherwise mypy expects
# BaseFileReader to be generic:
_StepFunctions = dict[type[T], "_UnboundStepFunction[T]"]
"""A convenience type indicating a mapping from config type to step method."""
Stage = Literal["Pre-filter", "Filter", "Post-filter"]
"""The name of a stage within a rule."""


class _UnboundStepFunction(Generic[T_contra], Protocol):  # pylint: disable=too-few-public-methods
    """A generic step function."""

    @staticmethod
    def __call__(  # pylint: disable=bad-staticmethod-argument
        self: "BaseStepImplementations",  # This is the protocol for an _unbound_ method.
        entities: Entities,
        *,
        config: T_contra,
    ) -> Messages: ...


class BaseStepImplementations(Generic[EntityType], ABC):  # pylint: disable=too-many-public-methods
    """An abstract implementation of the transformation rules."""

    __step_functions__: ClassVar[_StepFunctions] = {}
    """
    A dictionary mapping step config type to the method implementing the
    step.

    This is set and populated in `__init_subclass__` by identifying methods
    based on their config type.

    """

    __entity_type__: ClassVar[type[EntityType]]  # type: ignore
    """
    The entity type that the steps are implemented for.

    This will be populated from the generic annotation at class creation time.

    """

    def __init_subclass__(cls, *_, **__) -> None:
        """When this class is subclassed, create and populate the `__step_functions__`
        class variable for the subclass.

        """
        cls.__step_functions__ = {}

        # Set entity type from parent class subscript.
        if cls is not BaseStepImplementations:
            cls.__entity_type__ = get_entity_type(cls, "BaseStepImplementations")

        for method_name in dir(cls):
            if method_name.startswith("_"):
                continue

            method = getattr(cls, method_name, None)
            if method is None or not callable(method):
                continue

            type_hints = get_type_hints(method)
            if set(type_hints.keys()) != {"entities", "config", "return"}:
                continue
            config_type = type_hints["config"]
            if not issubclass(config_type, AbstractStep):
                continue
            cls.__step_functions__[config_type] = method  # type: ignore

    def __init__(  # pylint: disable=unused-argument
        self,
        logger: Optional[logging.Logger] = None,
        **kwargs: Any,
    ):
        self.logger = logger or get_logger(type(self).__name__)
        """The `logging.Logger instance for the data contract config."""

    @classmethod
    @abstractmethod
    def register_udfs(cls, **kwargs):
        """Method to register all custom dve functions for use during business rules application"""
        raise NotImplementedError()

    @staticmethod
    def add_row_id(entity: EntityType) -> EntityType:
        """Add a unique row id field to an entity"""
        raise NotImplementedError()

    @staticmethod
    def drop_row_id(entity: EntityType) -> EntityType:
        """Add a unique row id field to an entity"""
        raise NotImplementedError()

    @classmethod
    def _raise_notimplemented_error(
        cls, config_type: type[AbstractStep], source: Exception
    ) -> NoReturn:
        """Raise a `NotImplementedError` from a provided error."""
        raise NotImplementedError(
            f"Backend {cls.__name__} does not have an implementation for step type "
            + config_type.__name__
        ) from source

    @staticmethod
    def _step_metadata_to_location(step_metadata: "AbstractStep") -> str:
        """Convert a step definition to a location string."""
        if step_metadata.parent is None:
            return f"transformations (step: {step_metadata})"

        if isinstance(step_metadata.parent.rule, str):
            rule_name = step_metadata.parent.rule
        else:
            rule_name = step_metadata.parent.rule.name

        index = step_metadata.parent.index
        step_id = step_metadata.id
        return f"transformations (rule: {rule_name}; step: {index}; id: {step_id})"

    def _handle_rule_error(self, error: Exception, config: AbstractStep) -> Messages:
        """Log an error and create appropriate error messages."""
        return render_error(error, self._step_metadata_to_location(config))

    def evaluate(self, entities, *, config: AbstractStep) -> tuple[Messages, StageSuccessful]:
        """Evaluate a step definition, applying it to the entities."""
        config_type = type(config)
        success = True
        try:
            try:
                method = self.__step_functions__[config_type]
            except KeyError as err:
                self._raise_notimplemented_error(config_type, err)

            try:
                messages = method(self, entities, config=config)
            except NotImplementedError as err:
                self._raise_notimplemented_error(config_type, err)
        except Exception as err:  # pylint: disable=broad-except
            success = False
            messages = self._handle_rule_error(err, config)
        else:
            for message in messages:
                if not message.is_critical:
                    continue

                if success:
                    success = False
                    msg = f"Critical failure in rule {self._step_metadata_to_location(config)}"
                    self.logger.error(msg)
                self.logger.error(str(message))

        return messages, success

    @abstractmethod
    def add(self, entities: Entities, *, config: ColumnAddition) -> Messages:
        """A transformation step which adds a column to an entity."""
        raise NotImplementedError

    @abstractmethod
    def remove(self, entities: Entities, *, config: ColumnRemoval) -> Messages:
        """A transformation step which removes a column from an entity."""
        raise NotImplementedError

    @abstractmethod
    def select(self, entities: Entities, *, config: SelectColumns) -> Messages:
        """A transformation step which selects columns from an entity."""

    @abstractmethod
    def group_by(self, entities: Entities, *, config: Aggregation) -> Messages:
        """A transformation step which performs an aggregation on an entity."""

    def copy(self, entities: Entities, *, config: CopyEntity) -> Messages:
        """A transformation step which copies an entity."""
        entities[config.new_entity_name] = entities[config.entity_name]
        return []

    def remove_entity(self, entities: Entities, *, config: EntityRemoval) -> Messages:
        """A transformation step which removes an entity."""
        entity_names = config.entity_name
        if not isinstance(entity_names, list):
            entity_names = [entity_names]

        for entity_name in entity_names:
            try:
                del entities[entity_name]
            except KeyError:
                pass
        return []

    def remove_entities(self, entities: Entities, *, config: EntityRemoval) -> Messages:
        """A transformation step which removes multiple entities."""
        return self.remove_entity(entities, config=config)

    def rename_entity(self, entities: Entities, *, config: RenameEntity) -> Messages:
        """A transformation step which renames an entity."""
        entities[config.new_entity_name] = entities[config.entity_name]
        del entities[config.entity_name]
        return []

    def has_match(self, entities: Entities, *, config: ConfirmJoinHasMatch) -> Messages:
        """Add a boolean column to a source entity, indicating whether it matches
        a target for the given condition.

        This may not be implemented by some backends.

        """
        raise NotImplementedError

    @abstractmethod
    def left_join(self, entities: Entities, *, config: LeftJoin) -> Messages:
        """Perform a left join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        """
        raise NotImplementedError

    @abstractmethod
    def inner_join(self, entities: Entities, *, config: InnerJoin) -> Messages:
        """Perform an inner join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        """
        raise NotImplementedError

    def one_to_one_join(self, entities: Entities, *, config: OneToOneJoin) -> Messages:
        """Perform a join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        This will be a left join that enforces a one-to-one relationship.

        This may not be implemented by some backends.

        """
        raise NotImplementedError

    @abstractmethod
    def semi_join(self, entities: Entities, *, config: SemiJoin) -> Messages:
        """Perform a semi join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        """

    @abstractmethod
    def anti_join(self, entities: Entities, *, config: AntiJoin) -> Messages:
        """Perform an anti join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        """

    def join_header(self, entities: Entities, *, config: HeaderJoin) -> Messages:
        """Add a 'header' entity to each row in the source entity. The header entity
        must contain only a single record.

        This may not be implemented by some backends.

        """
        raise NotImplementedError

    def identify_orphans(self, entities: Entities, *, config: OrphanIdentification) -> Messages:
        """Identify records in an entity which don't have at least one corresponding
        match in the target. A new boolean column will be added to `entity` ('IsOrphaned')
        indicating whether the condition matched.

        If there is already an 'IsOrphaned' column in the entity, this will be set to the
        logical OR of its current value and the value it would have been set to otherwise.

        This may not be implemented by some backends.

        """
        raise NotImplementedError

    @abstractmethod
    def union(self, entities: Entities, *, config: TableUnion) -> Messages:
        """Union two entities together, taking the columns from each by name.

        Where columns have the same name, they must be the same type or coerceable.
        Where column casing differs, the casing from the `source` entity will be kept.

        Column order will be preserved, with columns from `source` taken first and extra
        columns in `target` added in order afterwards.

        """
        raise NotImplementedError

    @abstractmethod
    def filter(self, entities: Entities, *, config: ImmediateFilter) -> Messages:
        """Filter an entity immediately, and do not emit any messages.

        The synchronised filter stage will be implemented separately.

        """
        raise NotImplementedError

    @abstractmethod
    def notify(self, entities: Entities, *, config: Notification) -> Messages:
        """Emit a notification based on an expression. Where the expression is truthy,
        a nofication should be emitted according to the reporting config.

        This is not intended to be used directly, but is used in the implementation of
        the sync filters.

        """

    def apply_sync_filters(
        self, entities: Entities, *filters: DeferredFilter
    ) -> tuple[Messages, StageSuccessful]:
        """Apply the synchronised filters, emitting appropriate error messages for any
        records which do not meet the conditions.

        Any records which have emitted a record-level error should be removed _after_
        **all** filter conditions have been evaluated and appropriate messages emitted
        for that entity.

        """
        filters_by_entity: dict[EntityName, list[DeferredFilter]] = defaultdict(list)
        for rule in filters:
            filters_by_entity[rule.entity_name].append(rule)

        messages: Messages = []
        for entity_name, filter_rules in filters_by_entity.items():
            entity = entities[entity_name]

            filter_column_names: list[str] = []
            unmodified_entities = {entity_name: entity}
            modified_entities = {entity_name: entity}

            for rule in filter_rules:
                if rule.reporting.emit == "record_failure":
                    column_name = f"filter_{uuid4().hex}"
                    filter_column_names.append(column_name)
                    temp_messages, success = self.evaluate(
                        modified_entities,
                        config=ColumnAddition(
                            entity_name=entity_name,
                            column_name=column_name,
                            expression=rule.expression,
                            parent=rule.parent,
                        ),
                    )
                    messages.extend(temp_messages)
                    if not success:
                        return messages, False

                    temp_messages, success = self.evaluate(
                        modified_entities,
                        config=Notification(
                            entity_name=entity_name,
                            expression=f"NOT {column_name}",
                            excluded_columns=filter_column_names,
                            reporting=rule.reporting,
                            parent=rule.parent,
                        ),
                    )
                    messages.extend(temp_messages)
                    if not success:
                        return messages, False

                else:
                    temp_messages, success = self.evaluate(
                        unmodified_entities,
                        config=Notification(
                            entity_name=entity_name,
                            expression=f"NOT ({rule.expression})",
                            reporting=rule.reporting,
                            parent=rule.parent,
                        ),
                    )
                    messages.extend(temp_messages)
                    if not success:
                        return messages, False

            if filter_column_names:
                success_condition = " AND ".join(
                    [f"({c_name} IS NOT NULL AND {c_name})" for c_name in filter_column_names]
                )
                temp_messages, success = self.evaluate(
                    modified_entities,
                    config=ImmediateFilter(
                        entity_name=entity_name,
                        expression=success_condition,
                        parent=ParentMetadata(
                            rule="FilterStageRecordLevelFilterApplication", index=0, stage="Sync"
                        ),
                    ),
                )
                messages.extend(temp_messages)
                if not success:
                    return messages, False

                for index, filter_column_name in enumerate(filter_column_names):
                    temp_messages, success = self.evaluate(
                        modified_entities,
                        config=ColumnRemoval(
                            entity_name=entity_name,
                            column_name=filter_column_name,
                            parent=ParentMetadata(
                                rule="FilterStageRecordLevelFilterColumnRemoval",
                                index=index,
                                stage="Sync",
                            ),
                        ),
                    )
                    messages.extend(temp_messages)
                    if not success:
                        return messages, False

                entities.update(modified_entities)

        return messages, True

    def apply_rules(self, entities: Entities, rule_metadata: RuleMetadata) -> Messages:
        """Create rule definitions from the metadata for a given dataset and evaluate
        the impact on the provided entities, returning a deque of messages and
        altering the entities in-place.

        """
        rules_and_locals: Iterable[tuple[Rule, TemplateVariables]]
        if rule_metadata.templating_strategy == "upfront":
            rules_and_locals = []
            for rule, local_variables in rule_metadata:
                rules_and_locals.append(
                    (
                        rule.template(
                            local_variables, global_variables=rule_metadata.global_variables
                        ),
                        {},
                    )
                )
        else:
            rules_and_locals = rule_metadata

        messages: Messages = []
        for rule, local_variables in rules_and_locals:
            for step in rule.pre_sync_steps:
                if rule_metadata.templating_strategy == "runtime":
                    step = step.template(
                        local_variables, global_variables=rule_metadata.global_variables
                    )

                stage_messages, success = self.evaluate(entities, config=step)
                messages.extend(stage_messages)
                if not success:
                    return messages

        sync_steps = []
        for rule, local_variables in rules_and_locals:
            for step in rule.sync_filter_steps:
                if rule_metadata.templating_strategy == "runtime":
                    step = step.template(
                        local_variables, global_variables=rule_metadata.global_variables
                    )
                sync_steps.append(step)

        stage_messages, success = self.apply_sync_filters(entities, *sync_steps)
        messages.extend(stage_messages)
        if not success:
            return messages

        for rule, local_variables in rules_and_locals:
            for step in rule.post_sync_steps:
                if rule_metadata.templating_strategy == "runtime":
                    step = step.template(
                        local_variables, global_variables=rule_metadata.global_variables
                    )

                stage_messages, success = self.evaluate(entities, config=step)
                messages.extend(stage_messages)
                if not success:
                    return messages
        return messages

    def read_parquet(self, path: URI, **kwargs) -> EntityType:
        """Method to read parquet files"""
        raise NotImplementedError()

    def write_parquet(self, entity: EntityType, target_location: URI, **kwargs) -> URI:
        """Method to write parquet files"""
        raise NotImplementedError()
