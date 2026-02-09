"""Core functionality for the backend bases."""

from collections.abc import Callable, Iterator, Mapping, MutableMapping
from typing import Any, Generic, Optional

from typing_extensions import get_args, get_origin

from nhs_reusable_code_library.standard_data_validations.dve.backends.exceptions import ConstraintError, MissingEntity, MissingRefDataEntity
from nhs_reusable_code_library.standard_data_validations.dve.backends.types import EntityType
from nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.type_hints import EntityName

get_original_bases: Callable[[type], tuple[Any, ...]]
try:
    # pylint: disable=ungrouped-imports
    from typing_extensions import get_original_bases  # type: ignore
except ImportError:

    def get_original_bases(__cls: type) -> tuple[Any, ...]:
        """A basic version of 'get_original_bases' in case it's not in typing extensions."""
        try:
            return __cls.__orig_bases__  # type: ignore
        except AttributeError:
            pass

        try:
            return __cls.__mro_entries__  # type: ignore
        except AttributeError:
            pass
        return __cls.__mro__


def get_entity_type(child: type, annotated_type_name: str) -> type[EntityType]:
    """Get the annotated entity type from a subclass, given the name of the parent
    class which must be annotated.

    """
    for base in get_original_bases(child):
        if isinstance(base, type):
            if base.__name__ != annotated_type_name:
                continue
        else:
            origin = get_origin(base)
            if origin is None or origin.__name__ != annotated_type_name:
                continue

        annotations = get_args(base)
        if not annotations:
            raise TypeError(f"{child}: Cannot create an untyped `{annotated_type_name}` subclass")
        if len(annotations) != 1:
            raise TypeError(f"{child}: `{annotated_type_name}` must have exactly one entity type")
        return annotations[0]  # type: ignore

    raise TypeError(f"{child}: No `{annotated_type_name}` parent found")


IsRefdata = bool


class EntityManager(Generic[EntityType], MutableMapping[EntityName, EntityType]):
    """An entity manager that creates a copy of the entities to mutate
    during processing and ensures that reference data is not mutated.

    This also ensures appropriate errors are raised for get/sets that
    result in nicer error logs.

    """

    def __init__(
        self,
        entities: MutableMapping[EntityName, EntityType],
        reference_data: Optional[Mapping[EntityName, EntityType]] = None,
    ) -> None:
        self.entities = {}
        """A copy of the loaded entities."""
        for entity_name, entity in entities.items():
            if entity_name.startswith("refdata_"):
                raise ValueError(f"Entity name cannot start with 'refdata_', got {entity_name!r}")
            self.entities[entity_name] = entity

        self.reference_data = reference_data if reference_data is not None else {}
        """The reference data mapping."""

    @staticmethod
    def _get_key_and_whether_refdata(key: str) -> tuple[EntityName, IsRefdata]:
        """Get the key and whether the entity is a reference data entry."""
        if key.startswith("refdata_"):
            return key[8:], True
        return key, False

    def __getitem__(self, key: EntityName) -> EntityType:
        entity_name, is_refdata = self._get_key_and_whether_refdata(key)

        try:
            if is_refdata:
                return self.reference_data[entity_name]
            return self.entities[entity_name]
        except KeyError as err:
            error_type = MissingRefDataEntity if is_refdata else MissingEntity
            raise error_type(entity_name=entity_name) from err

    def __setitem__(self, key: EntityName, value: EntityType) -> None:
        entity_name, is_refdata = self._get_key_and_whether_refdata(key)
        if is_refdata:
            raise ConstraintError(
                f"Attempting to mutate reference data entity {entity_name!r}",
                constraint=f"reference data entry {entity_name!r} must not be mutated",
            )
        self.entities[entity_name] = value

    def __delitem__(self, key: EntityName) -> None:
        entity_name, is_refdata = self._get_key_and_whether_refdata(key)
        if is_refdata:
            raise ConstraintError(
                f"Attempting to remove reference data entity {entity_name!r}",
                constraint=f"reference data entry {entity_name!r} must not be mutated",
            )
        del self.entities[entity_name]

    def __iter__(self) -> Iterator[str]:
        yield from iter(self.entities.keys())
        yield from ("_".join(("refdata", key)) for key in self.reference_data.keys())

    def __len__(self) -> int:
        return len(self.entities) + len(self.reference_data)
