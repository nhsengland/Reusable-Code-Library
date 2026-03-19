"""The base implementation of the reference data loader.."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping
from typing import ClassVar, Generic, Optional, Union, get_type_hints

from pydantic import BaseModel, Field
from typing_extensions import Annotated, Literal

from dve.core_engine.backends.base.core import get_entity_type
from dve.core_engine.backends.exceptions import MissingRefDataEntity
from dve.core_engine.backends.types import EntityType
from dve.core_engine.type_hints import EntityName


class ReferenceTable(BaseModel, frozen=True):
    """Configuration for a reference data object when table_name."""

    type: Literal["table"]
    """The object type."""
    table_name: str
    """Name of the table where the data persists."""
    database: Optional[str] = None
    """Name of the database where the reference data is located."""

    @property
    def fq_table_name(self):
        """The fully qualified table name"""
        if self.database:
            return f"{self.database}.{self.table_name}"
        return self.table_name


class ReferenceFile(BaseModel, frozen=True):
    """Configuration for a reference data object when a file."""

    type: Literal["filename"]
    """The object type."""
    filename: str
    """The path to the reference data (as Parquet) relative to the contract."""


class ReferenceURI(BaseModel, frozen=True):
    """Configuration for a reference data object when a URI."""

    type: Literal["uri"]
    """The object type."""
    uri: str
    """The absolute URI of the reference data (as Parquet)."""


ReferenceConfig = Union[ReferenceFile, ReferenceTable, ReferenceURI]
"""The config utilised to load the reference data"""

ReferenceConfigUnion = Annotated[ReferenceConfig, Field(discriminator="type")]
"""Discriminated union to determine refdata config from supplied type"""


class BaseRefDataLoader(Generic[EntityType], Mapping[EntityName, EntityType], ABC):
    """A reference data mapper which lazy-loads requested entities."""

    __entity_type__: ClassVar[type[EntityType]]  # type: ignore
    """
    The entity type used for the reference data.

    This will be populated from the generic annotation at class creation time.

    """
    __step_functions__: ClassVar[dict[type[ReferenceConfig], Callable]] = {}
    """
    A mapping between refdata config types and functions to call to load these configs
    into reference data entities
    """
    prefix: str = "refdata_"

    def __init_subclass__(cls, *_, **__) -> None:
        """When this class is subclassed, create and populate the `__step_functions__`
        class variable for the subclass.

        """
        # Set entity type from parent class subscript.
        if cls is not BaseRefDataLoader:
            cls.__entity_type__ = get_entity_type(cls, "BaseRefDataLoader")

        cls.__step_functions__ = {}

        for method_name in dir(cls):
            if method_name.startswith("_"):
                continue

            method = getattr(cls, method_name, None)
            if method is None or not callable(method):
                continue

            type_hints = get_type_hints(method)
            if set(type_hints.keys()) != {"config", "return"}:
                continue
            config_type = type_hints["config"]
            if not issubclass(config_type, BaseModel):
                continue
            cls.__step_functions__[config_type] = method  # type: ignore

    # pylint: disable=unused-argument
    def __init__(
        self, reference_entity_config: dict[EntityName, ReferenceConfig], **kwargs
    ) -> None:
        self.reference_entity_config = reference_entity_config
        """
        Configuration options for the reference data. This is likely to vary
        from backend to backend (e.g. might be locations and file types for
        some backends, and table names for others).

        """
        self.entity_cache: dict[EntityName, EntityType] = {}
        """A cache for already-loaded entities."""

    @abstractmethod
    def load_table(self, config: ReferenceTable) -> EntityType:
        """Load reference entity from a database table"""
        raise NotImplementedError()

    @abstractmethod
    def load_file(self, config: ReferenceFile) -> EntityType:
        "Load reference entity from a relative file path"
        raise NotImplementedError()

    @abstractmethod
    def load_uri(self, config: ReferenceURI) -> EntityType:
        "Load reference entity from an absolute URI"
        raise NotImplementedError()

    def load_entity(self, entity_name: EntityName, config: ReferenceConfig) -> EntityType:
        """Load a reference entity given the reference config"""
        config_type = type(config)
        func = self.__step_functions__[config_type]
        entity = func(self, config)
        self.entity_cache[entity_name] = entity
        return entity

    def __getitem__(self, key: EntityName) -> EntityType:
        try:
            return self.entity_cache[key]
        except KeyError:
            try:
                config = self.reference_entity_config[key]
                return self.load_entity(entity_name=key, config=config)
            except Exception as err:
                raise MissingRefDataEntity(entity_name=key) from err

    def __iter__(self) -> Iterator[str]:
        return iter(self.reference_entity_config.keys())

    def __len__(self) -> int:
        return len(self.reference_entity_config)
