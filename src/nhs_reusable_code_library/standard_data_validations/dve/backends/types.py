"""Generic types specifically for backend implementations.

These are defined here because in general this information should not
'leak' outside of the backend.

"""

from collections.abc import MutableMapping
from typing import TypeVar

from src.nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.type_hints import EntityName

EntityType = TypeVar("EntityType")  # pylint: disable=invalid-name
"""
The type of an entity in a generic backend implementation.

This will probably be some sort of DataFrame implementation.

"""
Entities = MutableMapping[EntityName, EntityType]
"""
The type of the entities in a generic backend implementation.

"""
StageSuccessful = bool
"""Whether a stage has completed successfully."""
