from abc import ABC, abstractmethod
from typing import Any, List, Set, Dict


class FilteringExtractDefinitionDecorator(ABC):

    def __init__(self, delegate_definition: Any):
        self.__name__ = delegate_definition.__name__
        self._delegate_definition = delegate_definition

    @property
    def IncludedFields(self) -> List[str]:
        return self._delegate_definition.IncludedFields

    @property
    def Locations(self) -> Dict[str, str]:
        if self._should_apply_filter():
            return {
                **self._delegate_definition.Locations,
                **{field: "NULL as {}".format(field) for field in self._filtered_fields()}
            }

        return self._delegate_definition.Locations

    @property
    def MaintainOrder(self) -> bool:
        return self._delegate_definition.MaintainOrder

    @abstractmethod
    def _should_apply_filter(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _filtered_fields(self) -> Set[str]:
        raise NotImplementedError
