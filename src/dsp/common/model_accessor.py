from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Type, List, Any, Optional, Union

from pyspark.sql.types import DataType

from dsp.datasets.common import Cardinality

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class ModelAccessor(ABC, Generic[T, U]):

    def __init__(self, value_type: Type[U]):
        self.value_type = value_type

    @abstractmethod
    def __get__(self, instance: Optional[T], owner) -> Union['ModelAccessor[T, U]', U]:
        pass

    @abstractmethod
    def __getattr__(self, item: str) -> 'ModelAccessor[T, Any]':
        pass

    def __getitem__(self, item: Union[str, int, slice]) -> 'ModelAccessor[T, Any]':
        if isinstance(item, str):
            return self.__getattr__(item)

        if self.__output_cardinality__ == Cardinality.MANY:
            if isinstance(item, int):
                return IndexingModelPath(self, item)

            if isinstance(item, slice):
                return SlicingModelPath(self, item)

        raise TypeError

    @property
    @abstractmethod
    def __input_cardinality__(self) -> Cardinality:
        pass

    @property
    @abstractmethod
    def __output_cardinality__(self) -> Cardinality:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass


class ModelAttribute(ModelAccessor[T, U], Generic[T, U]):

    def __init__(self, field: object, value_type: object, spark_type: object = None) -> object:
        """

        Returns:
            object:
        """
        super().__init__(value_type)
        self.field = field
        self.spark_type = spark_type

    def __get__(self, instance: Optional[T], owner=None) -> Union['ModelAttribute[T, U]', U]:
        if instance is None:
            return self
        return getattr(instance, self.field)

    def __getattr__(self, item: str) -> ModelAccessor[T, Any]:
        return ModelPath(self, getattr(self.value_type, item))

    @property
    def __input_cardinality__(self) -> Cardinality:
        return Cardinality.ONE

    @property
    def __output_cardinality__(self) -> Cardinality:
        return Cardinality.ONE

    def __str__(self) -> str:
        return self.field


class RepeatingModelAttribute(ModelAccessor[T, List[U]], Generic[T, U]):

    def __init__(self, field: str, value_type: Type[U]):
        super().__init__(value_type)
        self.field = field

    def __get__(self, instance: Optional[T], owner=None) -> Union['RepeatingModelAttribute[T, U]', List[U]]:
        if instance is None:
            return self
        return getattr(instance, self.field)

    def __getattr__(self, item: str) -> ModelAccessor[T, Any]:
        return ModelPath(self, getattr(self.value_type, item))

    @property
    def __input_cardinality__(self) -> Cardinality:
        return Cardinality.ONE

    @property
    def __output_cardinality__(self) -> Cardinality:
        return Cardinality.MANY

    def __str__(self) -> str:
        return self.field


class ModelPath(ModelAccessor[T, V], Generic[T, U, V]):

    def __init__(self, left_path: ModelAccessor[T, U], right_path: ModelAccessor[U, V]):
        assert left_path.__output_cardinality__ != Cardinality.ONE \
               or right_path.__input_cardinality__ == Cardinality.ONE

        super().__init__(right_path.value_type)

        self.left_path = left_path
        self.right_path = right_path

    def __get__(self, instance: Optional[T], owner=None) -> Union['ModelPath[T, U, V]', V]:
        if instance is None:
            return self

        left_value = self.left_path.__get__(instance, owner)

        if self.left_path.__output_cardinality__ == Cardinality.MANY:
            if self.right_path.__input_cardinality__ == Cardinality.ONE:
                path_value = [self.right_path.__get__(left_element, owner) for left_element in left_value]
                if self.right_path.__output_cardinality__ == Cardinality.MANY:
                    return [element for sublist in path_value for element in sublist]

                return path_value

        return self.right_path.__get__(left_value, owner)

    def __getattr__(self, item: str) -> ModelAccessor[T, Any]:
        return ModelPath(self.left_path, getattr(self.right_path, item))

    @property
    def __input_cardinality__(self) -> Cardinality:
        return self.left_path.__input_cardinality__

    @property
    def __output_cardinality__(self) -> Cardinality:
        return self.right_path.__output_cardinality__

    def __str__(self) -> str:
        return "{}.{}".format(self.left_path, self.right_path)


class IndexingModelPath(ModelAccessor[T, U], Generic[T, U]):

    def __init__(self, left_path: ModelAccessor[T, List[U]], index: int):
        assert left_path.__output_cardinality__ == Cardinality.MANY

        super().__init__(left_path.value_type)

        self.left_path = left_path
        self.index = index

    def __get__(self, instance: Optional[T], owner=None) -> Union['IndexingModelPath[T, U]', U]:
        if instance is None:
            return self
        return self.left_path.__get__(instance)[self.index]

    def __getattr__(self, item: str) -> ModelAccessor[T, Any]:
        return ModelPath(self, getattr(self.value_type, item))

    @property
    def __input_cardinality__(self):
        return self.left_path.__input_cardinality__

    @property
    def __output_cardinality__(self):
        return Cardinality.ONE

    def __str__(self) -> str:
        return "[{}]".format(self.index)


class SlicingModelPath(ModelAccessor[T, List[U]], Generic[T, U]):

    def __init__(self, left_path: ModelAccessor[T, List[U]], selected_slice: slice):
        assert left_path.__output_cardinality__ == Cardinality.MANY

        super().__init__(left_path.value_type)

        self.left_path = left_path
        self.slice = selected_slice

    def __get__(self, instance: Optional[T], owner=None) -> Union['SlicingModelPath[T, U]', List[U]]:
        if instance is None:
            return self
        return self.left_path.__get__(instance, owner)[self.slice]

    def __getattr__(self, item: str) -> ModelAccessor[T, Any]:
        return ModelPath(self, getattr(self.value_type, item))

    @property
    def __input_cardinality__(self) -> Cardinality:
        return Cardinality.MANY

    @property
    def __output_cardinality__(self) -> Cardinality:
        return Cardinality.MANY

    def __str__(self) -> str:
        slice_start = self.slice.start
        slice_stop = self.slice.stop
        slice_step = self.slice.step
        return "{}[{}:{}{}]".format(self.left_path, slice_start if slice_start is not None else "",
                                    slice_stop if slice_stop is not None else "",
                                    ":{}".format(slice_step) if slice_step is not None else "")


class ModelRoot(ModelAccessor[T, T]):

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        return instance

    def __getattr__(self, item):
        return ModelPath(self, getattr(self.value_type, item))

    @property
    def __input_cardinality__(self) -> Cardinality:
        return Cardinality.ONE

    @property
    def __output_cardinality__(self) -> Cardinality:
        return Cardinality.ONE

    def __str__(self):
        return "$"
