import collections
import types
from enum import Enum, EnumMeta

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict


class LabelledEnumMeta(EnumMeta):

    def __new__(metacls, cls, bases, classdict):
        # pylint: disable=protected-access

        labels_member = '__labels__'
        enum_class = super(LabelledEnumMeta, metacls).__new__(metacls, cls, bases, classdict)

        # we need to remove __labels__ from _member_names_ and _member_map_
        # so iteration over the enum works as expected
        enum_class._member_names_ = [name for name in enum_class._member_names_ if name != labels_member]
        enum_class._member_map_ = collections.OrderedDict({
            k: v for k, v in enum_class._member_map_.items()
            if k != labels_member
        })

        return enum_class


class LabelledEnum(Enum, metaclass=LabelledEnumMeta):
    """
    Adds the possibility to set labels on enums.

    Examples:
        >>> class Season(LabelledEnum):
        ...     WINTER = 0
        ...     SPRING = 1
        ...     SUMMER = 2
        ...     AUTUMN = 3
        ...
        ...     __labels__ = {
        ...         WINTER: 'January, February, March',
        ...         SPRING: 'April, May, June',
        ...         SUMMER: 'July, August, September',
        ...         AUTUMN: 'October, November, December',
        ...     }
        >>> Season.SUMMER.label
        'July, August, September'

    """

    __labels__ = {}  # type: Dict[Any, str]

    @types.DynamicClassAttribute
    def label(self) -> str:
        return self.__labels__[self.value]
