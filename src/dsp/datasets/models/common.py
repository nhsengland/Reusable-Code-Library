from typing import Optional

from dsp.datasets.common import Cardinality


class BaseMerging:
    strategy = None  # type: Optional[int]
    cardinality = Cardinality.MANY
    merge_keys = []


class MergeStrategy:
    ALWAYS_APPEND = 1
    OVERWRITE_OR_APPEND = 2


class ViewDefinition:
    IncludedFields = []
    Locations = {}
    MaintainOrder = False


class OrderedViewDefinition(ViewDefinition):
    MaintainOrder = True


class ReconciliationDefinition(ViewDefinition):

    class Merging(BaseMerging):
        strategy = MergeStrategy.ALWAYS_APPEND
