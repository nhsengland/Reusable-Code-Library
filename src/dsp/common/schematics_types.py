from schematics.types import CompoundType


class AnyType(CompoundType):

    def _convert(self, value, context):
        return value

    def _export(self, value, format, context):
        return value
