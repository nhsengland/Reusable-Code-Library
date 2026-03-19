# from dsp.datasets.models.epmawspc2 import _EPMAWellSkyPrescriptionModel2
from dsp.common.expressions import ModelExpression

__all__ = \
    [
        "CompareTwoColumnsToTwoLists",
        "FieldBeginsWith",
        "ConvertLegacyMddf"
    ]


class CompareTwoColumnsToTwoLists(ModelExpression):
    """Checks if the specified inputs match any value in its respectively specified lookup collection."""

    def __init__(self, input1: ModelExpression, lookup1: ModelExpression, input2: ModelExpression,
                 lookup2: ModelExpression, val_if_true: ModelExpression, val_if_false: ModelExpression):
        self.input1 = input1
        self.lookup1 = lookup1
        self.input2 = input2
        self.lookup2 = lookup2
        self.val_if_true = val_if_true
        self.val_if_false = val_if_false

    def resolve_value(self, model):
        """ Tests if either input contains any one element in the respective collection.
        If either or both match values from their respective collections then val_if_true value will
        be returned, otherwise the val_if_false value will be returned.

        Args:
            model: Model used when evaluating the ModelExpressions
        """
        input1 = self.input1.resolve_value(model)
        lookup1 = self.lookup1.resolve_value(model)
        input2 = self.input2.resolve_value(model)
        lookup2 = self.lookup2.resolve_value(model)
        val_if_false = self.val_if_false.resolve_value(model)
        val_if_true = self.val_if_true.resolve_value(model)

        if any(value for value in [input1, lookup1, input2, lookup2] if value is None):
            return val_if_false
        elif any(value for value in lookup1 if value == input1) or any(value for value in lookup2 if value == input2):
            return val_if_true
        else:
            return val_if_false


class FieldBeginsWith(ModelExpression):
    def __init__(self, field: ModelExpression, start_character: ModelExpression):
        self.field = field
        self.start_character = start_character

    def resolve_value(self, model):
        field = self.field.resolve_value(model)
        start_character = self.start_character.resolve_value(model)
        if any([field is None, start_character is None]):
            return False

        return field.strip().lower().startswith(start_character.lower())


class ConvertLegacyMddf(ModelExpression):
    def __init__(self, field: ModelExpression):
        self.field = field

    def resolve_value(self, model):
        field = self.field.resolve_value(model)
        try:
            return str(int(field[-3:]) * int('1000000') + int(field[:-3]))
        except:
            return field