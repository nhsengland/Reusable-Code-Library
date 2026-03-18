import decimal
from collections import OrderedDict
from copy import copy
from datetime import datetime, date
from decimal import Decimal
from functools import partial
from typing import Any, Dict, List, Type, TypeVar, Generic, Generator, Tuple, Callable, Set

import json
from pyspark.rdd import RDD
from pyspark.sql import types as t
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DataType

from dsp.common.json_helpers import ISODateEncoder
from dsp.datasets.common import Fields as CommonFields, Cardinality
from dsp.common.expressions import ModelExpression
from dsp.common.model_accessor import ModelAccessor, ModelAttribute, RepeatingModelAttribute
from dsp.shared import safe_issubclass, safe_isinstance

T = TypeVar('T')
U = TypeVar('U')
SparkType = TypeVar('SparkType', bound=t.DataType)


class SubmittedAttribute(ModelAttribute[T, U], Generic[T, U]):
    pass


class AssignableAttribute(ModelAttribute[T, U], Generic[T, U]):
    pass


class RepeatingSubmittedAttribute(RepeatingModelAttribute[T, U], Generic[T, U]):
    pass


class DerivedAttributePlaceholder(ModelAttribute[T, U], Generic[T, U]):
    pass


class DerivedAttribute(DerivedAttributePlaceholder[T, U], Generic[T, U]):

    def __init__(self, field: object, value_type: object, expression: object) -> object:
        """

        Returns:
            object: 
        """
        super().__init__(field, value_type)
        self.expression = expression


def get_sql(spark_type_instance: SparkType, is_root: bool = False, indent: int = 0) -> str:
    # https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types

    if isinstance(spark_type_instance, t.StructType):
        indent_str = ' ' * indent
        fields = OrderedDict()
        for struct_field in spark_type_instance:
            inner_sql_type = get_sql(struct_field.dataType, indent=indent + 4)
            fields[struct_field.name] = inner_sql_type
        if is_root:
            sql_fields = ',\n'.join(['{}`{}` {}'.format(indent_str, f, t) for f, t in fields.items()])
            return sql_fields
        sql_fields = ',\n'.join(['{}`{}`: {}'.format(indent_str, f, t) for f, t in fields.items()])
        return 'STRUCT<\n{}\n>'.format(sql_fields)

    if isinstance(spark_type_instance, t.ArrayType):
        inner_sql_type = get_sql(spark_type_instance.elementType, indent=indent + 4)
        return 'ARRAY<{}>'.format(inner_sql_type)

    if isinstance(spark_type_instance, t.MapType):
        raise NotImplementedError('Please add MapType support if required')

    return spark_type_instance.simpleString().upper()


# Purely used for type mapping - doesn't offer any additional functionality
class Decimal_5_2(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_9_2(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_10_2(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_12_6(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_15_2(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_14_5(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_16_6(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_19_0(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_20_0(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_23_0(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


class Decimal_20_2(decimal.Decimal):
    def __new__(cls, value):
        return Decimal(value)


def get_spark_type(
        python_type: Any, include_derived_attr: bool = True, meta_available: bool = True,
        additional_fields: Callable[[Type['DSPStructuredModel']], Dict[str, DataType]] = None,
        use_spec_spark_type: bool = False
) -> T:
    simple_type_mapping = {
        datetime: lambda: t.TimestampType(),
        int: lambda: t.LongType(),
        str: lambda: t.StringType(),
        date: lambda: t.DateType(),
        bool: lambda: t.BooleanType(),
        Decimal: lambda: t.DecimalType(17),
        Decimal_5_2: lambda: t.DecimalType(5, 2),
        Decimal_9_2: lambda: t.DecimalType(9, 2),
        Decimal_10_2: lambda: t.DecimalType(10, 2),
        Decimal_12_6: lambda: t.DecimalType(12, 6),
        Decimal_14_5: lambda: t.DecimalType(14, 5),
        Decimal_15_2: lambda: t.DecimalType(15, 2),
        Decimal_16_6: lambda: t.DecimalType(16, 6),
        Decimal_19_0: lambda: t.DecimalType(19, 0),
        Decimal_20_0: lambda: t.DecimalType(20, 0),
        Decimal_20_2: lambda: t.DecimalType(20, 2),
        Decimal_23_0: lambda: t.DecimalType(23, 0)
    }
    if safe_issubclass(python_type, DSPStructuredModel):
        spark_type = t.StructType()
        for attribute_name, attribute_spec in python_type.__attribute_specs__.items():
            if isinstance(attribute_spec, DerivedAttributePlaceholder) and not include_derived_attr:
                continue

            if safe_issubclass(attribute_spec.value_type, META) and not meta_available:
                continue

            if use_spec_spark_type and hasattr(attribute_spec, 'spark_type') and attribute_spec.spark_type is not None:
                spark_inner_type = attribute_spec.spark_type
            else:
                python_inner_type = attribute_spec.value_type
                spark_inner_type = get_spark_type(
                    python_inner_type, include_derived_attr, meta_available, additional_fields,
                    use_spec_spark_type=use_spec_spark_type
                )

            if attribute_spec.__output_cardinality__ == Cardinality.MANY:
                spark_inner_type = t.ArrayType(spark_inner_type)

            spark_type.add(attribute_name, spark_inner_type)

        if additional_fields:
            fields = additional_fields(python_type)
            for name, type in fields.items():
                spark_type.add(name, type)

        return spark_type

    if python_type in simple_type_mapping:
        return simple_type_mapping[python_type]()

    if safe_issubclass(python_type, List):
        list_member_type = python_type.__args__[0]
        return t.ArrayType(get_spark_type(list_member_type, include_derived_attr, meta_available, additional_fields))

    raise NotImplementedError('Unable to produce a Spark DataType for {}'.format(python_type))


class MergeStrategy:
    """
    Defines the way two DSPStructuredModels should be combined to produce a superseding record.

    Attributes:
        ALWAYS_APPEND: model will always be appended to
        OVERWRITE_OR_APPEND: overwrite if certain parameters match between one and two,
                             otherwise append
    """

    ALWAYS_APPEND = 1
    OVERWRITE_OR_APPEND = 2


class DSPStructuredModelMeta(type):

    def __new__(cls, name, bases, dkt):
        model_class = super(DSPStructuredModelMeta, cls).__new__(cls, name, bases, dkt)  # type: DSPStructuredModel
        model_class.__attribute_specs__ = {}

        if getattr(model_class, '__concrete__', False):
            cls.augment_model_with_specs(model_class)

        return model_class

    @staticmethod
    def augment_model_with_specs(model_type):
        attribute_specs = OrderedDict()  # type: OrderedDict[str, ModelAccessor]

        for attribute_name in dir(model_type):
            if attribute_name.startswith("_"):
                continue

            attribute_value = getattr(model_type, attribute_name)

            if isinstance(attribute_value, (SubmittedAttribute, AssignableAttribute, RepeatingSubmittedAttribute)):
                attribute_specs[attribute_name] = attribute_value

                def get_submitted_value(self, field):
                    return self.values[field]

                setattr(model_type, attribute_name, property(fget=partial(get_submitted_value, field=attribute_name)))

            elif isinstance(attribute_value, DerivedAttribute):
                attribute_specs[attribute_name] = attribute_value

                def get_derived_value(self, field: str, expression: ModelExpression) -> U:
                    if field in self.values:
                        return self.values[field]

                    value = expression.resolve_value(self)
                    self.values[field] = value
                    return value

                setattr(model_type, attribute_name, property(fget=partial(get_derived_value, field=attribute_name,
                                                                          expression=attribute_value.expression)))

            elif isinstance(attribute_value, DerivedAttributePlaceholder):
                attribute_specs[attribute_name] = attribute_value
                setattr(model_type, attribute_name, None)

            else:
                setattr(model_type, attribute_name, attribute_value)

        model_type.__attribute_specs__ = attribute_specs


class DSPStructuredModel(metaclass=DSPStructuredModelMeta):
    """
    Base class for structured models
    """

    def __init__(self, values: Dict[str, Any], root: 'DSPStructuredModel' = None, parent: 'DSPStructuredModel' = None,
                 accept_derived_values: bool = True, ignore_cached_values: Set[str] = None):
        """
        Args:
            values: Underlying data store for the model
            root: The model at the root of the current hierarchy
            parent: The immediate parent model of this instance within the current hierarchy
            accept_derived_values: Whether to accept derived values specified within the given values
            ignore_cached_values: Set of cached values that will be re-derived by the model
        """
        if parent is None:
            assert root is self or root is None
        else:
            assert root is not self and parent is not self

        if ignore_cached_values:
            derived_fields = self.get_field_names(DSPStructuredModel.derived_fields_only)

            # remove keys present in ignore cached values
            self.values = {key: value for key, value in values.items() if
                           key not in derived_fields or key not in ignore_cached_values}
        else:
            self.values = copy(values)
        del values

        self.root = root if root is not None else self
        self.parent = parent

        def coerce_type(
                attribute: str,
                target_type: Type,
                value: Any,
                root: DSPStructuredModel,
                parent: DSPStructuredModel,
                ignore_cached_values: Set[str]
        ):
            def _check_already_null_or_typecast(_value: Any, _target_type: Type) -> bool:
                return _value is None or isinstance(_value, _target_type)
            
            if safe_issubclass(target_type, List):
                list_member_type = target_type.__args__[0]
                if isinstance(list_member_type, TypeVar):
                    return value
                else:
                    try:
                        return [vls if _check_already_null_or_typecast(vls, list_member_type) 
                                else list_member_type(vls) 
                                for vls in value]
                    except Exception:
                        raise AssertionError('Not all elements of {} can be cast as required type {}'.format(
                            attribute, list_member_type.__name__)
                        )

            if _check_already_null_or_typecast(value, target_type):
                return value

            if safe_issubclass(target_type, DSPStructuredModel):
                return target_type(value, root=root, parent=parent, ignore_cached_values=ignore_cached_values)

            try:
                return target_type(value)
            except Exception:
                raise AssertionError('{} is not of required type {} instead is {}'.format(
                    attribute, target_type.__name__, type(value))
                )

        for attribute_name, attribute_spec in self.__attribute_specs__.items():
            if isinstance(attribute_spec, (SubmittedAttribute, AssignableAttribute)):
                if safe_issubclass(attribute_spec.value_type, META) and attribute_name not in self.values:
                    continue
                self.values[attribute_name] = coerce_type(
                    attribute=attribute_name, target_type=attribute_spec.value_type,
                    value=self.values.get(attribute_name), root=self.root, parent=self,
                    ignore_cached_values=ignore_cached_values
                )

            elif isinstance(attribute_spec, RepeatingSubmittedAttribute):
                submitted_values = self.values.get(attribute_name, [])
                self.values[attribute_name] = [
                    coerce_type(
                        attribute=attribute_name, target_type=attribute_spec.value_type,
                        value=element, root=self.root, parent=self, ignore_cached_values=ignore_cached_values
                    )
                    for element in submitted_values
                ]

        if not accept_derived_values:
            derived_fields = self.get_field_names(DSPStructuredModel.derived_fields_only)
            submitted_derived_fields = set(derived_fields).intersection(self.values.keys())
            if submitted_derived_fields:
                raise ValueError("Derived fields submitted for {}: {}".format(type(self).__name__,
                                                                              submitted_derived_fields))

    def __iter__(self):
        return iter(self.__attribute_specs__)

    def __eq__(self, other):
        if not type(self) is type(other):
            return False

        if type(self).__attribute_specs__ != type(other).__attribute_specs__:
            return False

        for field_name in self:
            if getattr(self, field_name) != getattr(other, field_name):
                return False

        return True

    def as_dict(self) -> Dict:
        dikt = {}
        for attr_name, _ in self.__attribute_specs__.items():
            attr_value = getattr(self, attr_name)
            if isinstance(attr_value, DSPStructuredModel):
                attr_value = attr_value.as_dict()
            elif isinstance(attr_value, list):
                attr_value = [element.as_dict()
                              if isinstance(element, DSPStructuredModel) else element
                              for element in attr_value]

            dikt[attr_name] = attr_value
        return dikt

    def as_row(self, force_derivation: bool = True, meta_available: bool = True,
               additional_derivations: Callable[['DSPStructuredModel'], dict] = None) -> t.Row:

        dikt = {}

        for attr_name, attr_spec in self.__attribute_specs__.items():
            if isinstance(attr_spec, DerivedAttributePlaceholder) and not force_derivation:
                continue
            if safe_issubclass(attr_spec.value_type, META) and not meta_available:
                continue

            try:
                attr_value = getattr(self, attr_name)
            except Exception as exc:
                raise ValueError('Unable to derive {} on {}'.format(attr_name, self)) from exc

            if isinstance(attr_value, DSPStructuredModel):
                attr_value = attr_value.as_row(force_derivation, meta_available, additional_derivations)
            elif isinstance(attr_value, list):
                attr_value = [element.as_row(force_derivation, meta_available, additional_derivations)
                              if isinstance(element, DSPStructuredModel) else element
                              for element in attr_value]

            dikt[attr_name] = attr_value

        if additional_derivations:
            additional_attrs = additional_derivations(self)
            dikt.update(**additional_attrs)

        return t.Row(**dikt)

    def as_json(self):
        return json.dumps(self.as_dict(), cls=ISODateEncoder)

    @classmethod
    def from_dataframe(cls: Type[T], df: DataFrame) -> RDD:
        return df.rdd.map(cls.from_row)

    @classmethod
    def from_row(cls: Type[T], row: t.Row) -> T:
        return cls(row.asDict(True))

    @classmethod
    def get_model_attributes(cls, predicate: Callable[[str, ModelAttribute], bool] = lambda _, __: True) \
            -> Generator[Tuple[str, ModelAttribute], None, None]:
        for name, attribute in cls.__attribute_specs__.items():
            if predicate(name, attribute):
                yield (name, attribute)

    @classmethod
    def get_field_names(cls, predicate: Callable[[str, ModelAttribute], bool] = lambda _, __: True) -> List[str]:
        return [name for name, _ in cls.get_model_attributes(predicate)]

    @classmethod
    def get_fields(cls) -> Dict[str, str]:
        fields = dict([(name, field_type.field) for name, field_type in cls.get_model_attributes()])

        return fields

    @classmethod
    def get_struct(
            cls, include_derived_attr: bool = True, meta_available: bool = True,
            additional_fields: Callable[[Type['DSPStructuredModel']], dict] = None,
            use_spec_spark_type: bool = False
    ) -> T:
        return get_spark_type(
            cls, include_derived_attr, meta_available, additional_fields, use_spec_spark_type=use_spec_spark_type
        )

    @classmethod
    def get_fields_sql(cls, use_spec_spark_type: bool = False) -> str:
        return get_sql(cls.get_struct(use_spec_spark_type=use_spec_spark_type), is_root=True)

    @staticmethod
    def submitted_fields_only(_: str, field_type: ModelAttribute) -> bool:
        return isinstance(field_type, (SubmittedAttribute, RepeatingSubmittedAttribute))

    @staticmethod
    def derived_fields_only(_: str, field_type: ModelAttribute) -> bool:
        return isinstance(field_type, (DerivedAttribute, DerivedAttributePlaceholder))


class BaseTableModel(DSPStructuredModel):
    Root = None  # type: ModelAttribute
    Parent = None  # type: ModelAttribute
    __table__ = None  # type: str


class _META(DSPStructuredModel):
    EVENT_ID = SubmittedAttribute(CommonFields.EVENT_ID, str)  # type: SubmittedAttribute
    RECORD_INDEX = SubmittedAttribute(CommonFields.RECORD_INDEX, int)  # type: SubmittedAttribute
    EVENT_RECEIVED_TS = SubmittedAttribute(CommonFields.EVENT_RECEIVED_TS, datetime)  # type: SubmittedAttribute
    DATASET_VERSION = SubmittedAttribute(CommonFields.DATASET_VERSION, str)  # type: SubmittedAttribute
    RECORD_VERSION = SubmittedAttribute(CommonFields.RECORD_VERSION, int)  # type: SubmittedAttribute


class _CLINICAL(DSPStructuredModel):
    NicipId = SubmittedAttribute('NicipId', str)
    NicipDescription = SubmittedAttribute('NicipDescription', str)
    SnomedCtId = SubmittedAttribute('SnomedCtId', str)
    SnomedCtDescription = SubmittedAttribute('SnomedCtDescription', str)
    ModalityId = SubmittedAttribute('ModalityId', str)
    Modality = SubmittedAttribute('Modality', str)
    SubModalityId = SubmittedAttribute('SubModalityId', str)
    SubModality = SubmittedAttribute('SubModality', str)
    RegionId = SubmittedAttribute('RegionId', str)
    Region = SubmittedAttribute('Region', str)
    SubRegionId = SubmittedAttribute('SubRegionId', str)
    SubRegion = SubmittedAttribute('SubRegion', str)
    SystemId = SubmittedAttribute('SystemId', str)
    System = SubmittedAttribute('System', str)
    SubSystemId = SubmittedAttribute('SubSystemId', str)
    SubSystem = SubmittedAttribute('SubSystem', str)
    SubSystemComponentId = SubmittedAttribute('SubSystemComponentId', str)
    SubSystemComponent = SubmittedAttribute('SubSystemComponent', str)
    MorphologyId = SubmittedAttribute('MorphologyId', str)
    Morphology = SubmittedAttribute('Morphology', str)
    FetalId = SubmittedAttribute('FetalId', str)
    Fetal = SubmittedAttribute('Fetal', str)
    EarlyDiagnosisOfCancer = SubmittedAttribute('EarlyDiagnosisOfCancer', str)
    SubEarlyDiagnosisOfCancer = SubmittedAttribute('SubEarlyDiagnosisOfCancer', str)


class CLINICAL(_CLINICAL):
    __concrete__ = True


class _MPSConfidenceScores(DSPStructuredModel):
    MatchedAlgorithmIndicator = SubmittedAttribute('MatchedAlgorithmIndicator', str)  # type: SubmittedAttribute
    MatchedConfidencePercentage = SubmittedAttribute('MatchedConfidencePercentage',
                                                     Decimal_5_2)  # type: SubmittedAttribute
    FamilyNameScorePercentage = SubmittedAttribute('FamilyNameScorePercentage',
                                                   Decimal_5_2)  # type: SubmittedAttribute
    GivenNameScorePercentage = SubmittedAttribute('GivenNameScorePercentage',
                                                  Decimal_5_2)  # type: SubmittedAttribute
    DateOfBirthScorePercentage = SubmittedAttribute('DateOfBirthScorePercentage',
                                                    Decimal_5_2)  # type: SubmittedAttribute
    GenderScorePercentage = SubmittedAttribute('GenderScorePercentage', Decimal_5_2)  # type: SubmittedAttribute
    PostcodeScorePercentage = SubmittedAttribute('PostcodeScorePercentage', Decimal_5_2)  # type: SubmittedAttribute


class META(_META):
    __concrete__ = True


class MPSConfidenceScores(_MPSConfidenceScores):
    __concrete__ = True
