import functools
from typing import Dict, Generator, Iterable, List, Sequence, Tuple, TypeVar, Generic, Type

from schematics import models as schematics_models

from nhs_reusable_code_library.resuable_codes.shared import logger, safe_issubclass
from nhs_reusable_code_library.resuable_codes.shared import models
from nhs_reusable_code_library.resuable_codes.shared.aws import ddb_table, dynamodb_retry_backoff
from nhs_reusable_code_library.resuable_codes.shared.logger import debug_fields, add_fields


class ModelNotFound(ValueError):
    pass


TModel = TypeVar('TModel', covariant=True)


class BaseStore(Generic[TModel]):
    _table_name = None  # type: str
    _keys = None  # type: Sequence[str]

    class SchemaMigrations:
        pass

    def __init__(self, model_type: Type[TModel], table=None):
        self._table = table
        self._model_type = model_type

    def __getstate__(self):
        attributes = self.__dict__.copy()
        attributes["_table"] = None
        return attributes

    @classmethod
    @property
    def table_name(cls):
        return cls._table_name

    @property
    def table(self):
        if not self._table:
            self._table = ddb_table(self._table_name)
        return self._table

    def migrate_schema_if_required(self, item: Dict, target_version: int = None) -> Tuple[Dict, bool]:

        if not safe_issubclass(self._model_type, models.VersionedModel):
            return item, False

        model_version = item.get('version', 0)
        uplifting_class = getattr(self, 'SchemaMigrations')()
        uplifted = False

        target_version = target_version or self._model_type.CURRENT_VERSION

        if model_version < target_version:
            for version in range(int(model_version) + 1, target_version + 1):
                try:
                    uplifting_method = getattr(uplifting_class, 'from_{}_to_{}'.format(version - 1, version))
                    uplifted |= bool(uplifting_method(item))
                except AttributeError:
                    raise NotImplementedError('Unable to uplift {} to {}'.format(self._model_type, version))

        return item, uplifted

    @logger.log_action()
    @dynamodb_retry_backoff()
    def get(self, key: Dict, consistent_read: bool = False, raise_if_not_found: bool = True):

        add_fields(**key)

        item = self.table.get_item(Key=key, ConsistentRead=consistent_read)
        if 'Item' not in item:
            if raise_if_not_found:
                raise ModelNotFound
            return None

        item = item['Item']

        debug_fields(lambda: item)

        return self.uplift_item(item)

    def uplift_item(self, item):
        item_uplifted, _changed = self.migrate_schema_if_required(item)
        return self.item_to_model(item_uplifted)

    @logger.log_action()
    @dynamodb_retry_backoff()
    def delete(self, key: Dict) -> None:
        add_fields(**key)
        _ret = self.table.delete_item(
            Key=key
        )

    @logger.log_action()
    @dynamodb_retry_backoff()
    def put(self, model: schematics_models.Model, condition_expression: str = None):
        item = self.model_to_item(model)
        add_fields(**item)

        response = (
            self.table.put_item(Item=item, ConditionExpression=condition_expression) if condition_expression
            else self.table.put_item(Item=item)
        )
        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        return response

    @logger.log_action()
    def truncate_table(self):
        assert self._keys is not None

        for item in self.table.scan(
                AttributesToGet=self._keys,
                ConsistentRead=True
        )['Items']:
            self.table.delete_item(
                Key={k: item[k] for k in self._keys}
            )

    @logger.log_action()
    def remove_test_scope(self, test_scope: str):

        if not test_scope:
            return

        for item in self.table.scan(AttributesToGet=(list(self._keys) + ['test_scope']), ConsistentRead=True)['Items']:
            if item.get('test_scope') != test_scope:
                continue

            self.table.delete_item(
                Key={k: item[k] for k in self._keys}
            )

    def items_to_models(self, items: Iterable) -> List:
        return list(map(self.item_to_model, items))

    def item_to_model(self, item: Dict, partial_deserialization=True) -> TModel:
        deserialize = functools.partial(self._model_type, partial=partial_deserialization)
        return deserialize(item)

    @staticmethod
    def model_to_item(model: TModel) -> Dict:
        primitive = model.to_primitive()
        if not isinstance(primitive, dict):
            return primitive
        return {k: v for k, v in primitive.items() if v is not None and v != ''}

    def models_to_items(self, models_to_convert: Sequence[TModel]) -> Generator[Dict, None, None]:
        for model in models_to_convert:
            yield self.model_to_item(model)

    def query(self, max_results=None, **kwargs):
        paginator = self.table.meta.client.get_paginator('query')
        page_iterator = paginator.paginate(TableName=self._table_name, **kwargs)
        results = 0
        for page in page_iterator:
            if max_results is not None and results >= max_results:
                break
            yield from page['Items']
            results += 1
