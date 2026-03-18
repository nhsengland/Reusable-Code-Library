import importlib 
import itertools
from typing import Dict, List, Generator, Type, Optional, Iterable, FrozenSet

import dsp.datasets.models.iapt as iapt
import dsp.datasets.models.iapt_v2_1 as iapt_v2_1
import dsp.datasets.models.csds as csds
import dsp.datasets.models.csds_v1_6 as csds_v1_6
import dsp.datasets.models.mhsds_v5 as mhsds_v5
import dsp.datasets.models.mhsds_v6 as mhsds_v6
import dsp.datasets.models.msds as msds
import dsp.datasets.models.pcaremeds as pcaremeds
import dsp.datasets.models.p2c as p2c
import dsp.datasets.models.epmawspc2 as epmawspc2
import dsp.datasets.models.epmawsad2 as epmawsad2
import dsp.datasets.models.epmanationaladm as epmanationaladm
import dsp.datasets.models.epmanationalpres as epmanationalpres

from dsp.common.structured_model import RepeatingSubmittedAttribute, AssignableAttribute, \
    SubmittedAttribute, DerivedAttributePlaceholder, DSPStructuredModel, BaseTableModel
from dsp.shared import safe_issubclass
from dsp.shared.constants import DS


class Destructurer:
    ROOT_MODELS = {
        DS.IAPT_GENERIC: iapt.Referral,
        DS.IAPT_V2_1: iapt_v2_1.Referral,
        DS.CSDS_GENERIC: csds.MPI,
        DS.CSDS_V1_6: csds_v1_6.MPI,
        DS.MHSDS_V1_TO_V5_AS_V6: mhsds_v5.Referral,
        DS.MHSDS_V6: mhsds_v6.Referral,
        DS.MSDS: msds.PregnancyAndBookingDetails,
        DS.PCAREMEDS: pcaremeds.PrimaryCareMedicineModel,
        DS.P2C: p2c.P2CModel,
        DS.EPMAWSPC2: epmawspc2.EPMAWellSkyPrescriptionModel2,
        DS.EPMAWSAD2: epmawsad2.EPMAWellSkyAdministrationModel2,
        DS.EPMANATIONALADM: epmanationaladm.EPMAAdministrationModel,
        DS.EPMANATIONALPRES: epmanationalpres.EPMAPrescriptionModel
    }

    ANONYMOUS_MODELS = {
        DS.IAPT_GENERIC: frozenset(iapt.get_anonymous_models()),
        DS.IAPT_V2_1: frozenset(iapt_v2_1.get_anonymous_models()),
        DS.CSDS_GENERIC: frozenset(csds.get_anonymous_models()),
        DS.CSDS_V1_6: frozenset(csds_v1_6.get_anonymous_models()),
        DS.MHSDS_V1_TO_V5_AS_V6: frozenset(mhsds_v5.get_anonymous_models()),
        DS.MHSDS_V6: frozenset(mhsds_v6.get_anonymous_models()),
        DS.MSDS: frozenset(msds.get_anonymous_models()),
    }

    BASE_MODEL_TYPE = {
        DS.IAPT_GENERIC: iapt.BaseIAPT,
        DS.IAPT_V2_1: iapt_v2_1.BaseIAPT_V2_1,
        DS.CSDS_GENERIC: csds.BaseCSDS,
        DS.CSDS_V1_6: csds_v1_6.BaseCSDS_V1_6,
        DS.MHSDS_V1_TO_V5_AS_V6: mhsds_v5.BaseMHSDS_V5,
        DS.MHSDS_V6: mhsds_v6.BaseMHSDS_V6,
        DS.MSDS: msds.BaseMSDS,
    }

    DISTINCT_NESTED = {
        DS.IAPT_GENERIC: lambda x: "Referral:Patient" in x,
        DS.IAPT_V2_1: lambda x: "Referral:Patient" in x,
        DS.CSDS_GENERIC: lambda x: False,
        DS.CSDS_V1_6: lambda x: False,
        DS.MHSDS_V1_TO_V5_AS_V6: lambda x: "Referral:Patient" in x,
        DS.MHSDS_V6: lambda x: "Referral:Patient" in x,
        DS.MSDS: lambda x: "PregnancyAndBookingDetails:Mother" in x
    }

    DISTINCT_TABLE = {
        DS.IAPT_GENERIC: lambda x: x in ('MPI', 'Header'),
        DS.IAPT_V2_1: lambda x: x in ('MPI', 'Header'),
        DS.CSDS_GENERIC: lambda x: x == 'Header',
        DS.CSDS_V1_6: lambda x: x == 'Header',
        DS.MHSDS_V1_TO_V5_AS_V6: lambda x: x in ('MasterPatientIndex', 'Header'),
        DS.MHSDS_V6: lambda x: x in ('MasterPatientIndex', 'Header'),
        DS.MSDS: lambda x: x in ('MotherDemog', 'Header'),
    }

    GET_ANONYMOUS_MODELS = {
        DS.IAPT_GENERIC: iapt.get_anonymous_models,
        DS.IAPT_V2_1: iapt_v2_1.get_anonymous_models,
        DS.CSDS_GENERIC: csds.get_anonymous_models,
        DS.CSDS_V1_6: csds_v1_6.get_anonymous_models,
        DS.MHSDS_V1_TO_V5_AS_V6: mhsds_v5.get_anonymous_models,
        DS.MHSDS_V6: mhsds_v6.get_anonymous_models,
        DS.MSDS: msds.get_anonymous_models
    }

    def __init__(
            self, dataset_id: str, columns_tables_map: Dict[str, List[str]] = None,
            default_columns: List[str] = None, postfix: str = "", definition: str = None
    ):
        self.columns_tables_map = columns_tables_map or {}
        self.default_columns = default_columns or []
        self.postfix = postfix
        self.dataset_id = dataset_id
        self.root_model = self.ROOT_MODELS[dataset_id]
        self.root_dict = {}  # type: Dict[str, str]
        self.non_nested_array_tables = {}  # type: Dict[str, str]
        self.definition_module = importlib.import_module(definition) if definition else None

    @staticmethod
    def get_table_name(model: Type[BaseTableModel]):
        return model.__table__

    @classmethod
    def get_root_model_type(cls, dataset_id: str) -> Type[DSPStructuredModel]:
        """
        Args:
            dataset_id: The ID of the dataset to retrieve the model for

        Returns:
            The root model type of the denormalised schema of this dataset
        """
        return cls.ROOT_MODELS[dataset_id]

    @classmethod
    def get_anonymous_model_types(cls, dataset_id: str) -> FrozenSet[Type[DSPStructuredModel]]:
        """
        Args:
            dataset_id: The ID of the dataset to retrieve models for

        Returns:
            A set of model types of schemas of data detached from the primary denormalised schema of this dataset
        """
        return cls.ANONYMOUS_MODELS[dataset_id]

    def get_qualified_table_name(self, table_name, nested_structure):
        return self._get_doc_string(table_name) if self._get_doc_string(table_name) else nested_structure[-1]

    def _get_doc_string(self, table_name: str):
        return getattr(importlib.import_module("dsp.datasets.models.{}".format(self.dataset_id)), table_name).__doc__

    @staticmethod
    def _get_create_query_string(use_temp: bool, target_db: str, source_db: str, source_table: str, table_name: str,
                                 distinct: bool, columns: str, alias: str, postfix: str):
        return """
    CREATE OR REPLACE {use_temp}VIEW {target_db}`{table_name}{postfix}` AS 
    SELECT {distinct}{columns}
    FROM {source_db}`{source_table}` AS `{alias}`""".format(
            use_temp="TEMP " if use_temp else "",
            target_db="`{}`.".format(target_db) if target_db else "",
            source_db="`{}`.".format(source_db) if source_db else "",
            source_table=source_table,
            table_name=table_name,
            distinct='DISTINCT ' if distinct else '',
            columns=columns,
            alias=alias,
            postfix=postfix
        )

    def _get_common_columns(self, table_name: str) -> List[str]:

        if table_name in self.columns_tables_map.keys():
            return self.columns_tables_map[table_name]

        return self.default_columns

    def filter_columns_for_view(
            self, table_name: str, columns: Iterable[str], nested_structure: dict
    ) -> Generator[str, None, None]:


        if not self.definition_module:
            yield from columns
            return

        qualified_table_name = self.get_qualified_table_name(table_name, nested_structure)

        table_view_class = getattr(self.definition_module, qualified_table_name)

        def get_col_name(column: str):
            return column.split('.')[-1]

        column_name_to_locations = {get_col_name(column): column for column in columns}

        included_fields = table_view_class.IncludedFields
        if not table_view_class.MaintainOrder:
            included_fields.sort()

        custom_locations = table_view_class.Locations
        for field in included_fields:
            if field in custom_locations:
                yield custom_locations[field]
                continue
            if field not in column_name_to_locations:
                raise KeyError(f'field location not found: {field} from {table_name}')

            yield column_name_to_locations[field]

    def get_sql_for_struct_type(self, use_temp: bool, table_dict: dict, table_name: str, target_db: str,
                                table_fields: dict, source_db: str, source_table: str) -> Generator[str, None, None]:
        if not table_name:
            return

        item = table_dict.get(table_name)

        if not item:
            return

        nested_structure = item.split(':')

        common_columns = self._get_common_columns(table_name)

        distinct = self.DISTINCT_TABLE[self.dataset_id](table_name)

        columns = itertools.chain(
            ('{}.{}'.format(item.replace(':', '.'), f)
             for f in table_fields.get(table_name)
             if len([common_column for common_column in common_columns if common_column.endswith(f)]) == 0)
        )

        columns = ', '.join(itertools.chain(
            ("{}.{}".format(self.root_model.__name__, c) if not c.startswith('_') else c for c in common_columns),
            self.filter_columns_for_view(table_name, columns, nested_structure)
        ))

        if not columns:
            return

        query = Destructurer._get_create_query_string(
            use_temp=use_temp,
            target_db=target_db,
            source_db=source_db,
            source_table=source_table,
            table_name=self.get_qualified_table_name(table_name, nested_structure),
            distinct=distinct,
            columns=columns,
            alias=nested_structure[0],
            postfix=self.postfix
        )

        yield query

    def get_sql_for_array_type(self, use_temp: bool, table_dict: dict, table_name: str, target_db: str,
                               table_fields: dict, source_db: str, source_table: str) \
            -> Generator[str, None, None]:
        item = table_dict.get(table_name)
        if not item:
            return

        nested_structure = item.split(':')

        common_columns = self._get_common_columns(table_name)

        columns = itertools.chain(
            ('_{}.{}'.format(nested_structure[-1], f)
             for f in table_fields.get(table_name))
        )

        columns = ', '.join(itertools.chain(
            ("{}.{}".format(self.root_model.__name__, c) if not c.startswith('_') else c for c in common_columns),
            self.filter_columns_for_view(table_name, columns, nested_structure)
        ))

        if not columns:
            return

        query = [Destructurer._get_create_query_string(
            use_temp=use_temp,
            target_db=target_db,
            source_db=source_db,
            source_table=source_table,
            table_name=self.get_qualified_table_name(table_name, nested_structure),
            columns=columns,
            distinct=False,
            alias=nested_structure[0],
            postfix=self.postfix
        )]

        parent = nested_structure[0]

        for child in nested_structure[1:]:
            alias = '_{child}'.format(child=child)
            query.append(
                "LATERAL VIEW EXPLODE({parent}.{child}) AS {alias}".format(
                    parent=parent,
                    child=child,
                    alias=alias
                )
            )
            parent = alias

        yield "\n".join(query)

    def get_sql_for_struct_array_type(self, use_temp: bool, table_dict: dict, table_name: str, target_db: str,
                                      table_fields: dict, source_db: str, source_table: str) \
            -> Generator[str, None, None]:

        item = table_dict.get(table_name)

        if not item:
            return

        nested_structure = item.split(':')

        common_columns = self._get_common_columns(table_name)

        distinct = self.DISTINCT_NESTED[self.dataset_id](item)

        columns = itertools.chain(
            ('_{}.{}'.format(nested_structure[-1], f)
             for f in table_fields.get(table_name))
        )

        columns = ', '.join(itertools.chain(
            ("{}.{}".format(self.root_model.__name__, c) if not c.startswith('_') else c for c in common_columns),
            self.filter_columns_for_view(table_name, columns, nested_structure)
        ))

        if not columns:
            return

        query = [Destructurer._get_create_query_string(
            use_temp=use_temp,
            target_db=target_db,
            source_db=source_db,
            source_table=source_table,
            table_name=self.get_qualified_table_name(table_name, nested_structure),
            distinct=distinct,
            columns=columns,
            alias=nested_structure[0],
            postfix=self.postfix
        )]

        parent = '{}.{}'.format(nested_structure[0], nested_structure[1])

        for child in nested_structure[2:]:
            alias = '_{child}'.format(child=child)
            query.append(
                "LATERAL VIEW EXPLODE({parent}.{child}) AS {alias}".format(
                    parent=parent,
                    child=child,
                    alias=alias
                )
            )
            parent = alias

        yield "\n".join(query)

    def _create_nested_structure(self, model: Type[DSPStructuredModel], table_fields: dict = None,
                                 parent_list: list = None):
        nested_fields = []
        flatten_fields = []
        nested_field_dict = {}
        flat_field_dict = {}

        self.root_dict.update([(model.__name__, ':'.join(parent_list) if parent_list else model.__name__)])

        for name, tpe in model.get_model_attributes():

            if isinstance(tpe, RepeatingSubmittedAttribute):
                nested_fields.append(tpe.value_type)
                nested_field_dict.update([(tpe.value_type, name)])
                continue

            if not isinstance(tpe, (DerivedAttributePlaceholder, SubmittedAttribute, AssignableAttribute)):
                continue

            # noinspection PyTypeHints
            if safe_issubclass(tpe.value_type, self.BASE_MODEL_TYPE[self.dataset_id]):
                flatten_fields.append(tpe.value_type)
                flat_field_dict.update([(tpe.value_type, name)])

                self.non_nested_array_tables.update([(tpe.value_type.__name__, name)])

            else:
                table_fields.setdefault(model.__name__, []).append(name)

        for flatten_field in flatten_fields:
            column_name = flat_field_dict.get(flatten_field)
            self._create_nested_structure(
                flatten_field, table_fields, [
                    self.root_dict.get(model.__name__), column_name
                ] if self.root_dict.get(model.__name__) else [column_name]
            )

        for nested_field in nested_fields:
            column_name = nested_field_dict.get(nested_field)

            self._create_nested_structure(
                nested_field, table_fields, [
                    self.root_dict.get(model.__name__), column_name
                ] if self.root_dict.get(model.__name__) else [column_name]
            )

        return table_fields

    def generate_view_sql(self, target_db: Optional[str], source_db: Optional[str], source_table: str, use_temp: bool,
                          explicit_anonymous: Dict[str, Type[DSPStructuredModel]] = None) \
            -> Generator[str, None, None]:
        table_fields = self._create_nested_structure(self.root_model, table_fields={})
        f = []

        yield from self.get_sql_for_struct_type(
            use_temp, self.root_dict, self.root_model.__name__, target_db, table_fields, source_db, source_table
        )

        self.root_dict.pop(self.root_model.__name__)  # remove root table as not required anymore

        for k, v in self.non_nested_array_tables.items():  # build views for non nested array column tables
            yield from self.get_sql_for_struct_type(
                use_temp, self.root_dict, k, target_db, table_fields, source_db, source_table
            )

            self.non_nested_array_tables.update([(k, self.root_dict.get(k))])
            self.root_dict.pop(k)  # remove non nested tables as not required anymore

        for k, v in self.non_nested_array_tables.items():  # build a list for columns of struct type with array list

            for value in self.root_dict.values():
                if v in value:
                    f.append(v)
                    break

        for table, hierarchy in self.root_dict.items():
            if len(f) > 0:
                for item in f:
                    if item in hierarchy:
                        yield from self.get_sql_for_struct_array_type(
                            use_temp, self.root_dict, table, target_db, table_fields, source_db, source_table
                        )
                    else:
                        yield from self.get_sql_for_array_type(use_temp, self.root_dict, table, target_db, table_fields,
                                                               source_db, source_table)
            else:
                yield from self.get_sql_for_array_type(
                    use_temp, self.root_dict, table, target_db, table_fields, source_db, source_table
                )

        # This shouldn't be necessary once anonymous tables exist in submission output
        if explicit_anonymous is None:
            for model in self.GET_ANONYMOUS_MODELS[self.dataset_id]():
                yield self.get_sql_for_anonymous_table(model, use_temp, target_db, source_db)
        else:
            for table_name, model in explicit_anonymous.items():
                yield self.get_sql_for_anonymous_table(model, use_temp, target_db, source_db, table_name)

    def get_sql_for_anonymous_table(
            self, model: Type[DSPStructuredModel], use_temp: bool, target_db: str = None, source_db: str = None,
            source_table: str = None
    ):
        table_name = model.__table__
        common_cols = self._get_common_columns(table_name)

        columns = [
            name for name, tpe in model.get_model_attributes()
            if name not in {*common_cols, 'Header'}
        ]

        columns = ', '.join(itertools.chain(
            common_cols, self.filter_columns_for_view(model.__name__, columns, {})
        ))

        if not source_table:
            source_table = table_name[:6]

        return """
    CREATE OR REPLACE {use_temp}VIEW {target_db}`{table_name}{postfix}` AS 
    SELECT {columns}
    FROM {source_db}`{source_table}`""".format(
            use_temp="TEMP " if use_temp else "",
            target_db="`{}`.".format(target_db) if target_db else "",
            source_db="`{}`.".format(source_db) if source_db else "",
            source_table=source_table,
            table_name=table_name,
            columns=columns,
            postfix=self.postfix
        )
