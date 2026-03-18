from typing import List, Generator, Type, Dict, NamedTuple
from dsp.common.structured_model import DSPStructuredModel
from dsp.datasets.models.epmawspc import EPMAWellSkyPrescriptionModel
from dsp.datasets.models.epmawsad import EPMAWellSkyAdministrationModel
from dsp.datasets.schema.helpers.sql_generators import generate_table_sql_from_model


# Define code for generating table scripts

def generate_ops_tables(
        target_db: str, using: str = 'DELTA', partition_by='ReportedDate',
        include_effective_timestamps: bool = True
) -> Generator[str, None, None]:
    yield from generate_table_sql_from_model(
        target_db, [
            ('epmawspc', EPMAWellSkyPrescriptionModel),
            ('epmawsad', EPMAWellSkyAdministrationModel),
        ],
        using=using, partition_by=partition_by,
        include_effective_timestamps=include_effective_timestamps
    )


# Define function for generating sql code for Prescribing and Administration Views

class JoinConditions(NamedTuple):
    alias: str
    join_type: str
    conditions: str
    fields_to_retrieve: List[str]


def generate_view_sql_from_single_model(
        db_name: str,
        table_name: str,
        table_alias: str,
        model: Type[DSPStructuredModel],
        include_effective_timestamps: bool = True,
        columns_to_exclude: List[str] = None,
        required_joins: Dict[str, JoinConditions] = None
) -> str:
    columns_to_exclude = [] if not columns_to_exclude else columns_to_exclude
    effective_timestamps = "" if not include_effective_timestamps else f",\n{table_alias}.EFFECTIVE_FROM,\n{table_alias}.EFFECTIVE_TO"
    required_join_logic = "" if not required_joins else "\n".join(
        [f"{val.join_type.upper()} JOIN {key} AS {val.alias} ON {val.conditions}" for key, val in
         required_joins.items()])
    join_fields_to_retrieve = "" if not required_joins else ',\n'.join(
        [f"{val.alias}.{fld}" for key, val in required_joins.items() for fld in val.fields_to_retrieve])

    query = f"CREATE OR REPLACE VIEW `{db_name}`.`vw_{table_name}` AS (\nSELECT\n"
    query += ',\n'.join([f"{table_alias}.{fld}" for fld in model.get_fields() if fld not in columns_to_exclude])
    query += '\n' + join_fields_to_retrieve
    query += effective_timestamps + f"\nFROM `{db_name}`.`{table_name}` {table_alias}"
    query += '\n' + required_join_logic

    return query


# Generate Prescription View Code
generate_view_sql_from_single_model(db_name='epma',
                                    table_name='epmawspc',
                                    model=EPMAWellSkyPrescriptionModel,
                                    include_effective_timestamps=True,
                                    columns_to_exclude=['NHS'])
# Generate Administration View Code
generate_view_sql_from_single_model(db_name='epma',
                                    table_name='epmawsad',
                                    table_alias='ad',
                                    model=EPMAWellSkyAdministrationModel,
                                    columns_to_exclude=['NHS'],
                                    include_effective_timestamps=True,
                                    required_joins={'`epma`.`epmawspc`':\
                                                        JoinConditions(alias='pc',
                                                                       join_type='left',
                                                                       conditions='CONCAT(pc.ODS,pc.AdmLink)=CONCAT(ad.ODS,ad.AdmLink)',
                                                                       fields_to_retrieve=[
                                                                           'NHSLegallyExcluded'])},
                                    )
