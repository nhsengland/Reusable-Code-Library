import os
import tempfile
from collections import namedtuple
from typing import Tuple, Set, Optional

from dsp.common import jaydebeapi_conf
from dsp.common.relational import Table, TableField
from dsp.common.spark_helpers import empty_array, chain_joins, to_struct_type
from dsp.datasets.common import Cardinality
from dsp.datasets.common import Fields as Common
from dsp.datasets.definitions.dq.schema import DQ_MESSAGE_SCHEMA
from dsp.datasets.definitions.mhsds_v5.submission_constants import *
from dsp.datasets.validations.mhsds_v5.validation_functions import get_required_joins, get_dq_descriptor
from dsp.pipeline import get_ingestion_output_location, Metadata, ValidationResult
from dsp.validation.common_validation_functions import apply_rules, apply_referential_integrity_rules, is_valid_record
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (collect_list, struct, first, when, col, current_timestamp, size, explode,
                                   concat, array_contains, array)
from pyspark.sql.types import StructType, StructField, IntegerType
from shared.aws import local_mode
from shared.common.test_helpers import smart_download
from shared.constants import PATHS


def load_access_db_to_spark_context(spark: SparkSession, access_db_path: str):
    conn = jaydebeapi_conf.connect_to_access_db(access_db_path)
    
    try:
        cursor = conn.cursor()

        for table in ALL_TABLES.values():
            table_spark_schema = to_struct_type(table)

            submitted_fields = ", ".join([schema_field.name for schema_field in table_spark_schema])

            cursor.execute("SELECT {} FROM {}".format(submitted_fields, table.name))
            results = cursor.fetchall()
            results = [(index, *result) for index, result in enumerate(results)]

            dataframe_schema = StructType([
                StructField('RowNumber', IntegerType()),
                *table_spark_schema.fields
            ])
            spark.createDataFrame(results, dataframe_schema).alias(table.name).createOrReplaceTempView(table.name)

    finally:
        conn.close()

    return {table.name for table in ALL_TABLES.values()}


def validate_dataframes(spark: SparkSession, metadata: Metadata) -> ValidationResult:
    from dsp.datasets.validations.mhsds_v5.validations import DATAFRAME_VALIDATIONS, REFERENTIAL_INTEGRITY_VALIDATIONS

    apply_rules(spark, *DATAFRAME_VALIDATIONS)
    apply_referential_integrity_rules(spark, *REFERENTIAL_INTEGRITY_VALIDATIONS)
    validation_result = _get_validation_results(spark, metadata)

    for table in ALL_TABLES.values():
        spark.table(table.name).drop(Common.META).createOrReplaceTempView(table.name)

    return validation_result


def _get_validation_results(
        spark: SparkSession,
        metadata: Metadata
) -> ValidationResult:
    dq_error_count = 0
    dq_df = None

    for table in ALL_TABLES.values():
        table_df = spark.table(table.name)
        if Common.DQ not in table_df.schema.names:
            continue

        table_dq_df = _prepare_dq_results(spark, table_df, table.name)

        if table_dq_df is not None:
            if dq_df is not None:
                dq_df = dq_df.union(table_dq_df)
            else:
                dq_df = table_dq_df

        table_df.where(is_valid_record(Common.DQ)).drop(Common.DQ).createOrReplaceTempView(table.name)

    if dq_df is not None:
        output_folder = get_ingestion_output_location(metadata)
        dq_results_path = os.path.join(output_folder, PATHS.DQ)
        dq_df.write.mode('append').parquet(dq_results_path)

        dq_error_count += dq_df.count()

    return ValidationResult(total_records=0, dq_error_count=dq_error_count, dq_warning_count=0)


def _prepare_dq_results(spark: SparkSession, df: DataFrame, table_name: str) -> Optional[DataFrame]:
    df_with_dqs = df.where(size(Common.DQ) > 0)
    validation_codes = _get_validation_codes(df_with_dqs)

    if not validation_codes:
        return None

    required_joins = get_required_joins(*validation_codes)

    joined_df_with_dqs = chain_joins(spark, df_with_dqs, *required_joins)

    joined_df_with_dq_descriptors = joined_df_with_dqs.withColumn(
        Common.DQ, concat(*(when(array_contains(Common.DQ, validation_code),
                                 array(get_dq_descriptor(validation_code, table_name)))
                          .otherwise(empty_array(DQ_MESSAGE_SCHEMA)) for validation_code in validation_codes))
    )

    return joined_df_with_dq_descriptors.withColumn(Common.DQ, explode(Common.DQ)) \
        .select(*[col(Common.DQ + "." + field.name).alias(field.name) for field in DQ_MESSAGE_SCHEMA]) \
        .withColumn(Common.DQ_TS, current_timestamp())


def _get_validation_codes(df: DataFrame) -> Set[str]:
    return {row[Common.DQ] for row in df.select(explode(Common.DQ).alias(Common.DQ)).distinct().collect()}


RelationshipDetail = namedtuple('RelationshipDetail', ['primary_key', 'child_tables'])


class ChildTable:

    def __init__(
            self,
            field_name: str,
            table: Table,
            cardinality: Cardinality = Cardinality.MANY,
            join_key: TableField = None
    ):
        self.field_name = field_name
        self.table = table
        self.cardinality = cardinality
        self.join_key = join_key


TABLE_RELATIONSHIPS = {
    MHS101: RelationshipDetail(MHS101['ServiceRequestId'], [
        ChildTable('Patient', MHS001, Cardinality.ONE, MHS101['LocalPatientId']),
        ChildTable('ServiceTypesReferredTo', MHS102),
        ChildTable('OtherReasonsForReferral', MHS103),
        ChildTable('ReferralsToTreatment', MHS104),
        ChildTable('OnwardReferrals', MHS105),
        ChildTable('DischargePlanAgreements', MHS106),
        ChildTable('MedicationPrescriptions', MHS107),
        ChildTable('CareContacts', MHS201),
        ChildTable('IndirectActivities', MHS204),
        ChildTable('HospitalProviderSpells', MHS501),
        ChildTable('ProvisionalDiagnoses', MHS603),
        ChildTable('PrimaryDiagnoses', MHS604),
        ChildTable('SecondaryDiagnoses', MHS605),
        ChildTable('CodedScoredAssessmentReferrals', MHS606),
    ]),

    MHS001: RelationshipDetail(MHS001['LocalPatientId'], [
        ChildTable('GPs', MHS002),
        ChildTable('AccommodationStatuses', MHS003),
        ChildTable('EmploymentStatuses', MHS004),
        ChildTable('PatientIndicators', MHS005),
        ChildTable('MentalHealthCareCoordinators', MHS006),
        ChildTable('DisabilityTypes', MHS007),
        ChildTable('CarePlanTypes', MHS008),
        ChildTable('AssistiveTechnologiesToSupportDisabilityTypes', MHS010),
        ChildTable('SocialAndPersonalCircumstances', MHS011),
        ChildTable('OverseasVisitorChargingCategories', MHS012),
        ChildTable('MentalHealthCurrencyModels', MHS013),
        ChildTable('MentalHealthActLegalStatusClassificationAssignmentPeriods', MHS401),
        ChildTable('MedicalHistoryPreviousDiagnoses', MHS601),
        ChildTable('CPACareEpisodes', MHS701),
        ChildTable('ClusteringToolAssessments', MHS801),
        ChildTable('FiveForensicPathways', MHS804),
    ]),

    MHS008: RelationshipDetail(MHS008['CarePlanID'], [
        ChildTable('CarePlanAgreements', MHS009),
    ]),

    MHS201: RelationshipDetail(MHS201['CareContactId'], [
        ChildTable('CareActivities', MHS202),
        ChildTable('OtherAttendances', MHS203),
    ]),

    MHS202: RelationshipDetail(MHS202['CareActId'], [
        ChildTable('CodedScoredAssessmentCareActivities', MHS607),
    ]),

    MHS401: RelationshipDetail(MHS401['MHActLegalStatusClassPeriodId'], [
        ChildTable('ResponsibleClinicianAssignmentPeriods', MHS402),
        ChildTable('ConditionalDischarges', MHS403),
        ChildTable('CommunityTreatmentOrders', MHS404),
        ChildTable('CommunityTreatmentOrderRecalls', MHS405),
    ]),

    MHS501: RelationshipDetail(MHS501['HospProvSpellID'], [
        ChildTable('WardStays', MHS502),
        ChildTable('AssignedCareProfessionals', MHS503),
        ChildTable('DelayedDischarges', MHS504),
        ChildTable('RestrictiveInterventionIncidents', MHS505),
        ChildTable('HospitalProviderSpellCommissionersAssignmentPeriods', MHS512),
        ChildTable('SpecialisedMentalHealthExceptionalPackageOfCares', MHS517),
    ]),

    MHS502: RelationshipDetail(MHS502['WardStayId'], [
        ChildTable('Assaults', MHS506),
        ChildTable('SelfHarms', MHS507),
        ChildTable('HomeLeaves', MHS509),
        ChildTable('LeaveOfAbsences', MHS510),
        ChildTable('AbsenceWithoutLeaves', MHS511),
        ChildTable('SubstanceMisuses', MHS513),
        ChildTable('TrialLeaves', MHS514),
        ChildTable('PoliceAssistanceRequests', MHS516),
    ]),

    MHS505: RelationshipDetail(MHS505['RestrictiveIntIncID'], [
        ChildTable('RestrictiveInterventionTypes', MHS515),
    ]),

    MHS701: RelationshipDetail(MHS701['CPAEpisodeId'], [
        ChildTable('CPAReviews', MHS702),
    ]),

    MHS801: RelationshipDetail(MHS801['ClustId'], [
        ChildTable('ClusterAssesses', MHS802),
        ChildTable('CareClusters', MHS803),
    ]),

}


def collate_children(spark: SparkSession, table: Table) -> DataFrame:
    table_df = spark.table(table.name)

    if table not in TABLE_RELATIONSHIPS:
        return table_df

    relationships = TABLE_RELATIONSHIPS[table]
    for child_table in relationships.child_tables:
        child_df = collate_children(spark, child_table.table)
        join_key = child_table.join_key if child_table.join_key else relationships.primary_key

        if child_table.cardinality == Cardinality.MANY:
            aggregation_function = collect_list
        else:
            aggregation_function = first

        grouped_child_df = child_df.groupBy(join_key.name).agg(
            aggregation_function(struct("*")).alias(child_table.field_name))

        table_df = table_df.join(grouped_child_df, on=join_key.name, how="left_outer")

        if child_table.cardinality == Cardinality.MANY:
            element_type = table_df.schema[child_table.field_name].dataType.elementType
            table_df = table_df.withColumn(child_table.field_name,
                                           when(col(child_table.field_name).isNull(), empty_array(element_type))
                                           .otherwise(col(child_table.field_name)))

    return table_df


def load_access_db(spark: SparkSession, path: str):

    with tempfile.NamedTemporaryFile() as db_copy:
        smart_download(path, db_copy.name)
        load_access_db_to_spark_context(spark, db_copy.name)


def generate_mhsds_v5(spark: SparkSession, path: str, metadata: Metadata) -> Tuple[DataFrame, ValidationResult]:
    load_access_db(spark, path)

    validation_result = validate_dataframes(spark, metadata)

    referrals_df = collate_children(spark, MHS101)
    # Join header separately, is not a standard relationship

    referrals_with_header_df = referrals_df.crossJoin(
        spark.sql("SELECT struct(*) AS Header FROM MHS000Header")).orderBy("RowNumber")

    return referrals_with_header_df, validation_result
