from abc import ABC, abstractmethod
from typing import Dict, Iterable, List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, expr, lit
from pyspark.sql.types import StringType, StructField, StructType

from nhs_reusable_code_library.resuable_codes.spark_helpers import optimize_table, vacuum_table
from dsp.datasets.models.deid import SCHEMA as DEID_SCHEMA
from dsp.pipelines.deidentify.base import DeidentifyBase
from dsp.shared.constants import DS
from dsp.shared.logger import log_action, app_logger
from dsp.shared.models import DeIdJob, DeIdJobStatus, PipelineStatus
from dsp.shared.store.base import ModelNotFound
from dsp.shared.store.deid_jobs import DeIdJobs
from dsp.shared.store.submissions import SubmissionsStore

DB_NAME = 'pseudo'
TABLE_NAME = 'pseudo'

extracts_deid_domains = {
    'historic_commissioner': {
        DS.CSDS_GENERIC: 'COMM-CSDS',
        DS.CSDS_V1_6: 'COMM-CSDS',
        DS.IAPT_GENERIC: 'COMM-IAPT',
    },
    'commissioner': {
        DS.CSDS_GENERIC: 'COMM-CSDS',
        DS.CSDS_V1_6: 'COMM-CSDS',
        DS.IAPT_GENERIC: 'COMM-IAPT',
        DS.MHSDS_GENERIC: 'COMM-MHSDS',
        DS.MSDS: 'COMM-MSDS',
        DS.MHSDS_V1_TO_V5_AS_V6: 'COMM-MHSDS',
        DS.MHSDS_V6: 'COMM-MHSDS',
        DS.IAPT_V2_1: 'COMM-IAPT'
    },
    "dscro": {DS.MSDS: "Domain-1", DS.MHSDS_GENERIC: "Domain-1"},
}


class Deidentify(DeidentifyBase):
    domain_zero = 'Domain-0'
    data_flow_job_name = 'DPS Dataflow'
    pod_policy_name = 'DPS PoD Tokenise'
    ca_cert_path = '/tmp/privitar-rdd-ca-cert.pem'
    p12_cert_path = '/tmp/privitar-rdd-cert.p12'
    aws_p12_path = '/client-certs'
    config_path = '/tmp/privitar-config.json'
    ppm_prefix = 'policy.'

    root_domain = ''
    p12_secret = ''

    @log_action(log_args=["max_jobs_in_batch", "db_name", "jobs_status_view", "pseudo_batch_view"])
    def create_pseudo_queue(
            self,
            spark: SparkSession,
            max_jobs_in_batch: int,
            run_id: int,
            db_name=DB_NAME,
            jobs_status_view="jobs_status",
            pseudo_batch_view="pseudo_batch",
    ) -> List[Row]:

        df_pseudo_batch = None

        processing_jobs = []

        ready_jobs = DeIdJobs.get_all_non_blocked_ready(self.domain_zero)

        df_current_pseudo = (
            spark.table(f"{db_name}.{TABLE_NAME}").where(col("domain") == lit(self.domain_zero)).alias("target")
        )

        count = 0

        for ready_job in ready_jobs:
            count += 1
            if count > max_jobs_in_batch:
                break

            df_incoming = (
                spark.read.parquet(ready_job.source)
                    .alias("source")
                    .withColumn("job_id", lit(str(ready_job.id)))
                    .withColumn("domain", lit(self.domain_zero))
            ).where(
                col("clear").isNotNull() & (col("clear").cast("string") != "") &
                col("pseudo_type").isNotNull() & (col("pseudo_type").cast("string") != "")
            )

            if df_pseudo_batch:
                df_pseudo_batch = df_pseudo_batch.union(df_incoming)
            else:
                df_pseudo_batch = df_incoming

            DeIdJobs.mark_processing(ready_job.id, run_id=run_id)

            processing_jobs.append(
                Row(
                    job_id=ready_job.id,
                    domain=ready_job.domain,
                    status=DeIdJobStatus.Processing,
                    submission_id=ready_job.submission_id,
                    source=ready_job.source,
                    resume_deid_submission=ready_job.resume_deid_submission or False,
                    complete_after_retokenisation=ready_job.complete_after_retokenisation or False
                )
            )

        if df_pseudo_batch:
            df_pseudo_batch = df_pseudo_batch.drop_duplicates(['clear', 'pseudo_type'])

            df_pseudo_batch = df_pseudo_batch.join(
                df_current_pseudo,
                (col("source.pseudo_type") == col("target.pseudo_type"))
                & (col("source.clear") == col("target.clear")),
                "left_anti",
            ).select("correlation_id", "clear", "pseudo_type", "domain", "job_id")

            if df_pseudo_batch.limit(1).count() > 0:
                if processing_jobs:
                    df_jobs_status = spark.createDataFrame(processing_jobs)
                    df_jobs_status.createOrReplaceTempView(jobs_status_view)

                df_pseudo_batch.createOrReplaceTempView(pseudo_batch_view)
            else:
                submission_store = SubmissionsStore()
                for job in processing_jobs:
                    if not job.complete_after_retokenisation:
                        DeIdJobs.mark_success(job['job_id'])
                    # request an extract
                    if job.resume_deid_submission and not job.complete_after_retokenisation:
                        self.resume_deid_submission(deid_job=job, submission_store=submission_store)
                return []

        return processing_jobs

    @log_action(log_args=['processing_jobs', 'db_name'])
    def merge_pseudo_output(self, spark: SparkSession, processing_jobs: List[Row], db_name=DB_NAME):

        df_jobs_status = spark.table('jobs_status')
        failed_jobs = df_jobs_status.filter(col('status').isin([DeIdJobStatus.Failed])).limit(1).count()

        if failed_jobs:
            for job in processing_jobs:
                DeIdJobs.mark_failed(job['job_id'])
            raise Exception('Jobs failed')

        sql = f"""
            INSERT INTO {db_name}.{TABLE_NAME}
            SELECT correlation_id, domain, pseudo_type, pseudo, clear, current_timestamp(), job_id
            FROM pseudo_output
            """
        try:
            spark.sql(sql)

            optimize_table(spark, db_name, TABLE_NAME)
            vacuum_table(spark, db_name, TABLE_NAME, 24)

            submission_store = SubmissionsStore()

            # get enabled flag for extract
            for job in processing_jobs:
                if not job.complete_after_retokenisation:
                    DeIdJobs.mark_success(job['job_id'])
                # request an extract
                if job.resume_deid_submission and not job.complete_after_retokenisation:
                    self.resume_deid_submission(deid_job=job, submission_store=submission_store)

        except Exception as ex:
            for job in processing_jobs:
                DeIdJobs.mark_failed(job['job_id'])
            raise ex

        # Ensure that the insert has not created any duplicates
        has_duplicates = spark.sql(
            f"SELECT domain, pseudo_type, clear, count(*) FROM {db_name}.{TABLE_NAME} "
            "GROUP BY domain, pseudo_type, clear HAVING COUNT(*)>1"
        ).count()

        if has_duplicates:
            raise Exception('Duplicate domain, clear and pseudo_type records found')

    def get_config_jobs(self) -> Dict:
        transit_job = self.get_dataflow_job()

        policy_id = self.get_policy_id()

        tokenisation_job = None

        for domain in self.get_pod_jobs(policy_id):
            if domain['name'] == self.domain_zero:
                tokenisation_job = domain['id']
                break

        if tokenisation_job is None:
            raise ValueError('Could not find tokenisation job ID for Domain-0')

        return {'transitJobId': transit_job, 'tokenisationJobId': tokenisation_job}

    def get_retokenisation_jobs(self, policy_id: str) -> List[Dict[str, str]]:
        return [job for job in self.get_pod_jobs(policy_id) if job['name'] != self.domain_zero]

    @log_action(log_args=['db_name', 'pseudo_domains_view'])
    def create_retokenisation_queue(
            self,
            spark: SparkSession,
            db_name: str = DB_NAME,
            pseudo_domains_view: str = 'pseudo_domains',
            process_new_domains: bool = False,
    ) -> bool:

        domain_jobs = []

        df_pseudo = spark.table(f'{db_name}.pseudo')

        domain_zero_df = df_pseudo.where(col('domain') == lit(self.domain_zero))
        domain_zero_count = domain_zero_df.count()

        domains_to_retokenise = self._get_domains_to_retokenise(df_pseudo, process_new_domains)

        for target_domain in domains_to_retokenise:
            target_domain_count = df_pseudo.where(col('domain') == lit(target_domain['name'])).count()

            if domain_zero_count != target_domain_count:
                domain_jobs.append(target_domain)

        if domain_jobs:
            self._create_pseudo_domains_view(spark, pseudo_domains_view, domain_jobs)
            return True

        return False

    def _create_pseudo_domains_view(
            self,
            spark: SparkSession,
            pseudo_domains_view: str,
            domain_jobs: List[Dict[str, str]],
    ) -> None:
        df_pseudo_domains = spark.createDataFrame(
            domain_jobs, StructType([StructField("id", StringType()), StructField("name", StringType())])
        )

        df_pseudo_domains.createOrReplaceTempView(pseudo_domains_view)

    def _get_domains_to_retokenise(self, df_pseudo: DataFrame, process_new_domains: bool) -> List[Dict[str, str]]:
        """
        if process_new_domains is True it returns only unpopulated domains (the new ones),
        if it is False returns domains which are populated but may require adding new records.
        """

        policy_id = self.get_policy_id()
        currently_retokenised_domains_df = df_pseudo.select("domain").distinct().where(
            col("domain") != lit(self.domain_zero)
        )
        currently_retokenised_domains = [x.domain for x in currently_retokenised_domains_df.collect()]

        currently_open_domains = self.get_open_pdds()

        currently_retokenised_open_domains = [domain for domain in currently_open_domains if domain in currently_retokenised_domains]

        if process_new_domains:
            return [job for job in self.get_retokenisation_jobs(policy_id) if job['name'] not in currently_retokenised_domains]

        return [job for job in self.get_retokenisation_jobs(policy_id) if job['name'] in currently_retokenised_open_domains]

    @log_action(log_args=["deid_job"])
    def resume_deid_submission(self, deid_job: DeIdJob, submission_store: SubmissionsStore):
        submission_id = None
        try:
            submission = submission_store.get_by_id(submission_id=deid_job.submission_id)
            if submission:
                submission_store.update_status(
                    submission.id, PipelineStatus.Pending, PipelineStatus.Suspended, raise_if_condition_fails=False
                )
                submission_id = submission.id
        except ModelNotFound:
            submission_id = None

        return submission_id


class DeidFields(ABC):
    @abstractmethod
    def de_id_fields(
            self, spark: SparkSession, df_in: DataFrame, extra_column_names: Iterable[str] = frozenset()
    ) -> DataFrame:
        """
        Returns a dataframe with pseudonomised fields
        Args:
            spark: Current spark session
            df_in: input dataframe
            extra_column_names: any extra column to be considered
        Returns:
           A dataframe with fields deided returned columns are
        'correlation_id', 'clear', 'pseudo_type', *extra_column_names
        """

    @staticmethod
    def deid_out_data(
            spark: SparkSession,
            df_in: DataFrame,
            _de_identifications: Dict[str, str],
            extra_column_names: Iterable[str] = frozenset(),
    ):
        extra_columns = [field for field in df_in.schema.fields if field.name in extra_column_names]
        assert len(extra_columns) == len(extra_column_names), \
            set(extra_column_names) - {column.name for column in extra_columns}

        deid_schema = StructType([*DEID_SCHEMA.fields, *extra_columns])
        df_out = spark.createDataFrame([], deid_schema)

        for field_path, deid_type in _de_identifications.items():
            field_name = field_path.split('.')[-1]
            df_out = df_out.union(
                df_in.select(field_path, *extra_column_names)
                    .withColumnRenamed(field_name, "clear")
                    .filter(col("clear").isNotNull() & (col("clear").cast("string") != ""))
                    .withColumn("correlation_id", expr("uuid()"))
                    .withColumn("pseudo_type", lit(deid_type))
                    .select('correlation_id', 'clear', 'pseudo_type', *extra_column_names)
            )

        return df_out.dropDuplicates(['clear', 'pseudo_type', *extra_column_names])


@log_action(log_args=['processing_jobs'])
def handle_outstanding_deid_jobs(processing_jobs: List[Row], deidentify: Deidentify):
    """
    This function is used to handle jobs (and associated pipelines) where we want to delay the
    marking a job as success until non Domain-0 domains have been tokenised
    i.e. before the final exit in the de-id process
    """
    latest_error = None
    submission_store = SubmissionsStore()

    for job in processing_jobs:
        if job.complete_after_retokenisation and job.resume_deid_submission:
            try:
                DeIdJobs.mark_success(job['job_id'])
                deidentify.resume_deid_submission(deid_job=job, submission_store=submission_store)

            except Exception as ex:
                app_logger.exception(lambda: dict(job_id=job['job_id'], exception=ex))
                if job.complete_after_retokenisation and job.resume_deid_submission:
                    DeIdJobs.mark_failed(job['job_id'])
                latest_error = ex

    if latest_error:
        raise latest_error
