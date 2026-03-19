from dsp.shared.models import ExtractType


class ExtractValidationTypes:
    SUBMISSION_ID_UNDEFINED = 'submission_id is undefined'
    SUBMISSION_WORKING_FOLDER_NOT_EXPECTED = 'submission_working_folder is not expected in this type of extract request'
    SUBMISSION_WORKING_FOLDER_EXPECTED = 'submission_working_folder is undefined'
    EXPECTED_FILE_NOT_EXPECTED = 'expected_file is a parameter used for testing and not allowed in production'
    AS_AT_DATE_EXPECTED = 'as_at_date is undefined'
    RP_START_DATE_EXPECTED = 'rp_start_date is undefined'
    FROM_EXPECTED = 'from is undefined'
    TO_EXPECTED = 'to is undefined'
    RFAS_EXPECTED = 'rfas is undefined - a non-empty rfas list is expected'
    RFAS_EXPECTED_NON_EMPTY = 'rfas should not be empty'
    RFAS_ONLY_ONE_PROVIDER_RFA = 'rfas should just contain one provider RFA in the form P:<ORG_CODE>'
    RFAS_ONLY_COMMISSIONER_RFAS = 'rfas should contain only comissioner provider RFAs in the form C:<ORG_CODE>'
    DQ_FILE_UNEXPECTED_FORMAT = 'the dq_file_format must be set to one of [json, parquet], or left unspecified (defaulting to parquet)'
    MESH_MAILBOX_ID_EXPECTED = 'mesh_mailbox_id is undefined'
    FILE_SPLITTING_NOT_SUPPORTED = 'file splitting not supported for extract'
    CHUNK_SIZE_REQUIRED = 'chunk size required'
    CORRELATION_ID_EXPECTED = 'correlation_id is required'
    EXTRACT_TABLES_EXPECTED = 'extract tables list must be defined'
    SOURCE_DATABASES_EXPECTED = 'source databases list must be defined'
    SOURCE_DATABASE_EXPECTED = 'source database must be defined'
    ORGS_IDS_EXPECTED = 'organisation ids must be a list of strings'
    YEAR_EXPECTED = 'year must be defined'
    MONTH_EXPECTED = 'month must be defined'


class DataManagerExtractValidationTypes:
    DIRECTORY_EXPECTED = 'directory is required'
    FILENAME_EXPECTED = 'filename is required'
    TABLES_EXPECTED = 'tables are required'
    UNIQUE_TABLES_EXPECTED = 'tables must be unique'
    PROJECT_EXPECTED = 'project is required'
    FORMAT_EXPECTED = 'format is required'
    INVALID_FORMAT = 'invalid format value'
    INVALID_TABLES_FOR_FORMAT = 'invalid number of tables for extract format'


class Covid19TestingExtractValidationTypes:
    COUNTRY_CATEGORIES_EXPECTED = 'country_categories_expected'
    INVALID_FROM_DATE = 'invalid_from_date'
    INVALID_TO_DATE = 'invalid_to_date'


class ExtractDetailsFields:
    SUBMISSION_ID = 'submission_id'
    SUBMISSION_WORKING_FOLDER = 'submission_working_folder'
    EXPECTED_FILES = 'expected_files'
    AS_AT_DATE = 'as_at_date'
    RP_START_DATE = 'rp_start_date'
    FROM = 'from'
    TO = 'to'
    RFAS = 'rfas'
    DQ_FILE_FORMAT = 'dq_file_format'
    SOURCE_DATABASE = 'source_database'
    SOURCE_DATABASES = 'source_databases'
    DEID_DATABASE = 'deid_database'
    MESH_MAILBOX_ID = 'mesh_mailbox_id'
    MESH_UPLOAD = 'mesh_upload'
    CHUNK_SIZE_BYTES = 'chunk_size_bytes'
    CORRELATION_ID = 'correlation_id'
    REQUESTED_DOMAINS = 'requested_domains'
    AHAS_DATASET = 'ahas_dataset'
    YEAR = 'year'
    MONTH = 'month'
    CCG = 'ccg'
    EXTRACT_TABLES = 'extract_tables'
    DELTA_NUMBER = 'delta_number'
    ORGS_IDS = 'orgs_ids'
    EXTRA = 'extra'
    RECIPIENT = 'recipient'


class DataManagerExtractDetailsFields:
    PROJECT = 'project'
    TABLES = 'tables'
    DIRECTORY = 'directory'
    FILENAME = 'filename'
    FORMAT = 'format'


class DeIdTargetDomainExtractDetailsFields:
    MESH_MAILBOX_ID = 'mesh_mailbox_id'
    MESH_UPLOAD = 'mesh_upload'
    REQUESTED_DOMAINS = 'requested_domains'


class DeIdDomainOneExtractDetailsFields:
    MESH_MAILBOX_ID = 'mesh_mailbox_id'
    MESH_UPLOAD = 'mesh_upload'
    SOURCE = 'source'
    SUBMISSION_ID = 'submission_id'


class Covid19TestingExtractDetailsFields:
    COUNTRY_CATEGORIES = 'country_categories'
    FROM_DATE = 'from_date'
    TO_DATE = 'to_date'


class SGSSExtractDetailsFields:
    SUBMISSION_FILE_NAME = 'submission_file_name'
    SUBMISSION_LOCAL_ID = 'submission_local_id'
    SUBMISSION_WORKFLOW_ID = 'submission_workflow_id'
    SUBMISSION_SENDER_MAILBOX = 'submission_sender_mailbox'
    TARGET_MAILBOXES = 'target_mailboxes'
    COUNTRY_CATEGORIES = 'country_categories'


class Covid19HomeOrdersExtractDetailsFields:
    COUNTRY_CATEGORIES = 'country_categories'


PORTAL_BACKEND_EXTRACT_TYPE_MAP = {
    ExtractType.IAPTProviderPostDeadlineGeneric: ExtractType.IAPT_V2_1_ProviderPostDeadline,
    ExtractType.IAPTProviderPreDeadlineGeneric: ExtractType.IAPT_V2_1_ProviderPreDeadline,
    ExtractType.IAPT_V2_1_Summary: ExtractType.IAPT_V2_1_Summary,
    ExtractType.CSDS_V1_6_Summary: ExtractType.CSDS_V1_6_Summary,
    ExtractType.CSDSProviderPreDeadlineGeneric: ExtractType.CSDS_V1_6_ProviderPreDeadline,
    ExtractType.CSDSProviderPostDeadlineGeneric: ExtractType.CSDS_V1_6_ProviderPostDeadline,
}