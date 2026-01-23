import re
from typing import Set


class PSEUDO_TYPES:
    PERSON_ID = 'Person_ID'
    NHS_NUMBER = 'NHSNumber'
    LOCAL_PATIENT_ID = 'LocalPatientId'
    LOCAL_PATIENT_NUMBER = 'LocalPatientNumber'  # GP Data privitar type
    POSTCODE = 'Postcode'
    DATE_OF_DEATH = 'DoD'
    DATE_OF_BIRTH = 'DoB'

    ALL = [
        'Person_ID',
        'NHSNumber',
        'LocalPatientId',
        'Postcode',
        'DoD',
        'DoB'
    ]


class PATHS:
    META_DOT_DATA = 'meta.data'
    RAW_DATA = 'raw'
    OUT = 'out'
    DQ = 'dq'
    SUBMISSIONS = 'submissions'
    DEADLETTER = 'deadletter'
    PIPELINE_STATE = 'pipeline_state'
    PRE_DEADLINE_OUT = 'pre_deadline_out'
    DEID = 'deid'
    DATABRICKS_LOCAL_DISK0 = '/local_disk0'
    DAE_DATA_IN = 'dae_data_in'
    DM_PROJECTS_WORKSPACES = "/data_manager_projects"
    TEMP = "temp"
    DVE = "dve"


class RT:  # Root table
    CSDS_V1_6 = 'csds'
    IAPT_V2_0 = 'iapt'
    IAPT_V2_1 = 'iapt'
    MSDS = 'msds'
    MHSDS_V6 = 'mhsds'

class DS:
    AHAS = 'ahas'
    AHAS_DQ = 'ahas_dq'
    CQUIN = 'cquin'
    CSDS_GENERIC = 'csds'
    CSDS_V1_6 = 'csds_v1_6'
    CSDS_V1_TO_V1_5_AS_V1_6 = 'csds_v1_to_v1_5_as_v1_6'
    DIDS = 'dids'
    MHSDS_GENERIC = 'mhsds'
    MHSDS_V1_TO_V5_AS_V6 = 'mhsds_v1_to_v5_as_v6'
    MHSDS_V6 = 'mhsds_v6'
    MSDS = 'msds'
    DataManager = 'datamanager'
    MPSAAS = 'mpsaas'
    PCAREMEDS = 'pcaremeds'
    PLICS = 'plics'
    DEID = 'deid'
    DAE_DATA_IN = 'dae_data_in'
    DMS_UPLOAD = 'ws'
    API_GENERIC = 'api_generic'
    GP_DATA = 'gp_data'
    GP_DATA_PATIENT = 'gp_data_patient'
    GP_DATA_APPOINTMENT = 'gp_data_appointment'
    GP_DATA_PATIENT_REPORT = 'gp_data_patient_report'
    GP_DATA_APPOINTMENT_REPORT = 'gp_data_appointment_report'
    GP_DATA_PREM_STOP = 'gp_data_prem_stop'
    GP_DATA_PREM_INVALID = 'gp_data_prem_invalid'
    GP_DATA_PREM_EXTRACT_ACK = 'gp_data_prem_extract_ack'
    GP_DATA_CQRS_CRO_ACK = 'gp_data_cqrs_cro_ack'
    PDS = 'pds'
    SNAPSHOT = 'snapshot'
    IAPT_GENERIC = 'iapt'
    IAPT_V2_1 = 'iapt_v2_1'
    IAPT_V2_0 = 'iapt_v2_0'
    IAPT_V2_AS_V2_1 = 'iapt_v2_as_v2_1'
    DM_MPS = 'dm_mps'
    LPOS = 'lpos'
    CPOS = 'cpos'
    MESH_TO_POS = 'mesh_to_pos'
    POS_DISSEMINATION = 'pos_dissemination'
    POS_CANCEL_DISSEMINATION = 'pos_cancel_dissemination'
    RANDOX = 'randox'
    EPMAWSPC = 'epmawspc'
    EPMAWSAD = 'epmawsad'
    EPMAWSAD2 = 'epmawsad2'
    EPMAWSPC2 = 'epmawspc2'
    EPMANATIONALPRES = 'epmanationalpres'
    EPMANATIONALADM = 'epmanationaladm'
    NPEX = 'npex'
    NPEX_MPS = 'npex_mps'
    NPEX_PRISON_RESIDENTS = 'npex_prison_residents'
    GENERIC = 'generic'
    DUMMY_PIPELINE = 'dummy_pipeline'
    COVID_ANTIBODY_TESTING = 'covid_antibody_testing'
    P2C = 'p2c'  # permission to contact
    GDPPR = 'gdppr'
    SGSS = 'sgss'
    SGSS_DELTA = 'sgss_delta'
    NDRS_RDC = 'rapid_cancer_diag'
    PHE_CANCER = 'phe_cancer'
    OXI_HOME = 'oxi_home'
    NHS_LFD_TEST_RESULTS = 'NHSLFDTestResults'
    CVDP = 'cvdp'
    DARS_EXTRACT = 'dars_extract'
    DATA_PROVISIONING_EXTRACTS = 'data_provisioning_extracts'
    DIGITRIALS = 'digitrials'
    DIGITRIALS_ROUTING = 'digitrials_routing'
    DIGITRIALS_DCM = 'digitrials_dcm'
    DIGITRIALS_OUT = 'digitrials_out'
    DIGITRIALS_COMS = 'digitrials_coms'
    DIGITRIALS_COMS_REPORT = 'digitrials_coms_report'
    DIGITRIALS_REC = 'digitrials_rec'
    DIGITRIALS_REC_UPTAKE = 'digitrials_rec_uptake'
    DIGITRIALS_REC_EXTERNAL = 'digitrials_rec_external'
    DIGITRIALS_BMC = 'digitrials_bmc'
    DIGITRIALS_OPTOUTS = 'digitrials_optouts'
    ORPHAN_RESULTS = 'orphan_results'
    ORPHAN_RESULTS_LFD = 'orphan_results_lfd'
    ORPHAN_RESULTS_RMS = 'orphan_results_rms'
    COVID19_PCR_LFD_HOME_ORDERS = 'covid19_pcr_lfd_home_orders'
    REFLEX_ASSAY_RESULTS = 'reflex_assay_results'
    REFLEX_ASSAY_RESULTS_RELINK = 'reflex_assay_results_relink'
    GRAIL_KCL = 'grail_kcl'
    GRAIL_OPTOUT = 'grail_optout'
    MYS = 'mys'
    LFD_COLLECT = 'lfd_collect'
    ORPHAN_REGISTRATION = 'orphan_registration'
    ORPHAN_REGISTRATION_LFD = 'orphan_registration_lfd'
    ORPHAN_REGISTRATION_RMS = 'orphan_registration_rms'
    COVID19_VACCINE_EXEMPTION = 'covid19_vaccine_exemption'
    CSMS = 'csms'
    PCRM_LIST_RECON = 'pcrm_list_recon'
    PCRM = 'pcrm'
    RESULTS_MATCHING_SERVICE = 'results_matching_service'
    NDRS_GERMLINE = 'ndrs_germline'
    VIMS = 'vims'
    VIMS_INVITATION_APPROVALS = "vims_invitation_approvals"


class DB:
    # Some of these may not be used, but are here to stress that there is a difference between a dataset and a database.
    IAPT_V2_0 = 'iapt'
    IAPT_V2_1 = 'iapt_v2_1'
    IAPT_V1_5 = 'iapt_15'
    IAPT_V2_AS_V2_1 = 'iapt_v2_as_v2_1'
    MHSDS_V1_TO_V5_AS_V6 = 'mhsds_v1_to_v5_as_v6'
    CSDS_V1_6 = 'csds_v1_6'
    MSDS = 'msds'
    MHSDS_V6 = 'mhsds_v6'
    DSS_CORPORATE = 'dss_corporate'


class JOBS:
    DELTA_MERGE = 'delta_merge'
    INGESTION = 'ingestion'
    DQ_MERGE = 'dq_merge'
    PDS_MERGE = 'pds_merge'
    POS_MERGE = 'pos_merge'
    EXTRACTING = 'extracting'
    DE_ID = 'de_id'
    DM_PROJECT_CREATE = 'dm_project_create'
    DM_PROJECT_RESET = 'dm_project_reset'
    DM_PROJECT_REMOVE = 'dm_project_remove'
    DM_PROJECT_CREATE_MPS_TABLE = 'dm_project_create_mps_table'
    DM_PROJECT_MAKE_MPS_REQUEST = 'dm_project_make_mps_request'
    DM_PROJECT_CREATE_DEID_TABLE = 'dm_project_create_deid_table'
    BACKUP_DATABASE = 'backup_database'
    DM_PROJECT_WRITE_LOG = 'dm_project_write_log'
    GP_DATA_INGESTION = 'gp_data_ingestion'
    S3_INGESTION = 's3_ingestion'
    DARS_MESSAGE = 'dars_message_jobs'


class ROOTNODE:
    provider_pre_deadline = "_PPreE"
    provider_post_deadline = "_PPostE"


class FT:
    ACCESS = 'accdb'
    AVRO = 'avro'
    CSV = 'csv'
    JSON = 'json'
    JSONL = 'jsonl'
    XML = 'xml'
    XMLL = 'xmll'
    XLSX = 'xlsx'
    XLSM = 'xlsm'
    DATGZIP = 'dat.gz'
    CSVGZIP = 'csv.gz'
    PARQUET = 'parquet'
    ZIP = 'zip'
    TABLE = 'table'
    TXT = 'txt'


class WRITE_MODE:
    APPEND = "append"
    MERGE = "merge"
    LOG = "log"
    OVERWRITE = "overwrite"


class EXTENSION:
    """
    File extensions.
    """
    Json = '.json'
    Jsonl = '.jsonl'
    CompressedJson = '.json.gz'
    CompressedJsonl = '.jsonl.gz'
    Xml = '.xml'
    CompressedXml = '.xml.gz'
    Csv = '.csv'
    CompressedCsv = '.csv.gz'
    DAT = '.dat'
    CompressedDAT = '.dat.gz'
    ZIP = '.zip'
    XLSX = '.xlsx'
    XLSM = '.xlsm'
    Gzip = '.gz'
    PARQUET = '.parquet'
    Text = '.txt'


class COMPRESS:
    GZIP = 'gzip'
    BZIP = 'bzip2'


FT_BINARY = [FT.ACCESS, FT.XLSX]
FT_TEXT = [FT.CSV, FT.JSON, FT.JSONL, FT.XML]


class SUMMARY_EXTRACT_TYPES:
    DATA_QUALITY = 'data_quality'
    VALIDATIONS = 'validations'
    DIAGNOSTICS = 'diagnostics'
    AGGREGATION = 'aggregation'


class SUMMARY_REPORT_TYPES:
    DATA_QUALITY = 'Data Quality Issues'
    SUMMARY = 'Validation Summary'
    VALIDATIONS = 'Validation Failures'
    DIAGNOSTICS = 'Diagnostic'
    AGGREGATION = 'Aggregation'


class METADATA:
    DATASET_ID = 'dataset_id'
    REQUEST = 'request'
    EXTRA = 'extra'
    SENDER_ID = 'sender_id'
    FILENAME = 'filename'


class BLOCKTYPE:
    SUPER = 10
    DATASET = 20
    DATASET_TIME = 30
    GROUP = 40
    GROUP_DATASET = 50
    GROUP_DATASET_TIME = 60


class MESH_WORKFLOW_ID:
    MPS_REQUEST = 'SPINE_MPTPHASE4_TRACE'
    MPS_RESPONSE = 'SPINE_MPT_RESPONSE'
    PDS = 'SPINE_COHORT_EXTRACT'
    POS = 'SPINE_DISSENTER_UPDATE'
    MPSAAS = 'DSP_MPS'
    EXTRACT = 'DSP_EXTRACT'
    SUBMISSION = 'DSP_SUBMISSION'
    GP_DATA_PATIENT = 'GPDRP_PAT_EXTRACT'
    GP_DATA_PATIENT_ACK = 'GPDRP_PAT_EXTRACT_ACK'
    GP_DATA_PATIENT_REPORT = 'GPDRP_PAT_REPORT'
    GP_DATA_PATIENT_REPORT_ACK = 'GPDRP_PAT_REPORT_ACK'
    GP_DATA_APPOINTMENT = 'GPDRP_APPT_EXTRACT'
    GP_DATA_APPOINTMENT_ACK = 'GPDRP_APPT_EXTRACT_ACK'
    GP_DATA_APPOINTMENT_REPORT = 'GPDRP_APPT_REPORT'
    GP_DATA_APPOINTMENT_REPORT_ACK = 'GPDRP_APPT_REPORT_ACK'
    GP_DATA_PREM_STOP = 'GPDRP_PREM_STOP'
    GP_DATA_PREM_STOP_ACK = 'GPDRP_PREM_STOP_ACK'
    GP_DATA_PREM_INVALID = 'GPDRP_PREM_INVALID'
    GP_DATA_PREM_INVALID_ACK = 'GPDRP_PREM_INVALID_ACK'
    GP_DATA_PREM_EXTRACT = 'GPDRP_PREM_EXTRACT'
    GP_DATA_PREM_EXTRACT_ACK = 'GPDRP_PREM_EXTRACT_ACK'
    CQRS_NHSD_CRO = 'CQRS_NHSD_CRO'
    CQRS_NHSD_CRO_ACK = 'CQRS_NHSD_CRO_ACK'
    LPOS_RESPONSE = 'LPOS_RESPONSE'
    CPOS_RESPONSE = 'CPOS_RESPONSE'
    MESH_TO_POS = 'SPINE_NTT_UPHOLDING'
    MESH_TO_POS_DPS = 'POS_NTT_UPHOLDING'
    POS_DISSEMINATION_RESPONSE = 'POS_DISSEMINATION_RESPONSE'
    SUS_EXTRACTS = 'SUS_EXTRACTS'
    GPDC_RTPR = "GPDC_RTPR"
    GPDC_QR = "GPDC_QUERY_RESULTS"
    GPDC_QRA = "GPDC_QUERY_RESULTS_ACK"
    PHE_CANCER = "PHE_CANCER"
    PHE_COVID19_SGSS = "PHE_COVID19_SGSS"
    PHE_COVID19_FULL_NON_DE_DUPE = "PHE_COVID19_FULL_NON_DE_DUPE"
    KAINOS_COVID19_HOME_ORDERS = "KAINOS_COVID19_HOME_ORDERS"
    PHE_COVID19_REFLEX_ASSAY = "PHE_COVID19_REFLEX_ASSAY"
    GRAIL_KCL = 'GRAIL_PHASE_ONE_KCL'
    GRAIL_OPTOUT = 'GRAIL_PHASE_ONE_OPTOUT'
    RAPID_CANCER_DIAG = "NDRS_RDC"
    NDRS_RDC = "NDRS_RDC"
    PCAREMEDS = "EPACT2_PCARE_FLOW"
    COVID19_VACCINE_EXEMPTION = "COVID_EXEMPTIONS"
    DIGMED_FLU_VAC = "DIGMED_FLU_VAC"
    DIGMED_FLU_VAC_ACK = "DIGMED_FLU_VAC_ACK"
    PHE_COVID19_P1_HISTORY = "PHE_COVID19_P1_HISTORY"
    EPMA_NATIONAL_COLLECTION = "DPS_EPMA_SUBMISSION"
    UDAL_NHSD_CRO = 'UDAL_NHSD_CRO'
    UDAL_NHSD_CRO_ACK = 'UDAL_NHSD_CRO_ACK'
    VIMS = "VIMS_INVITATIONS_CONFIG"
    VIMS_INVITATION_APPROVALS = "VIMS_INVITATION_APPROVALS"
    DCM_PROCESSED_COHORT_EXTRACT = 'DCM_PROCESSED_COHORT_EXTRACT'
    NHS_NOTIFY_SEND_REQUEST = "NHS_NOTIFY_SEND_REQUEST"
    NHS_NOTIFY_SEND_REQUEST_ACK = "NHS_NOTIFY_SEND_REQUEST_ACK"
    NHS_NOTIFY_BATCH_REPORT = "NHS_NOTIFY_BATCH_REPORT"
    NHS_NOTIFY_DAILY_REPORT = "NHS_NOTIFY_DAILY_REPORT"
    DIGITRIALS_NHS_NOTIFY_SEND_REQUEST = "DIGITRIALS_NHS_NOTIFY_SEND_REQUEST"
    DIGITRIALS_NHS_NOTIFY_SEND_REQUEST_ACK = "DIGITRIALS_NHS_NOTIFY_SEND_REQUEST_ACK"
    DIGITRIALS_NHS_NOTIFY_BATCH_REPORT = "DIGITRIALS_NHS_NOTIFY_BATCH_REPORT"
    DIGITRIALS_NHS_NOTIFY_DAILY_REPORT = "DIGITRIALS_NHS_NOTIFY_DAILY_REPORT"
    DVE_PLICS = "DVE_PLICS"
    DVE_PLICS_ACK = "DVE_PLICS_ACK"

    @staticmethod
    def upload_workflow_ids() -> Set[str]:
        return {
            MESH_WORKFLOW_ID.MPS_REQUEST,
            MESH_WORKFLOW_ID.EXTRACT,
            MESH_WORKFLOW_ID.SUBMISSION,
            MESH_WORKFLOW_ID.MPSAAS,
            MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT_ACK,
            MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT_REPORT_ACK,
            MESH_WORKFLOW_ID.GP_DATA_PATIENT_ACK,
            MESH_WORKFLOW_ID.GP_DATA_PATIENT_REPORT_ACK,
            MESH_WORKFLOW_ID.LPOS_RESPONSE,
            MESH_WORKFLOW_ID.CPOS_RESPONSE,
            MESH_WORKFLOW_ID.POS_DISSEMINATION_RESPONSE,
            MESH_WORKFLOW_ID.MESH_TO_POS,
            MESH_WORKFLOW_ID.GPDC_QRA,
            MESH_WORKFLOW_ID.NDRS_RDC,
            MESH_WORKFLOW_ID.PHE_CANCER,
            MESH_WORKFLOW_ID.CQRS_NHSD_CRO,
            MESH_WORKFLOW_ID.DIGMED_FLU_VAC,
            MESH_WORKFLOW_ID.UDAL_NHSD_CRO,
            MESH_WORKFLOW_ID.DCM_PROCESSED_COHORT_EXTRACT,
            MESH_WORKFLOW_ID.NHS_NOTIFY_SEND_REQUEST,
            MESH_WORKFLOW_ID.DIGITRIALS_NHS_NOTIFY_SEND_REQUEST,
            MESH_WORKFLOW_ID.DVE_PLICS,
        }

    @staticmethod
    def download_workflow_ids() -> Set[str]:
        return {
            MESH_WORKFLOW_ID.MPS_RESPONSE,
            MESH_WORKFLOW_ID.PDS,
            MESH_WORKFLOW_ID.SUBMISSION,
            MESH_WORKFLOW_ID.POS,
            MESH_WORKFLOW_ID.MPSAAS,
            MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT,
            MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT_REPORT,
            MESH_WORKFLOW_ID.GP_DATA_PATIENT,
            MESH_WORKFLOW_ID.GP_DATA_PATIENT_REPORT,
            MESH_WORKFLOW_ID.SUS_EXTRACTS,
            MESH_WORKFLOW_ID.MESH_TO_POS,
            MESH_WORKFLOW_ID.NDRS_RDC,
            MESH_WORKFLOW_ID.PHE_CANCER,
            MESH_WORKFLOW_ID.GPDC_RTPR,
            MESH_WORKFLOW_ID.GPDC_QR,
            MESH_WORKFLOW_ID.PHE_COVID19_SGSS,
            MESH_WORKFLOW_ID.PHE_COVID19_FULL_NON_DE_DUPE,
            MESH_WORKFLOW_ID.KAINOS_COVID19_HOME_ORDERS,
            MESH_WORKFLOW_ID.PHE_COVID19_REFLEX_ASSAY,
            MESH_WORKFLOW_ID.GRAIL_KCL,
            MESH_WORKFLOW_ID.GRAIL_OPTOUT,
            MESH_WORKFLOW_ID.PCAREMEDS,
            MESH_WORKFLOW_ID.COVID19_VACCINE_EXEMPTION,
            MESH_WORKFLOW_ID.DIGMED_FLU_VAC_ACK,
            MESH_WORKFLOW_ID.CQRS_NHSD_CRO_ACK,
            MESH_WORKFLOW_ID.EPMA_NATIONAL_COLLECTION,
            MESH_WORKFLOW_ID.VIMS,
            MESH_WORKFLOW_ID.VIMS_INVITATION_APPROVALS,
            MESH_WORKFLOW_ID.DVE_PLICS_ACK,
            MESH_WORKFLOW_ID.NHS_NOTIFY_SEND_REQUEST_ACK,
            MESH_WORKFLOW_ID.NHS_NOTIFY_BATCH_REPORT,
            MESH_WORKFLOW_ID.NHS_NOTIFY_DAILY_REPORT,
            MESH_WORKFLOW_ID.DIGITRIALS_NHS_NOTIFY_SEND_REQUEST_ACK,
            MESH_WORKFLOW_ID.DIGITRIALS_NHS_NOTIFY_BATCH_REPORT,
            MESH_WORKFLOW_ID.DIGITRIALS_NHS_NOTIFY_DAILY_REPORT,
        }


class MESH_MAILBOXES:
    PDS_SENDER = 'INTERNAL-COHORT'
    MPS_SYSTEM = 'INTERNAL-SPINE'
    POS_SENDER = 'INTERNAL-SPINE'


class MPS_LOCAL_ID_PREFIXES:
    ADHOC = 'ADHOC'
    NPEX = 'NPEX'
    NPEX_MPS = 'NPEX-MPS'
    COVID_ANTIBODY_TESTING = 'COVID-ANTIBODY-TESTING'
    NHS_LFD_TESTS = 'NHS-LFD-TESTS'
    SGSS = 'SGSS'
    SGSS_DELTA = 'SGSS-DELTA'

    @staticmethod
    def local_id_prefixes() -> Set[str]:
        return {
            MPS_LOCAL_ID_PREFIXES.ADHOC,
            MPS_LOCAL_ID_PREFIXES.NPEX,
            MPS_LOCAL_ID_PREFIXES.NPEX_MPS,
            MPS_LOCAL_ID_PREFIXES.COVID_ANTIBODY_TESTING,
            MPS_LOCAL_ID_PREFIXES.NHS_LFD_TESTS,
            MPS_LOCAL_ID_PREFIXES.SGSS,
            MPS_LOCAL_ID_PREFIXES.SGSS_DELTA
        }


DATASET_ID_TO_MPS_LOCAL_ID_PREFIX = {
    DS.NPEX: MPS_LOCAL_ID_PREFIXES.NPEX,
    DS.NPEX_MPS: MPS_LOCAL_ID_PREFIXES.NPEX_MPS,
    DS.COVID_ANTIBODY_TESTING: MPS_LOCAL_ID_PREFIXES.COVID_ANTIBODY_TESTING,
    DS.NHS_LFD_TEST_RESULTS: MPS_LOCAL_ID_PREFIXES.NHS_LFD_TESTS,
    DS.SGSS: MPS_LOCAL_ID_PREFIXES.SGSS,
    DS.SGSS_DELTA: MPS_LOCAL_ID_PREFIXES.SGSS_DELTA
}


class _ReasonsForAccess(object):
    SenderIdentity = 'S'
    """
    The value of the organisation code of the sender.
    """

    ProviderCode = 'P'
    """
    The value of the organisation code of the provider.
    """

    ResponsibleCcgFromGeneralPractice = 'G'
    """The CCG that the patient's GP falls within at the time of treatment.

    The GP Practice is derived from PDS using the patient's NHS number
    as assigned by the provider.
    """

    ResidenceCcgFromPatientPostcode = 'H'
    """The CCG that the patient's postcode falls within at the time of
    treatment.

    This value is derived from data recorded with the Personal
    Demographics Service (PDS) using the patient's NHS number as
    assigned by the provider.
    """

    ResidenceCcg = 'R'
    """
    The original CCG of residence assigned by the provider.
    """

    CommissionerCode = 'C'
    """
    The commissioner code assigned by the provider.
    """

    @staticmethod
    def format(rfa_type: str, rfa_value) -> str:
        return '{}:{}'.format(rfa_type, rfa_value)


ReasonsForAccess = _ReasonsForAccess()
del _ReasonsForAccess


class ExtractConsumers:
    SDCS = 'sdcs'
    DSP = 'dsp'


class ExtractRequestKeys:
    SUBMISSION_ID = 'submission_id'
    SUBMISSION_WORKING_FOLDER = 'submission_working_folder'
    DQ_FILE_FORMAT = 'dq_file_format'


class SubmissionExtraFields:
    REPORTING_PERIOD_START = 'reporting_period_start'
    REPORTING_PERIOD_END = 'reporting_period_end'


class ClusterPools:
    SYSTEM = 'system_4'  # Default System Cluster
    SYSTEM_4 = 'system_4'
    SYSTEM_8 = 'system_8'
    SYSTEM_16 = 'system_16'
    SYSTEM_32 = 'system_32'

    SYSTEM_DBR_6_4 = 'system_DBR_6_4'
    SYSTEM_DBR_6_6 = 'system_DBR_6_6'
    SYSTEM_DBR_9_1 = 'system_DBR_9_1'
    SYSTEM_DBR_10_4 = 'system_DBR_10_4'
    SYSTEM_DBR_11_3 = 'system_DBR_11_3'

    SYSTEM_DBR_9_1_PHOTON = 'system_DBR_9_1_photon'
    SYSTEM_DBR_10_4_PHOTON = 'system_DBR_10_4_photon'
    SYSTEM_DBR_11_3_PHOTON = 'system_DBR_11_3_photon'

    TEMP_USERS = 'temp-users-acls'
    TEMP_USERS_DBR_6_4 = 'temp-users-acls_DBR_6_4'
    TEMP_USERS_DBR_6_6 = 'temp-users-acls_DBR_6_6'
    TEMP_USERS_DBR_9_1 = 'temp-users-acls_DBR_9_1'
    TEMP_USERS_DBR_10_4 = 'temp-users-acls_DBR_10_4'
    TEMP_USERS_DBR_11_3 = 'temp-users-acls_DBR_11_3'
    TEMP_USERS_DBR_9_1_PHOTON = 'temp-users-acls_DBR_9_1_photon'
    TEMP_USERS_DBR_10_4_PHOTON = 'temp-users-acls_DBR_10_4_photon'
    TEMP_USERS_DBR_11_3_PHOTON = 'temp-users-acls_DBR_11_3_photon'
    DAE_ACLS = 'dae-acls'
    DAE_TOOLS = 'dae-tools'
    DAE_TOOLS_73 = 'dae-tools-7.3'

    NPEX_8 = 'npex_8'

    SGSS_DELTA_4 = 'sgss_delta_4'
    MESH_TO_POS_4 = 'mesh_to_pos_4'

    EPMA_16 = 'epma_16'

    GP_DATA_4 = 'gp_data_4'

    DIGITRIALS_OUT = 'digitrials_out'


REGEX_SAFE = '[{}]*'.format(re.escape(''.join([
    '\t',
    '\r',
    '\n',
    *[chr(c) for c in range(32, 127)]
])))


class ServiceToggles:
    BROKER = 'broker'
    SCHEDULER = 'scheduler'
    STATUS_MONITOR = 'status_monitor'
    GP_DATA_RECEIVER = 'gp_data_receiver'
    GP_DATA_MESH_TRANSFER = 'gp_data_mesh_transfer'
    SDCS_CLASSIC_SUBMISSION_RECEIVER = 'sdcs_classic_submission_receiver'


class FeatureToggles:
    SUBMISSIONS_BIG_4 = 'submissions_big_4'
    HISTORICAL_SUBMISSION_WINDOWS = 'historical_submission_windows'
    INGESTION_MPS = 'ingestion_mps'
    SUBMISSION_WINDOW_VALIDATION = 'submission_window_validation'
    PRIVITAR_INTEGRATION = 'privitar_integration'
    MATERNITY_SNOMED_DERIVATIONS = 'maternity_snomed_derivations'
    IAPT_SNOMED_DERIVATIONS = 'iapt_snomed_derivations'
    CSDS_SNOMED_DERIVATIONS = 'csds_snomed_derivations'
    CSDS_WAITING_TIME_DERIVATIONS = 'csds_waiting_time_derivations'
    INCLUDE_LEAD_PROVIDER_IN_EXTRACTS = 'include_lead_provider_in_extracts'
    MHSDS_YTD_SUBMISSIONS = 'mhsds_ytd_submissions'
    NPEX_SEND_KEYSTONE_EXTRACTS = 'npex_send_keystone_extracts'
    COVID_ANTIBODY_TESTING_SEND_DELTA_EXTRACT_PPDS = 'covid_antibody_send_delta_extract_ppds'
    COVID_ANTIBODY_TESTING_SEND_DELTA_EXTRACT_ENGLAND = 'covid_antibody_send_delta_extract_england'
    COVID_ANTIBODY_TESTING_SEND_DELTA_EXTRACT_SCOTLAND = 'covid_antibody_send_delta_extract_scotland'
    COVID_ANTIBODY_TESTING_SEND_DELTA_EXTRACT_NORTHERN_IRELAND = 'covid_antibody_send_delta_extract_northern_ireland'
    COVID_ANTIBODY_TESTING_SEND_DELTA_EXTRACT_WALES = 'covid_antibody_send_delta_extract_wales'
    COVID_ANTIBODY_TESTING_SEND_DELTA_EXTRACT_EDGE = 'covid_antibody_send_delta_extract_edge'
    COVID_ANTIBODY_TESTING_SEND_DELTA_EXTRACT_NTP = 'covid_antibody_send_delta_extract_ntp'
    COVID_ANTIBODY_TESTING_SEND_KEYSTONE_EXTRACTS = 'covid_antibody_testing_send_keystone_extracts'
    NPEX_SEND_DELTA_EXTRACT_ENGLAND = 'npex_send_delta_extract_england'
    NPEX_SEND_DELTA_EXTRACT_SCOTLAND = 'npex_send_delta_extract_scotland'
    NPEX_SEND_DELTA_EXTRACT_WALES = 'npex_send_delta_extract_wales'
    NPEX_SEND_DELTA_EXTRACT_NORTHERN_IRELAND = 'npex_send_delta_extract_northern_ireland'
    NPEX_SEND_DELTA_EXTRACT_PPDS = 'npex_send_delta_extract_ppds'
    NPEX_SEND_DELTA_EXTRACT_DELOITTE = 'npex_send_delta_extract_deloitte'
    NPEX_SEND_DELTA_EXTRACT_EDGE = 'npex_send_delta_extract_edge'
    NPEX_SEND_DELTA_EXTRACT_EDGE_SCOTLAND = 'npex_send_delta_extract_edge_scotland'
    NPEX_SEND_DELTA_EXTRACT_EDGE_WALES = 'npex_send_delta_extract_edge_wales'
    NPEX_SEND_DELTA_EXTRACT_EDGE_NORTHERN_IRELAND = 'npex_send_delta_extract_edge_northern_ireland'
    NPEX_SEND_DELTA_EXTRACT_FRIMLEY = 'npex_send_delta_extract_frimley'
    NPEX_SEND_DELTA_EXTRACT_RTTS = 'npex_send_delta_extract_rtts'
    NPEX_MPS_SEND_DELTA_EXTRACT_EDGE = 'npex_mps_send_delta_extract_edge'
    NPEX_MPS_SEND_DELTA_EXTRACT_NTP = 'npex_mps_send_delta_extract_ntp'
    MPEX_MPS_SEND_DELTA_EXTRACT_RTTS = 'npex_mps_send_delta_extract_rtts'
    NPEX_KEYSTONE_EXCLUDE_NEGATIVE_LFT_RESULT = 'npex_keystone_exclude_negative_lft_result'
    NPEX_KEYSTONE_EXCLUDE_ASYMPTOMATIC_PCR_RESULT = 'npex_keystone_exclude_negative_asymptomatic_pcr_result'
    NPEX_KEYSTONE_EXCLUDE_NEGATIVE_RESULT = 'npex_keystone_exclude_negative_result'
    NPEX_KEYSTONE_SEND_LFT_RESULT = 'npex_keystone_send_lft_result'
    NPEX_SEND_DELTA_EXTRACT_SANGER = 'npex_send_delta_extract_sanger'
    NPEX_KEYSTONE_EXCLUDE_NEGATIVE_ASYMPTOMATIC_PCR_LAMP_EXCLUDE_SPECIAL_LOCATIONS = 'npex_keystone_exclude_negative_asymptomatic_pcr_lamp_exclude_special_locations'
    NPEX_EXCLUDE_NEGATIVE_ASYMPTOMATIC_PCR_LAMP_INCLUDE_SPECIAL_LOCATIONS = 'npex_exclude_negative_asymptomatic_pcr_lamp_include_special_locations'
    NPEX_EXCLUDE_VOID_ASYMPTOMATIC_PCR_LAMP_EXCLUDE_SPECIAL_LOCATION_RESIDENTS_PATIENTS = 'npex_exclude_void_asymptomatic_pcr_lamp_exclude_special_location_residents_patients'
    NPEX_EXCLUDE_VOID_ASYMPTOMATIC_PCR_LAMP_INCLUDING_SPECIAL_LOCATIONS = 'npex_exclude_void_asymptomatic_pcr_lamp_including_special_locations'
    NPEX_EXCLUDE_VOID_PCR_LAMP = 'npex_exclude_void_pcr_lamp'
    NPEX_EXCLUDE_POSITIVE_AT_THE_LIMIT_OF_DETECTION_PCR_LAMP = 'npex_exclude_positive_at_the_limit_of_detection_pcr_lamp'
    NPEX_EXCLUDE_NEGATIVE_SELFTEST_LFT = 'npex_exclude_negative_selftest_lft'
    NPEX_EXCLUDE_VOID_SELFTEST_LFT = 'npex_exclude_void_selftest_lft'
    NPEX_EXCLUDE_POSITIVE_SELFTEST_LFT = 'npex_exclude_positive_selftest_lft'
    NPEX_EXCLUDE_NEGATIVE_ASYMPTOMATIC_ADMINISTERED_LFT = 'npex_exclude_negative_asymptomatic_administered_lft'
    NPEX_EXCLUDE_VOID_ASYMPTOMATIC_ADMINISTERED_LFT = 'npex_exclude_void_asymptomatic_administered_lft'
    NPEX_EXCLUDE_STALE_RESULTS = 'npex_exclude_stale_results'
    NPEX_LIVERPOOL_INCLUDE_INSTITUTION = 'npex_liverpool_include_institution'
    NPEX_SEPARATE_LFT_TESTS_DELTA_EXTRACT_WALES = 'npex_separate_lft_tests_delta_extract_wales'
    NPEX_SEPARATE_LFT_TESTS_DELTA_EXTRACT_SCOTLAND = 'npex_separate_lft_tests_delta_extract_scotland'
    COVAX_EXTRACT_SEND_ADVERSE_REACTIONS = 'covax_extract_send_adverse_reactions'
    COVAX_EXTRACT_SEND_UPDATES = 'covax_extract_send_updates'
    COVAX_NEW_FHIR_OUTPUT = 'covax_new_fhir_output'
    COVAX_NEW_FHIR_GRADUAL_RAMP_UP = 'covax_new_fhir_gradual_ramp_up'
    COVAX_EXTRACT_SEND_DELETES = 'covax_extract_send_deletes'
    COVAX_FHIR_EXTRACT_PARALLELIZE = 'covax_fihr_extract_parallelize'
    COVAX_NEW_CSV_OUTPUT = 'covax_new_csv_output'
    SDCS_NHS_LFD_TESTS_SEND_EXTRACT_ENGLAND = 'sdcs_nhs_lfd_tests_send_extract_england'
    SDCS_NHS_LFD_TESTS_SEND_EXTRACT_EDGE = 'sdcs_nhs_lfd_tests_send_extract_edge'
    NPEX_SEND_DELTA_EXTRACT_NTP = 'npex_send_delta_extract_ntp'
    NPEX_SEND_POSITIVE_ASSISTED_LFT_ONLY = 'npex_send_positive_assisted_lft_only'
    NPEX_SEND_DELTA_EXTRACT_LIVERPOOL = 'npex_send_delta_extract_liverpool'
    NPEX_SEND_PRISON_RESIDENT_EXTRACT_TO_KEYSTONE = 'npex_send_prison_resident_extract_keystone'
    PRISON_RESIDENT_EXCLUDE_NON_POSITIVE_LFD_RESULTS = 'npex_prison_resident_exclude_non_positive_lfd_results'
    NPEX_INLINE_MPS_FOR_LFD_NEGATIVES = 'npex_inline_mps_for_lfd_negatives'
    SGSS_INGESTION_BY_PIPELINE = 'sgss_ingestion_by_pipeline'
    SGSS_DELTA_INGESTION_BY_PIPELINE = 'sgss_delta_ingestion_by_pipeline'
    SGSS_SEND_DELTA_EXTRACTS = 'sgss_send_delta_extracts'
    SGSS_DELTA_SEND_KEYSTONE_EXTRACTS = 'sgss_delta_send_keystone_extracts'
    SGSS_RUN_FULL_MPS = 'sgss_run_full_mps'
    SGSS_DELTA_RUN_FULL_MPS = 'sgss_delta_run_full_mps'
    SGSS_DELTA_RTTS_INCLUDE_PRIVATE_POCT = 'sgss_delta_rtts_include_private_poct'
    SGSS_SOFT_FAIL_ON_RECORD_LEVEL_MPS_FAILURE = 'sgss_soft_fail_on_record_level_mps_failure'
    SGSS_LOG_RECORDS_TO_SPLUNK = 'sgss_log_records_to_splunk'
    PILLAR2_LOG_RECORDS_TO_SPLUNK = 'pillar2_log_records_to_splunk'

    REFLEX_ASSAY_TESTING_SEND_DELTA_EXTRACT_SCOTLAND = 'reflex_assay_send_delta_extract_scotland'
    REFLEX_ASSAY_TESTING_SEND_DELTA_EXTRACT_NORTHERN_IRELAND = 'reflex_assay_send_delta_extract_northern_ireland'
    REFLEX_ASSAY_TESTING_SEND_DELTA_EXTRACT_WALES = 'reflex_assay_send_delta_extract_wales'

    COVID19_HOME_ORDERS_SEND_DA_EXTRACTS = 'covid19_home_orders_send_da_extracts'

    PDS_S3_ENABLED = 'pds_s3_enabled'
    PDS_S3_TEMP_LOADING = 'pds_s3_temp_loading'
    PDS_S3_PDS_UPDATE = 'pds_s3_pds_update'
    PDS_S3_PDS_PREVIEW = 'pds_s3_pds_preview'
    PDS_S3_CREATE_FULL = 'pds_s3_create_full'
    PDS_S3_FULL_PREVIEW = 'pds_s3_full_preview'

    DIGITRIALS_DCM_OUTPUT_ASSURANCE_FILE = 'digitrials_dcm_output_assurance_file'
    DIGITRIALS_NOTIFY_MESH_FEATURE_TOGGLE = 'send_requests_to_notify'
    DIGITRIALS_COALESCE_OPTIMISATION = 'digitrials_coalesce_optimisation'

    DIGITRIALS_PET_INTEGRATION = 'digitrials_pet_integration'
    DIGITRIALS_PRIVATAR_INTEGRATION = 'digitrials_privatar_integration'

    DIGITRIALS_DARS_EXTRACT_RELEASE = 'dars_extract_release'

    BROKER_BASED_ACL_CHECK = 'broker_based_acl_check'
    PET_DEID_ENABLED = 'pet_deid_enabled'


class Receivers(MESH_WORKFLOW_ID):
    MESH = 'mesh'
    SEFT = 'seft'
    SFTP = 'sftp'
    GP_DATA_RECEIVER = 'gp_data_receiver'


class Blockables(JOBS):
    RECEIVERS = "Receivers"
    SENDERS = "Senders"


class Senders:
    SFTP_ALL = 'sftp_all'
    SFTP_SEFT = 'sftp_seft'
    SFTP_HOST = 'sftp_host'
    MESH_ALL = 'mesh_all'
    MESH_WORKFLOW = 'mesh_workflow'
    MESH_MAILBOX_TO = 'mesh_mailbox_to'
    MESH_MAILBOX_FROM = 'mesh_mailbox_from'


class ByteMultipliers:
    GIGABYTES = 1024 * 1024 * 1024
    MEGABYTES = 1024 * 1024
    KILOBYTES = 1024


class UserGroupPrefixes:
    DATASET_USER_GROUP = 'db_owners_'


class FileTypeTypes:
    REFRESH = 'Refresh'
    PRIMARY = 'Primary'
    DELTA = 'Delta'
    MID_WINDOW = 'Mid-Window'
    POST_DEADLINE = 'Post-Deadline'


class ExtractFileVersions:
    Summary_v10 = 10  # summary extracts v1 formatted
    Default = 15  # default for current extracts
    Summary_v20 = 20  # summary extracts v2 formatted


class GPDataFolders:
    SUBMISSIONS_FOLDER = "submissions"
    ACKS_FOLDER = "acks"
    EXTRACTS_FOLDER = "extracts"


GP_DATA_RECEIVER_GRACE_PERIOD_END_HOUR = 1
GP_DATA_SUBMISSION_WORKFLOWS = [MESH_WORKFLOW_ID.GP_DATA_PATIENT, MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT]
GP_DATA_REPORT_WORKFLOWS = [MESH_WORKFLOW_ID.GP_DATA_PATIENT_REPORT, MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT_REPORT]
GP_DATA_ADHOC_DATASETS = [DS.GP_DATA_CQRS_CRO_ACK]
GP_DATA_SUBMISSION_DATASETS = [DS.GP_DATA_PATIENT, DS.GP_DATA_APPOINTMENT]

DSCRO_LIST = ['0CM', '0CT', '0CV', '0CP', '0CL', '0CN']
COMMISSIONING_LIST = ['0CM', '0CT', '0CP', '0CN', '0CL_ZIP']

PROJECT_DIFFERENTLY = [DS.PHE_CANCER, DS.CSMS]

DATASET_TO_DATABASE_MAP = {
    DS.IAPT_V2_0 : DB.IAPT_V2_0,
}

class DatasetVersions:
    IAPT_V2_0 = '2.0'
    IAPT_V2_1 = '2.1'
    CSDS_V1_6 = '1.6'
    MHSDS_V6 = '6.0'


DATASET_ID_VERSION_MAP = {
    (DS.IAPT_GENERIC, DatasetVersions.IAPT_V2_1): DS.IAPT_V2_1,
    (DS.CSDS_GENERIC, DatasetVersions.CSDS_V1_6): DS.CSDS_V1_6,
    (DS.MHSDS_GENERIC, DatasetVersions.MHSDS_V6): DS.MHSDS_V6,
}


class DigiTrialsPipelines:
    DIGITRIALS = DS.DIGITRIALS
    DIGITRIALS_DCM = DS.DIGITRIALS_DCM
    DIGITRIALS_OUT = DS.DIGITRIALS_OUT
    DIGITRIALS_COMS = DS.DIGITRIALS_COMS
    DIGITRIALS_COMS_REPORT = DS.DIGITRIALS_COMS_REPORT
    DIGITRIALS_REC = DS.DIGITRIALS_REC
    DIGITRIALS_REC_UPTAKE = DS.DIGITRIALS_REC_UPTAKE
    DIGITRIALS_REC_EXTERNAL = DS.DIGITRIALS_REC_EXTERNAL
    DIGITRIALS_BMC = DS.DIGITRIALS_BMC


TOS_FORMAT_TYPE_TWO = {
    'validations_listed_horizontally': [DS.MHSDS_V6],
    'validation_message_uses_uid_plus_idb_element_name': [DS.MHSDS_V6],
    'snomed_code_values_listed_on_separate_lines': [DS.MHSDS_V6],
}

DATASET_CAN_SUBMIT_CHECK = [
    DS.MHSDS_GENERIC, 
    DS.MSDS, 
    DS.IAPT_GENERIC,
    DS.CSDS_GENERIC
]
