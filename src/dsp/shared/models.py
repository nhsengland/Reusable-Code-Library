import re
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import schematics
from dateutil.relativedelta import relativedelta
from schematics import types
from schematics.exceptions import DataError
from schematics.types import serializable

from dsp.shared import common, constants
from dsp.shared.aws import instance_profile_to_arn
from dsp.shared.common import DEFAULT_SPARK_VERSION, enumlike_values
from dsp.shared.constants import (
    BLOCKTYPE,
    COMPRESS,
    DS,
    EXTENSION,
    FT,
    GP_DATA_ADHOC_DATASETS,
    ClusterPools,
    ExtractFileVersions,
)
from dsp.shared.utils import SUBMISSION_DATASET_IDS, parse_filename
from dsp.shared.version_data import FULL_VERSION_STRING


class VersionedModel(schematics.Model):
    """
    A model with explicit version.
    """

    version = types.IntType()  # type: int

    CURRENT_VERSION = None  # type: Optional[int]

    uplifted = False

    def __init__(self, *args, **kwargs):
        args, kwargs = self.uplift(*args, **kwargs)
        super(VersionedModel, self).__init__(*args, **kwargs)
        self.version = self.CURRENT_VERSION

    def uplift(self, *args, **kwargs):
        return args, kwargs


class StrictModel(schematics.Model):
    """
    A model with implicit validation.
    """

    def __init__(self, raw_data=None, trusted_data=None, deserialize_mapping=None,
                 init=True, partial=False, strict=True, validate=True, app_data=None,
                 lazy=False, validation_appdata=None, **kwargs):
        super(StrictModel, self).__init__(
            raw_data=raw_data,
            trusted_data=trusted_data,
            deserialize_mapping=deserialize_mapping,
            init=init,
            partial=partial,
            strict=strict,
            validate=False,
            app_data=app_data,
            lazy=lazy,
            **kwargs
        )
        if validate:
            self.validate(partial=partial, app_data=validation_appdata or {})

    def __setattr__(self, key, value):
        super(StrictModel, self).__setattr__(key, value)
        if key in self:
            self.validate()


class LooseModel(schematics.Model):
    """
    A model with implicit validation.
    """

    def __init__(self, raw_data=None, trusted_data=None, deserialize_mapping=None,
                 init=True, partial=False, app_data=None, lazy=False, **kwargs):
        super(LooseModel, self).__init__(
            raw_data=raw_data,
            trusted_data=trusted_data,
            deserialize_mapping=deserialize_mapping,
            init=init,
            partial=partial,
            strict=False,
            validate=False,
            app_data=app_data,
            lazy=lazy,
            **kwargs
        )


class AgentStatus:
    Active = 'active'
    Disabled = 'disabled'


class Agent(StrictModel, VersionedModel):
    """
    An entity making a Submission.

    Attributes:
        id: numeric primary key
        common_name: must match the CN field in the SSL certificate subject
        name: human-friendly name
        serial: the certificate serial number, less the 0x octal prefix, e.g.
                for Serial Number: 1462904641 (0x57322741), this would be just 57322741
        version: model version
        status: if the agent is active or disabled
        submit_datasets: datasets the agent is allowed to have extracts from
        extract_datasets: datasets the agent is allowed to have extracts from
        dsa_ids: a list of agreement ids associated with the agent
    """
    CURRENT_VERSION = 1

    id = types.IntType(required=True)  # type: int
    common_name = types.StringType(required=True)  # type: str
    name = types.StringType(required=True)  # type: str
    serial = types.StringType(required=True)  # type: str
    status = types.StringType(
        choices=common.enumlike_values(AgentStatus),
        required=True,
        default=AgentStatus.Active
    )  # type: str
    version = types.IntType(required=True, default=0),  # type: int
    submit_datasets = types.ListType(types.StringType, required=True, default=[])  # type: List[str]
    extract_datasets = types.ListType(types.StringType, required=True, default=[])  # type: List[str]
    dsa_ids = types.ListType(types.StringType, required=False)  # type: List[str]


class PipelineStatus:
    Rejected = 'rejected'
    Pending = 'pending'
    Scheduling = 'scheduling'
    Running = 'running'
    Failed = 'failed'
    Success = 'success'
    Skipped = 'skipped'
    Unknown = 'unknown'
    Timedout = 'timedout'
    Cancelled = 'cancelled'
    Suspended = 'suspended'
    Processed = 'processed'
    Archived = 'archived'
    Received = 'received'
    Preparing = 'preparing'
    Investigating = 'investigating'

    @staticmethod
    def is_terminal(value: str):
        if not value:
            return False

        return value.lower() in [
            PipelineStatus.Rejected,
            PipelineStatus.Failed,
            PipelineStatus.Success,
            PipelineStatus.Skipped,
            PipelineStatus.Timedout,
            PipelineStatus.Cancelled,
            PipelineStatus.Archived,
        ]

    @staticmethod
    def is_success(value: str):
        if not value:
            return False

        return value.lower() == PipelineStatus.Success

    @staticmethod
    def all_statuses() -> List[str]:
        return [
            PipelineStatus.Rejected,
            PipelineStatus.Pending,
            PipelineStatus.Scheduling,
            PipelineStatus.Running,
            PipelineStatus.Suspended,
            PipelineStatus.Failed,
            PipelineStatus.Success,
            PipelineStatus.Skipped,
            PipelineStatus.Unknown,
            PipelineStatus.Timedout,
            PipelineStatus.Cancelled,
            PipelineStatus.Processed,
            PipelineStatus.Archived,
            PipelineStatus.Investigating,
        ]


class RunResultState:
    """
    Databricks run result states that can be returned for a job.
        If life_cycle_state = TERMINATED: if the run had a task, the result is guaranteed to be available, and
            it indicates the result of the task.
        If life_cycle_state = PENDING, RUNNING, or SKIPPED, the result state is not available.
        If life_cycle_state = TERMINATING or lifecyclestate = INTERNAL_ERROR: the result state is available if
            the run had a task and managed to start it.
    """
    Success = 'SUCCESS'
    Failed = 'FAILED'
    Timeout = 'TIMEDOUT'
    Cancelled = 'CANCELED'

    @staticmethod
    def is_success(value: str):
        return value == RunResultState.Success


class RunLifeCycleState:
    Pending = 'PENDING'
    Running = 'RUNNING'
    Terminating = 'TERMINATING'
    Terminated = 'TERMINATED'
    Skipped = 'SKIPPED'
    InternalError = 'INTERNAL_ERROR'

    @staticmethod
    def is_result_state_available(value):
        return value in [
            RunLifeCycleState.Terminated,
            RunLifeCycleState.Terminating,
            RunLifeCycleState.InternalError,
            RunLifeCycleState.Skipped,
        ]


class PipelineResult(StrictModel):
    """
    Basic information about pipeline's status.

    Attributes:
        status: either 'rejected' or 'success'
        total_records: total records parsed during loading phase
        dq_pass_count: total number of rows that passed all DQ checks
        dq_error_count: total number of rows that had any DQ errors
        dq_warning_count: total number of rows that had any DQ warnings
        validation_summary_extract_id: validation summary extract id

    Notes:
        dq_error_count = 1 does not imply that there was just one error -
        it only means, that there was only one line that had at least one DQ error
        in order to find all the errors, a scan on the default.dq Hive table is necessary
    """

    status = types.StringType(required=True, choices=[
        PipelineStatus.Suspended,
        PipelineStatus.Rejected,
        PipelineStatus.Success
    ])  # type: str
    total_records = types.IntType(required=True, min_value=0, default=0)  # type: int
    dq_pass_count = types.IntType(required=True, min_value=0, default=0)  # type: int
    dq_error_count = types.IntType(required=True, min_value=0, default=0)  # type: int
    dq_warning_count = types.IntType(required=True, min_value=0, default=0)  # type: int
    validation_summary_extract_id = types.IntType(required=False)  # type: int
    deid_extract_id = types.IntType(required=False)  # type: int
    extract_ids = types.DictType(types.IntType(), required=False)
    queued_submission_id = types.IntType(required=False)  # type: int

    def should_dq_merge(self) -> bool:
        return (self.dq_error_count + self.dq_warning_count) > 0

    def has_successful_results(self) -> bool:
        return self.successful() and self.dq_pass_count > 0

    def successful(self) -> bool:
        return self.status == PipelineStatus.Success


class Run(StrictModel):
    """
    An ingestion JobRun in Databricks.

    See Also: https://docs.databricks.com/api/latest/jobs.html
    See Also: https://db.dev.core.data.digital.nhs.uk/#joblist

    """
    run_id = types.IntType(required=True)  # type: int
    run_url = types.StringType(default=None)  # type: str
    ts = types.DateTimeType(required=True, tzd='reject', default=datetime.utcnow)  # type: datetime


class SubmittedMetadata(StrictModel):
    """
    Some of the metadata submitted along with the file.

    Attributes:
        dataset_id: one of constants.DS
        dataset_version: dataset version string
        submitted_timestamp: the time at which the event was first recorded in the HSCIC systems
        expected_file: path to expected result on S3 - only used in integration / e2e testing
        expected_failure: if True, the pipeline will error, regardless of the submission's validity
        extra: dataset-specific set of extra metadata
    """

    dataset_id = types.StringType(choices=common.enumlike_values(constants.DS), required=True)  # type: str
    dataset_version = types.StringType(required=False)  # type: str
    submitted_timestamp = types.IntType(required=True)  # type: int
    expected_file = types.StringType()  # type: str
    expected_failure = types.BooleanType(default=False)  # type: bool
    extra = types.DictType(types.UnionType([types.StringType, types.BooleanType]))  # type: Dict
    reporting_period_name = types.StringType(required=False)  # type: str
    reporting_period_start = types.DateType(required=False)  # type: datetime.date
    reporting_period_end = types.DateType(required=False)  # type: datetime.date
    submission_start_date = types.DateType(required=False)  # type: datetime.date
    submission_end_date = types.DateType(required=False)  # type: datetime.date
    submitted_by = types.StringType(required=False)  # type: str
    submitted_on = types.DateType(required=False)  # type: datetime.date
    is_test = types.BooleanType(required=False)  # type: bool
    stage = types.StringType(required=False)  # type: str


class SubmissionStage(StrictModel):
    """
    Stage of a Submission pipeline.

    Attributes:
        stage: name of the stage
        ts: integer timestamp in the format of YYYYMMDDHHIISS when Stage was completed
        passed: True if stage passed successfully
        details: optional list of strings, this should be non-empty if passed is False
    """

    stage = types.StringType(required=True)  # type: str
    ts = types.IntType(required=True)  # type: int
    passed = types.BooleanType(required=True)  # type: bool
    details = types.ListType(types.StringType)  # type: List[str]


class SubmissionMetadata(StrictModel):
    """
    Submission metadata.

    Attributes:
        submission_id: ID of the submission
        dataset_version: dataset version
        sender_id: the id of the sender
        test_submission: boolean indicating whether this submission is a test (validation only, not submitted) or not
        received_timestamp: integer timestamp in the format of YYYYMMDDHHmmSSffffff when associated
                                      Submission was created
        submitted_timestamp: interger timestamp  time at which the event was first recorded in the HSCIC systems
        file_type: one of constants.FT
        request: instance of SubmittedMetadata
        filename: name of the file as submitted
        filesize: actual size of the file in bytes, established in the Submission API
        file_hash: Hash of the ingested file
        dataset_id: one of constants.DS
        working_folder: working folder on S3, e.g. s3://nhsd-dspp-core-dev-raw/submissions/00000000000000001622
        agent_id: ID of an Agent
        s3_url: Location of submission file - applies to API V3

    """

    submission_id = types.IntType(required=True)  # type: int
    dataset_version = types.StringType(required=False)  # type: str
    sender_id = types.StringType(required=False)  # type: str
    test_submission = types.BooleanType(required=True)  # type: bool
    received_timestamp = types.IntType(required=True)  # type: int
    submitted_timestamp = types.IntType(required=True)  # type: int
    file_type = types.StringType(choices=common.enumlike_values(constants.FT), required=False)  # type: str
    request = types.ModelType(SubmittedMetadata, required=True)  # type: SubmittedMetadata
    filename = types.StringType(required=True)  # type: str
    filesize = types.IntType()  # type: int
    file_hash = types.StringType(default=None)  # type: str
    dataset_id = types.StringType(choices=common.enumlike_values(constants.DS), required=True)  # type: str
    working_folder = types.StringType(required=True)  # type: str
    agent_id = types.IntType(required=False)  # type: int
    s3_url = types.StringType(default=None)  # type: str


class Submission(StrictModel, VersionedModel):
    """
    A single Submission to the Submission API.

    Attributes:
        id: ID of the submission
        status: one of PipelineStatus
        received: datetime when the record was received by dsp core
        submitted: datetime when the record was originally submitted by the provider
        finished: datetime when the Submission read a terminal PipelineStatus - e.g. it failed or succeeded
        stages: list of SubmissionStage - this can grow over time as the Submission goes through the pipeline
        runs: list of Run related to this Submission; immediately after a submission this will be empty
        agent_id: ID of an Agent
        dataset_id: one of constants.DS
        dataset_version: optional version of the dataset (for multi version support)
        metadata: SubmissionMetadata instance
        pipeline_result: PipelineResult once the PipelineStatus is either 'success' or 'rejected' - empty otherwise
    """

    # any schema changes in Submission's embedded models:
    # PipelineResult, Run, SubmissionStage, SubmissionMetadata, SubmittedMetadata
    # should trigger a CURRENT_VERSION update on Submission
    CURRENT_VERSION = 10

    id = types.IntType(required=True)  # type: int
    status = types.StringType(choices=common.enumlike_values(
        PipelineStatus), required=True)  # type: str
    received = types.DateTimeType(required=True, tzd='reject',
                                  default=datetime.utcnow)  # type: datetime
    submitted = types.DateTimeType(required=True, tzd='reject')  # type: datetime
    finished = types.DateTimeType(tzd='reject')  # type: datetime
    stages = types.ListType(types.ModelType(SubmissionStage))  # type: List[SubmissionStage]
    runs = types.ListType(types.ModelType(Run), default=[])  # type: List[Run]
    sender_id = types.StringType(required=False)  # type: str
    agent_id = types.IntType(required=True)  # type: int
    dataset_id = types.StringType(choices=common.enumlike_values(constants.DS), required=True)  # type: str
    dataset_version = types.StringType(required=False)  # type: str
    workspace = types.StringType(required=False)
    metadata = types.ModelType(SubmissionMetadata, required=True)  # type: SubmissionMetadata
    pipeline_result = types.ModelType(PipelineResult)  # type: PipelineResult
    test_scope = types.StringType(required=False)  # type: str

    # todo: can we move this out of models
    _sender_resolvers = {
        DS.CQUIN: lambda sub: sub.metadata.filename[:3],
    }

    _block_group_resolvers = {
        DS.PHE_CANCER: lambda sub: f'{sub.get_sender_id()}_{sub.metadata.request.extra["table_name"].upper()}'
    }

    def get_sender_id(self) -> Optional[str]:
        resolver = self._sender_resolvers.get(self.dataset_id, lambda sub: sub.sender_id)
        return resolver(self)

    def get_block_group(self):
        resolver = self._block_group_resolvers.get(self.dataset_id, lambda sub: sub.get_sender_id())
        block_group = resolver(self)
        if not self.test_scope:
            return block_group

        return "{}:{}".format(self.test_scope, block_group)


class LastSubmission(StrictModel, VersionedModel):
    """
    Stores the last Submission datetime / submission_id per sender_id per dataset.

    Attributes:
        dataset_id: dataset id of submission
        sender_id: sender ORG code
        submitted_timestamp: datetime when the record was originally submitted by the provider
        submission_id: ID of the submission
        test_scope: test scope for isolation
    """

    CURRENT_VERSION = 0

    dataset_id = types.StringType(required=True)  # type: str
    sender_id = types.StringType(required=True)  # type: str
    submitted_timestamp = types.IntType(required=True)  # type: int
    submission_id = types.IntType(required=True)  # type: int
    test_scope = types.StringType(required=False)  # type: str

    @classmethod
    def from_submission(cls, submission: Submission):
        return cls(dict(
            dataset_id=submission.dataset_id,
            sender_id=submission.get_sender_id(),
            submitted_timestamp=submission.metadata.submitted_timestamp,
            submission_id=submission.id
        ))

    @classmethod
    def from_gp_submission(cls, submission):
        return cls(dict(
            dataset_id=DS.GP_DATA,
            sender_id='mesh' if submission.mailbox_from() else submission.sender_id,
            submitted_timestamp=int(submission.received.replace(tzinfo=timezone.utc).timestamp() * 1e6),
            submission_id=submission.id
        ))


class LastSequenceNumber(StrictModel, VersionedModel):
    """
    Stores the last sequence number / datetime / submission_id per channel_id per dataset.

    Attributes:
        dataset_id: dataset id of submission
        channel_id: channel id of submission
        submitted_timestamp: datetime when the record was originally submitted by the provider
        head_submission_id: ID of the submission
        head_sequence_number: sequence number for submission
        last_received_submission_id: ID of latest received submission
        last_received_sequence_number: last received sequence number for submission
        alerted: whether a current series of incorrect sequence numbers has been alerted or not
        test_scope: test scope for isolation
    """

    CURRENT_VERSION = 0

    dataset_id = types.StringType(required=True)  # type: str
    channel_id = types.StringType(required=True)  # type: str
    submitted_timestamp = types.IntType(required=True)  # type: int
    head_submission_id = types.IntType(required=True)  # type: int
    head_sequence_number = types.IntType(required=True)  # type: int
    last_received_submission_id = types.IntType(required=True)  # type: int
    last_received_sequence_number = types.IntType(required=True)  # type: int
    alerted = types.BooleanType(required=True)  # type: bool
    test_scope = types.StringType(required=False)  # type: str

    @classmethod
    def from_gp_submission(
            cls, submission, channel_id: str, sequence_number: int, last_received_sequence_number: int = None,
            alerted: bool = False):
        if last_received_sequence_number is None:
            last_received_sequence_number = sequence_number

        return cls(dict(
            dataset_id=submission.dataset_id,
            channel_id=channel_id,
            submitted_timestamp=int(submission.received.timestamp() * 1e6),
            head_submission_id=submission.id,
            head_sequence_number=sequence_number,
            last_received_submission_id=submission.id,
            last_received_sequence_number=last_received_sequence_number,
            alerted=alerted
        ))


class SubmissionHash(StrictModel):
    """
    A hash of a Submission contents.

    Attributes:
        hash: sha1 hash of the contents of the file submitted
        submission_id: ID of a Submission
        dataset_id: dataset id of submission
        created: datetime when the record was created
        test_scope: test scope for isolation
    """

    hash = types.StringType(required=True)  # type: str
    submission_id = types.IntType(required=True)  # type: int
    dataset_id = types.StringType(required=True)  # type: str
    test_scope = types.StringType(required=False)  # type: str
    created = types.DateTimeType(
        required=True,
        tzd='reject',
        default=datetime.utcnow
    )  # type: datetime


class MergeStatus:
    Received = 'received'
    Pending = 'pending'
    Merging = 'merging'
    Merged = 'merged'
    Failed = 'failed'
    Archived = 'archived'


class MergeType:
    DQFail = 'dq'
    DQPass = 'ok'


class DeltaMerge(StrictModel, VersionedModel):
    CURRENT_VERSION = 0

    key = types.StringType(required=True)  # type: str
    created = types.DateTimeType(
        required=True,
        tzd='reject',
        default=datetime.utcnow
    )  # type: datetime
    submitted = types.DateTimeType(required=True, tzd='reject')  # type: datetime
    dataset_id = types.StringType(choices=common.enumlike_values(
        constants.DS), required=True)  # type: str
    submission_id = types.IntType(required=True)  # type: int
    status = types.StringType(choices=common.enumlike_values(
        MergeStatus), required=True)  # type: str
    type = types.StringType(choices=common.enumlike_values(
        MergeType), required=True)  # type: List[MergeType]
    uri = types.StringType(required=True)  # type: str
    block_group = types.StringType(required=True)  # type: str
    test_scope = types.StringType(required=False)  # type: str


class MergeQueue(StrictModel, VersionedModel):
    CURRENT_VERSION = 0

    dataset_id = types.StringType(required=True)  # type: str
    block_group = types.StringType(required=True)  # type: str
    submission_ids = types.ListType(types.IntType)  # type: List[int]


class Block(StrictModel, VersionedModel):
    ALL = 'all'

    CURRENT_VERSION = 0

    job_type = types.StringType(choices=common.enumlike_values(
        constants.Blockables, True), required=True)  # type: str
    comp_range_key = types.StringType(required=True)  # type: str
    dataset_id = types.StringType(
        choices=(common.enumlike_values(constants.DS) + ['all']),
        required=True
    )  # type: str
    block_group = types.StringType(required=True)  # type: str
    block_type = types.IntType(choices=common.enumlike_values(
        constants.BLOCKTYPE), required=True)  # type: int
    from_date = types.DateTimeType(required=False, tzd='reject')  # type: datetime
    comment = types.StringType(required=False)  # type: str

    @staticmethod
    def create_composite_block_group(*parts: str) -> str:
        return ':'.join(parts)

    def is_blocked(
            self,
            block_group: str = None,
            dataset_id: str = None,
            submission_date: datetime = None
    ) -> bool:

        if self.block_type == BLOCKTYPE.SUPER:
            return True

        if self.dataset_id != self.ALL and self.dataset_id != dataset_id:
            return False

        if self.block_group != self.ALL and self.block_group != block_group:
            return False

        if not self.from_date:
            return True

        return submission_date >= self.from_date


class PublishStatus:
    """ The different states a publish job can move through """
    Pending = 'pending'
    Running = 'running'
    Failed = 'failed'
    Success = 'success'
    Cleaned = 'cleaned'
    Archived = 'archived'

    @staticmethod
    def is_terminal(value: str):
        if not value:
            return False

        return value.lower() in [
            PublishStatus.Failed,
            PublishStatus.Success,
        ]

    @staticmethod
    def is_success(value: str):
        return value.lower() == PublishStatus.Success or value.lower() == PublishStatus.Cleaned


class PublishSource:
    """
    The different datasets that can be sources for publish jobs.
    """
    RefData = 'ref_data'
    MSDS = 'msds'
    MAT_15 = 'mat_15'
    PCAREMEDS = 'pcaremeds'
    GPWP = 'gpwp'
    MHSDS_V6 = 'mhsds_v6'
    MHSDS_V1_TO_V5_AS_V6 = 'mhsds_v1_to_v5_as_v6'
    PREPUB_HES = 'prepub_hes'
    PREPUB_FLAT_HES_S = 'prepub_flat_hes_s'
    PREPUB_ONS = 'prepub_ons'
    PREPUB_ONS_S = 'prepub_ons_s'
    DAE_DATA_IN = 'dae_data_in'
    IAPT_V2_1 = 'iapt_v2_1'
    IAPT_15 = 'iapt_15'
    IAPT_V2_AS_V2_1 = 'iapt_v2_as_v2_1'
    CSDS_V1_6 = 'csds_v1_6'
    CSDS_V1_TO_V1_5_AS_V1_6 = 'csds_v1_to_v1_5_as_v1_6'
    AHAS = 'ahas'
    AHAS_DQ = 'ahas_dq'
    GDPPR = 'gdppr'
    PREPUB_HES_AHAS = "prepub_hes_ahas"
    GP_DATA = "gp_data"
    DIDS = "dids"


class PublishTarget:
    """
    The different environments a publish job can target. Dev, ref and prod are targets that are DSP core environments,
    other environments will require context from where they are being run. E.g. a job targeting "DAE" running in the
    "ref" environment will need to be configured to know where the DAE-Ref metastore is.
    """
    Core = 'core'
    Dae = 'dae'
    Covid = 'covid'

    @staticmethod
    def targets():
        return [PublishTarget.Core, PublishTarget.Dae, PublishTarget.Covid]


class Snapshot(StrictModel, VersionedModel):
    """
    Capture the information required to actually perform a publish operation.

    Attributes:
        unique_id: a GUID that allows a publish job to reference this snapshot uniquely
        source: where the data came from (dataset or db.table)
        created: the timestamp for when this snapshot was created
        location: the URI of the snapshot parquet on S3
        schema: a string encoding a JSON object representing the schema of the snapshot
    """
    CURRENT_VERSION = 1

    unique_id = types.StringType(required=True)  # type: str
    source = types.StringType(required=True)  # type: str
    target = types.StringType(required=True)  # type: str
    data_format = types.StringType(required=True)  # type: str
    created = types.DateTimeType(
        required=True,
        tzd='reject',
        default=datetime.utcnow
    )  # type: datetime
    location = types.StringType(required=True)  # type: str
    schema = types.StringType()  # type: str


class PublishQueue(StrictModel, VersionedModel):
    """
    Metadata wrapping a snapshot that is to be published.

    Attributes:
        created: the timestamp for when this publish job was created
        started: the timestamp for when this publish job was picked up and started being processed
        completed: the timestamp for when this publish job reached a terminal state
        unique_id: a GUID that can be used to track this job between different environments
        status: the current state of this job
        source: enumeration with the source dataset for this snapshot publish
        target: enumeration with the intended target environment for this snapshot publish
        snapshots: a list of GUIDs that reference the snapshots table
    """
    CURRENT_VERSION = 0

    created = types.DateTimeType(
        required=True,
        tzd='reject',
        default=datetime.utcnow
    )  # type: datetime
    started = types.DateTimeType(tzd='reject')  # type: datetime
    completed = types.DateTimeType(tzd='reject')  # type: datetime
    cleaned = types.DateTimeType(tzd='reject')  # type: datetime
    unique_id = types.StringType(required=True)  # type: str
    status = types.StringType(choices=common.enumlike_values(
        PublishStatus), required=True)  # type: str
    source = types.StringType(choices=common.enumlike_values(
        PublishSource), required=True)  # type: str
    target = types.StringType(choices=common.enumlike_values(
        PublishTarget), required=True)  # type: str
    snapshots = types.ListType(types.StringType, required=True)  # type: List[str]


class MeshTransferStatus:
    """ The different states a mesh download job can move through """
    Pending = 'pending'  # MESH message has been identified
    Preparing = 'preparing'  # MESH message content is being prepared to S3
    Ready = 'ready'  # MESH message is ready to be processed
    Processing = 'processing'  # MESH message is being processed
    Failed = 'failed'  # MESH message has failed to be processed, and will block the queue
    Unprocessable = 'unprocessable'  # MESH message has failed to be processed, but will not block the queue
    Success = 'success'  # MESH message was successfully processed
    Archived = 'archived'  # we've archived
    Paused = 'paused'  # MESH message is paused for future processing

    @staticmethod
    def is_terminal(value: str) -> bool:
        if not value:
            return False

        return value.lower() in [
            MeshTransferStatus.Failed,
            MeshTransferStatus.Unprocessable,
            MeshTransferStatus.Success,
            MeshTransferStatus.Archived,
        ]

    @staticmethod
    def is_success(value: str) -> bool:
        return value.lower() == MeshTransferStatus.Success

    @staticmethod
    def all_statuses() -> List[str]:
        return [
            MeshTransferStatus.Pending,
            MeshTransferStatus.Preparing,
            MeshTransferStatus.Ready,
            MeshTransferStatus.Processing,
            MeshTransferStatus.Success,
            MeshTransferStatus.Failed,
            MeshTransferStatus.Unprocessable,
            MeshTransferStatus.Archived,
            MeshTransferStatus.Paused
        ]

    @staticmethod
    def except_archived() -> List[str]:
        return [stat for stat in MeshTransferStatus.all_statuses() if stat != MeshTransferStatus.Archived]


class MeshMessageType:
    """
    Identifies the type of transfer

    * Data will have a data file and a control file.
    * Report will have a control file.
    """
    DATA = 'Data'
    REPORT = 'Report'


class MeshMetadata(StrictModel):
    filename = types.StringType(required=False)  # type: str
    local_id = types.StringType(required=False)  # type: str
    message_type = types.StringType(required=False)  # type: str
    subject = types.StringType(required=False)  # type: str
    encrypted = types.BooleanType(required=False)  # type: bool
    compressed = types.BooleanType(required=False)  # type: bool
    test_ws_db_name = types.StringType(required=False)  # type: str
    checksum = types.StringType(required=False)  # type: str
    
    def to_dict(self):
        return dict(
            filename = self.filename,
            local_id = self.local_id,
            message_type = self.message_type,
            subject = self.subject,
            encrypted = self.encrypted,
            compressed = self.compressed,
            test_ws_db_name = self.test_ws_db_name,
            checksum = self.checksum,
        )


class MeshTransferJobPriority(StrictModel):
    Lowest = 0
    Lower = 3
    Low = 5
    Normal = 10
    High = 20


class MeshTransferJob(StrictModel, VersionedModel):
    CURRENT_VERSION = 2

    transfer_id = types.StringType(required=True)  # type: str
    created = types.DateTimeType(
        required=True,
        tzd='reject',
        default=datetime.utcnow
    )  # type: datetime
    started = types.DateTimeType(tzd='reject')  # type: datetime
    completed = types.DateTimeType(tzd='reject')  # type: datetime
    message_id = types.StringType(required=False)  # type: str
    status = types.StringType(
        choices=common.enumlike_values(MeshTransferStatus),
        required=True
    )  # type: str
    workflow_id = types.StringType(required=True)  # type: str
    s3_locations = types.ListType(types.StringType, required=True)  # type: List[str]
    mesh_metadata = types.ModelType(MeshMetadata, required=True)  # type: MeshMetadata
    mailbox_from = types.StringType(required=False)  # type: str
    mailbox_to = types.StringType(required=True)  # type: str
    priority = types.IntType(default=MeshTransferJobPriority.Normal)  # type: int
    content_length = types.IntType(default=0)  # type: int

    @serializable
    def priority_created(self):
        """
        This is used as a sort key, highest priority, lowest (oldest) created
        """
        return f'{10_000 - self.priority:04}_{self.created.isoformat()}'

    def single_s3_location(self):
        location_count = len(self.s3_locations)
        if location_count != 1:
            raise ValueError('Expected a single s3 location but got {}'.format(location_count))

        return next(iter(self.s3_locations), None)

    def validate(self, partial=False, convert=True, app_data=None, **kwargs):
        super().validate(partial=partial, convert=convert, app_data=app_data, **kwargs)

        if not 1 <= self.priority <= 10_000:
            raise DataError(
                {'priority': f'priority of {self.priority} is out of range'}
            )

    def to_dict(self):
        return dict(
            transfer_id=self.transfer_id,
            created=self.created,
            started=self.started,
            completed=self.completed,
            message_id=self.message_id,
            status=self.status,
            workflow_id=self.workflow_id,
            s3_locations=self.s3_locations,    
            mesh_metadata=self.mesh_metadata.to_dict(),
            mailbox_from=self.mailbox_from,
            mailbox_to=self.mailbox_to,
            priority=self.priority,
            content_length=self.content_length,
        )


class SftpTransferStatus:
    """ The different states a SFTP transfer job can move through """
    Ready = 'ready'
    Processing = 'processing'
    Failed = 'failed'
    Success = 'success'
    Archived = 'archived'

    @staticmethod
    def is_terminal(value: str) -> bool:
        if not value:
            return False

        return value.lower() in [
            SftpTransferStatus.Failed,
            SftpTransferStatus.Success,
            SftpTransferStatus.Archived,
        ]

    @staticmethod
    def is_success(value: str) -> bool:
        return value.lower() == SftpTransferStatus.Success

    @staticmethod
    def all_statuses() -> List[str]:
        return [
            SftpTransferStatus.Ready,
            SftpTransferStatus.Processing,
            SftpTransferStatus.Success,
            SftpTransferStatus.Failed,
            SftpTransferStatus.Archived
        ]


class SftpTransferJob(StrictModel, VersionedModel):
    CURRENT_VERSION = 3

    transfer_id = types.StringType(required=True)  # type: str
    created = types.DateTimeType(
        required=True,
        tzd='reject',
        default=datetime.utcnow
    )  # type: datetime
    started = types.DateTimeType(tzd='reject')  # type: datetime
    completed = types.DateTimeType(tzd='reject')  # type: datetime
    status = types.StringType(
        choices=common.enumlike_values(SftpTransferStatus),
        required=True
    )  # type: str
    s3_location = types.StringType(required=True)  # type: str
    directory = types.StringType(required=False)  # type: str
    filename = types.StringType(required=False)  # type: str
    correlation_id = types.StringType(required=True)  # type: str
    hostname = types.StringType(required=False)  # type: str
    username = types.StringType(required=False)  # type: str
    is_seft = types.BooleanType(required=True)  # type: bool
    content_length = types.IntType(required=True)  # type: int

    def validate(self, partial=False, convert=True, app_data=None, **kwargs):
        super().validate(partial=partial, convert=convert, app_data=app_data, **kwargs)

        if self.is_seft and not self.directory:
            raise DataError({'directory': 'field is expected for SEFT transfer jobs'})

        if not self.is_seft and not self.hostname:
            raise DataError({'hostname': 'field is expected for non-SEFT transfer jobs'})


class MpsManifestStatus:
    """The different states an MPS Manifest can go through"""
    Pending = 'pending'
    Success = 'success'


class MpsManifest(StrictModel, VersionedModel):
    """
    Track status of MPS requests and responses

    Attributes:
        submission_id: an idenfitier from the submission that allows us to reconcile all chunks for submission
        all_chunks: all chunks including those matched with PDS in the local cross check trace
        remaining_chunks: request chunks to be processed by MPS
        status: the current state of the MPS manifest
        metadata: additional metadata about the manifest
    """
    CURRENT_VERSION = 0

    submission_id = types.StringType(required=True)  # type: str
    all_chunks = types.ListType(types.StringType, required=True)  # type: List[str]
    remaining_chunks = types.ListType(types.StringType, required=True)  # type: List[str]
    status = types.StringType(
        choices=common.enumlike_values(MpsManifestStatus),
        required=True
    )  # type: str
    metadata = types.StringType(required=False)  # type: str


class UnsuspendJobStatus:
    """The different states an unsuspend job can go through"""
    Success = 'success'
    Processing = 'processing'
    Failed = 'failed'

    @staticmethod
    def all_statuses() -> List[str]:
        return [
            UnsuspendJobStatus.Failed,
            UnsuspendJobStatus.Success,
            UnsuspendJobStatus.Processing,
        ]


class UnsuspendJob(StrictModel, VersionedModel):
    """
    Track status of unsuspend job of given pipeline

    Attributes:
        source_pipeline_id: the id of the pipeline that wants to be unsuspended
        source_pipeline_table: the table name of the pipeline that needs to be unsuspended
        target_watch_list: json blob containing DynamoDB table names and the subsequent ids to monitor the status of
        status: related to whether the submission has been unsuspended
    """
    CURRENT_VERSION = 0

    source_pipeline_id = types.IntType(required=True)  # type: int
    source_pipeline_table = types.StringType(required=True)  # type: str
    target_watch_list = types.DictType(
        types.UnionType([
            types.ListType(types.IntType),
            types.DictType(types.ListType(types.StringType))
            ]),
        required=True
    )  # type: Dict[int]
    status = types.StringType(
        choices=common.enumlike_values(UnsuspendJobStatus),
        required=True
    )  # type: str


class ExtractPriority:
    """The different priorities an extract can be placed at"""
    Low = 5
    Normal = 10
    High = 20

class ExtractTypeDicommissioned:
    IAPT_V2_1_Summary = 'iapt_v2_1_summary'
    IAPT_V2_1_ProviderPreDeadline = 'iapt_v2_1_provider_pre_deadline'
    IAPT_V2_1_ProviderPostDeadline = 'iapt_v2_1_provider_post_deadline'

    MHSDSV6ProviderPreDeadline = 'mhsds_v6_provider_pre_deadline'
    MHSDSV6ProviderPostDeadline = 'mhsds_v6_provider_post_deadline'
    MHSDSV6Summary = 'mhsds_v6_summary'

    CSDS_V1_6_Summary = 'csds_v1_6_summary'
    CSDS_V1_6_ProviderPreDeadline = 'csds_v1_6_provider_pre_deadline'
    CSDS_V1_6_ProviderPostDeadline = 'csds_v1_6_provider_post_deadline'

class ExtractType:
    """The different types of extract that can be run"""
    AHASDataPump = 'ahas_data_pump'
    AHASDataQualityReports = 'ahas_data_quality_reports'
    AHASDataQualityRaw = 'ahas_data_quality_raw'
    AHASDataQualityProd = 'ahas_data_quality_prod'

    MHSDSProviderPreDeadlineGeneric = 'mhsds_provider_pre_deadline'
    MHSDSProviderPostDeadlineGeneric = 'mhsds_provider_post_deadline'
    MHSDSSummaryGeneric = 'mhsds_summary'

    MHSDSV6ProviderPreDeadline = 'mhsds_v6_provider_pre_deadline'
    MHSDSV6ProviderPostDeadline = 'mhsds_v6_provider_post_deadline'
    MHSDSV6Comm = 'mhsds_v6_commissioning'
    MHSDSV6Summary = 'mhsds_v6_summary'
    MHSDSV6CommPersonIDLinkage = 'mhsds_v6_comm_person_id_linkage'

    CSDSProviderPreDeadlineGeneric = 'csds_provider_pre_deadline'
    CSDSProviderPostDeadlineGeneric = 'csds_provider_post_deadline'
    CSDSSummaryGeneric = 'csds_summary'

    CSDS_V1_6_Comm = 'csds_v1_6_commissioning'
    CSDS_V1_6_CommPersonIDLinkage = 'csds_v1_6_comm_person_id_linkage'
    CSDS_V1_6_Summary = 'csds_v1_6_summary'
    CSDS_V1_6_ProviderPreDeadline = 'csds_v1_6_provider_pre_deadline'
    CSDS_V1_6_ProviderPostDeadline = 'csds_v1_6_provider_post_deadline'
    CSDS_V1_6_HistComm = 'csds_v1_6_historic_commissioning'

    CovidAntibodyTestingDeltaExtract = 'covid_antibody_testing_delta_extract'
    CovidAntibodyTestingKeystoneExtract = 'covid_antibody_testing_keystone_extract'

    DataManager = 'data_manager'
    DEIDDomainOne = 'deid_domain_one'
    DEIDTargetDomain = 'deid_target_domain'

    GDPPRDscro = 'gdppr_dscro'
    GPDataCQRS = 'gp_data_cqrs'
    GPDataUDAL = 'gp_data_udal'

    IAPTProviderPreDeadlineGeneric = 'iapt_provider_pre_deadline'
    IAPTProviderPostDeadlineGeneric = 'iapt_provider_post_deadline'
    IAPTSummaryGeneric = 'iapt_summary'

    IAPT_V2_1_Summary = 'iapt_v2_1_summary'
    IAPT_V2_1_ProviderPreDeadline = 'iapt_v2_1_provider_pre_deadline'
    IAPT_V2_1_ProviderPostDeadline = 'iapt_v2_1_provider_post_deadline'
    IAPT_V2_1_Comm = 'iapt_v2_1_commissioning'
    IAPT_V2_1_CommPersonIDLinkage = 'iapt_v2_1_comm_person_id_linkage'

    MSDSComm = 'msds_commissioning'
    MSDSCommPersonIDLinkage = 'msds_comm_person_id_linkage'
    MSDSProviderPreDeadline = 'msds_provider_pre_deadline'
    MSDSProviderPostDeadline = 'msds_provider_post_deadline'
    MSDSProviderMidWindow = 'msds_provider_mid_window'
    MSDSSummary = 'msds_summary'

    NPExDeltaExtract = 'npex_delta_extract'
    NPExKeystoneExtract = 'npex_keystone_extract'

    NPExMpsDeltaExtract = 'npex_mps_delta_extract'

    P2CTableauExtract = 'p2c_tableau_extract'

    ReflexAssayResultsDeltaExtract = "reflex_assay_results_delta_extract"

    SGSSDeltaExtract = 'sgss_delta_extract'
    SGSSDeltaKeystoneExtract = 'sgss_delta_keystone_extract'

    ValidationSummary = 'validation_summary'

    NHSLFDTestsDqExtract = 'nhs_lfd_tests_dq_extract'

    Covid19HomeOrdersDAExtract = 'covid19_home_orders_da_extract'

    DidsNhseExtract = 'dids_nhse'
    DidsNcrasDailyExtract = 'dids_ncras_cohort_daily'
    DidsNcrasMonthlyExtract = 'dids_ncras_cohort_monthly'

    CodePromotionMeshExtract = 'cp_mesh_send'
    CodePromotionS3Extract = 'cp_s3_send'
    CodePromotionSeftExtract = 'cp_seft_send'

class ExtractRequestMessage(StrictModel, VersionedModel):
    """
    Minimal processing information about a received extract request.
    Attributes:
        extract_id: A unique identifier for this extract (can be used to look up the extract request in DynamoDB)
        priority: job priority
        type: the type of extract, if it will require additional permissions/capabilities
        test_scope (str): the test scope locator
    """
    CURRENT_VERSION = 0

    extract_id = types.IntType(required=True)  # type: int
    priority = types.IntType(
        choices=common.enumlike_values(ExtractPriority),
        default=ExtractPriority.Normal
    )  # type: int
    type = types.StringType(
        choices=common.enumlike_values(ExtractType),
        required=True
    )  # type: str
    test_scope = types.StringType(required=False)  # type: str


class ExtractFileOrPrefix:
    """ File prefixs """
    File = 'file'
    Prefix = 'prefix'


class ExtractFile(StrictModel):
    """
        extract response holder class for generated extracts

        Attributes:
            extract_name (str): name of the extract 'key'  e.g. 'header', 'summary' etc
            path (str): extract full output path
            file_format (str): ExtractFileFormat ... e.g. jsonl, json, xml, csv xmll (xml lines)
            file_or_prefix (str): file or folder ... ( is it a cluster generated output)
            file_suffix (str): suffix search filter ( eg .. .json / .json.gz )
            compression (str): e.g. gzip or None
            version (int): an arbitrary version number to allow DPS portal to filter or branch their processing

    """
    extract_name = types.StringType(required=True)  # type: str
    path = types.StringType(required=True)  # type: str
    file_format = types.StringType(
        choices=list(common.enumlike_values(FT)),
        required=True
    )  # type: str
    file_or_prefix = types.StringType(
        choices=list(common.enumlike_values(ExtractFileOrPrefix)),
        required=True,
    )  # type: str
    file_suffix = types.StringType(
        choices=list(common.enumlike_values(EXTENSION)),
        required=False,
        default=None,
    )  # type: str
    compression = types.StringType(
        choices=list(common.enumlike_values(COMPRESS)),
        required=False,
        default=None,
    )  # type: str
    version = types.IntType(default=ExtractFileVersions.Default, required=False)


class ExtractResult(StrictModel):
    """
    extract response holder class for generated extracts

    Attributes:
        status: (str): status
        started_at (datetime): timestamp started
        finished_at (datetime): timestamp finished
        files (List[ExtractFile]): generated files
    """
    status = types.StringType(required=True, choices=[
        PipelineStatus.Suspended,
        PipelineStatus.Rejected,
        PipelineStatus.Success
    ])  # type: str
    started_at = types.DateTimeType(required=True, tzd='reject')  # type: datetime
    finished_at = types.DateTimeType(required=True, tzd='reject')  # type: datetime
    files = types.ListType(types.ModelType(ExtractFile), required=True)  # type: List[ExtractFile]


class ExtractRequest(StrictModel, VersionedModel):
    """
    Capture an extract request as well as metadata about the processing job (state, results, etc.)

    Attributes:
        extract_id: unique identifier for this extract request
        sender: the sender of the request
        consumer: who the extract is for ( e.g. sdcs / mesh etc .. )
        type: extract type e.g. validation_summary
        request: the request as received
        received: when the request was received
        started: when the request was accepted by a processor
        completed: when the request was completed by a processor
        status: current state of the processing job
        results: list of results file URIs
        test_scope: string test scope
    """
    CURRENT_VERSION = 0

    extract_id = types.IntType(required=True)  # type: int
    priority = types.IntType(default=ExtractPriority.Normal)  # type: int
    sender = types.StringType(required=True)  # type: str
    consumer = types.StringType(required=True)  # type: str
    type = types.StringType(
        choices=list(common.enumlike_values(ExtractType)),
        required=True
    )  # type: str
    request = types.DictType(
        types.UnionType([
            types.BooleanType,
            types.IntType,
            types.StringType,
            types.FloatType,
            types.DateTimeType,
            types.ListType(types.StringType),
            types.ListType(types.UnionType([
                types.StringType,
                types.ListType(types.StringType)
            ])),
            types.DictType(types.UnionType([
                types.StringType,
                types.DictType(types.StringType)
            ]))
        ]), required=True
    )  # type: dict
    received = types.DateTimeType(required=True, tzd='reject')  # type: datetime
    started = types.DateTimeType(tzd='reject')  # type: datetime
    finished = types.DateTimeType(tzd='reject')  # type: datetime
    completed = types.DateTimeType(tzd='reject')  # type: datetime
    status = types.StringType(
        choices=common.enumlike_values(PipelineStatus),
        required=True
    )  # type: str
    runs = types.ListType(types.ModelType(Run), default=[])  # type: List[Run]
    db_run_id = types.IntType(required=False)  # type: int
    result = types.ModelType(ExtractResult, required=False, default=None)  # type: ExtractResult
    failure_reason = types.StringType()  # type: str
    test_scope = types.StringType(required=False)  # type: str

    def get_block_group(self):
        extract_type = self.type
        if not self.test_scope:
            return extract_type

        return "{}:{}".format(self.test_scope, extract_type)


class DatabricksUser(StrictModel, VersionedModel):
    """
    Capture a databricks user details
    """
    CURRENT_VERSION = 0

    user_name = types.StringType(required=True)  # type: str
    user_id = types.IntType(required=True)  # type: int
    users_groups = types.ListType(types.StringType, required=False)  # type: str


class DaeAclRequestType:
    """
    The different types of DAE ACL Request that can be run
    """
    grant = 'grant'
    revoke = 'revoke'


class DaeAclPrivilegeType:
    """
    The different types of access that can be passed from a DAE ACL Request
    """
    select = 'select'
    update = 'update'
    create = 'create'
    drop = 'drop'
    alter = 'alter'
    index = 'index'
    lock = 'lock'
    all_privileges = 'all'
    read = 'read'
    write = 'write'


class DatabricksAclPrivilegeType:
    """
    The different types of access that can be applied to a Databricks ACL Statement
    """
    select = 'SELECT'
    create = 'CREATE'
    modify = 'MODIFY'
    read_metadata = 'READ_METADATA'
    create_named_function = 'CREATE_NAMED_FUNCTION'
    all_privileges = 'ALL PRIVILEGES'


class DaeDatabase(StrictModel, VersionedModel):
    """
    Database as represented in a DAE ACL Request Message
    """
    name = types.StringType(required=True)
    tables = types.ListType(types.StringType, required=True)
    excluded_tables = types.ListType(types.StringType)
    accesses = types.ListType(types.StringType)


class DaePermissions(StrictModel, VersionedModel):
    """
    Permissions as represented in a DAE ACL Request Message
    Attributes:
        grants (list): A list of the databases to grant access for.
        revokes (list): A list of the database to revoke access for.
    """
    grants = types.ListType(types.ModelType(DaeDatabase, required=True))
    revokes = types.ListType(types.ModelType(DaeDatabase, required=True))
    groups = types.ListType(types.StringType(required=True))


class DaeAclRequestMessage(StrictModel, VersionedModel):
    """
    Minimal processing information about a received DAE ACL request.
    Attributes:
        correlation_id (str): A unique identifier for this request (can be used to look up the request from DAE).
        session_id (str): A unique identifier for the user's session (used to update the session in DAE when an ACL response message is sent back).
        principal (str): The requesting users email address.
        permissions (object): Contains the grant and revoke permissions for a user.
        collab_name (str): The normalised form of the agreement name.
        collab_space (bool): A flag to determine if a collaboration space should be created for the current agreement.
    """
    CURRENT_VERSION = 4

    correlation_id = types.StringType(required=True)
    session_id = types.StringType(required=True)
    principal = types.StringType(required=True)
    permissions = types.ModelType(DaePermissions, required=True)
    collab_name = types.StringType(required=True)
    collab_space = types.BooleanType(required=True)


class DaeAclResponseMessage(StrictModel, VersionedModel):
    """
    Minimal processing information about a DAE ACL response message.
    Attributes:
        correlation_id (str): A unique identifier for this request (can be used to look up the request from DAE).
        session_id (str): A unique identifier for the user's session (used to update the session in DAE when an ACL response message is sent back).
        request_id (str): This is a copy of the correlation_id until it is removed in DSP-22223.
        principal (str): The requesting users email address.
    """
    CURRENT_VERSION = 0

    correlation_id = types.StringType(required=True)  # type: str
    session_id = types.StringType(required=True)  # type: str
    principal = types.StringType(required=True)  # type: str
    succeeded = types.ListType(types.StringType)
    failed = types.ListType(types.DictType(types.UnionType([types.StringType, types.ListType(types.StringType)])))
    exception = types.StringType()
    run_id = types.IntType()


class ApiResponseError(StrictModel):
    errors = types.ListType(types.StringType, required=True)  # type: List[str]
    filename = types.StringType()  # type: str

    class Options:
        serialize_when_none = False


class PingResponse(StrictModel):
    time = types.DateTimeType(tzd='reject', required=True, default=datetime.utcnow)  # type: datetime
    version = types.StringType(required=True)  # type: str


# pylint: disable=no-self-use
class RunJobRequest(StrictModel):
    job_id = types.IntType(default=None)  # type: int
    job_name = types.StringType(default=None)  # type: str

    jar_params = types.ListType(types.StringType, default=None)
    python_params = types.ListType(types.StringType, default=None)
    spark_submit_params = types.ListType(types.StringType, default=None)

    notebook_params = types.DictType(
        types.UnionType([types.IntType, types.StringType]), default=None
    )  # type: Dict[str, Union[str, int]]

    def validate(self, partial=False, convert=True, app_data=None, **kwargs):
        super().validate(partial=partial, convert=convert, app_data=app_data, **kwargs)

        if not self.job_id and not self.job_name:
            raise DataError({"job_id": "'must supply a job_id or job_name'"})

        if self.job_id and self.job_name:
            raise DataError({"job_id": "'please specify job_id or job_name .. not both'"})

    def to_databricks_safe_primitive(self):

        db_request = self.to_primitive()

        if 'job_name' in db_request:
            del db_request['job_name']

        return db_request


class RunJobResponse(LooseModel):
    run_id = types.IntType(required=True)  # type: int
    number_in_job = types.IntType(required=True)  # type: int
    submitted = types.DateTimeType(
        tzd='reject', required=True, default=datetime.utcnow
    )  # type: datetime


class NotebookTask(LooseModel):
    notebook_path = types.StringType(required=True)  # type: str
    base_parameters = types.DictType(types.UnionType([types.IntType, types.StringType]), default=None)  # type: str


class SparkJarTask(LooseModel):
    jar_uri = types.StringType(default=None)  # type: str
    main_class_name = types.StringType(required=True)  # type: str
    parameters = types.ListType(types.StringType, default=None)  # type: str


class SparkPythonTask(LooseModel):
    python_file = types.StringType(required=True)
    parameters = types.ListType(types.StringType, default=None)


def is_not_none(value: Any) -> bool:
    return value is not None


def exactly_one_of(attrib: str, fields: Iterable[str], *values):
    not_none = sum([is_not_none(v) for v in values])

    fields = ', '.join(fields)

    if not_none == 0:
        raise DataError(
            {attrib: "please specify one of " + fields}
        )

    if not_none > 1:
        raise DataError(
            {attrib: "please specify only one of " + fields + ' (not multiple)'}
        )


class RunSubmitRequest(StrictModel):
    cluster_pool = types.StringType(default=None)  # type: str
    instance_pool = types.StringType(default=None)  # type: str
    as_user = types.StringType(default=None)  # type: str

    run_name = types.StringType(default=None)  # type: str
    timeout_seconds = types.IntType(default=None)  # type: str
    libraries = types.ListType(
        types.DictType(types.UnionType([types.StringType, types.DictType(types.StringType)]))
    )  # type: List[Union[str, Dict[str, str]]]
    existing_cluster_id = types.StringType(default=None)  # type: str
    new_cluster = types.DictType(
        types.UnionType([types.IntType, types.StringType, types.DictType(types.StringType)])
    )  # type: Dict[str, Union[str, int, Dict[str, Any]]]

    notebook_task = types.ModelType(NotebookTask, default=None)  # type: NotebookTask
    spark_jar_task = types.ModelType(SparkJarTask, default=None)  # type: SparkJarTask
    spark_python_task = types.ModelType(SparkPythonTask, default=None)  # type: SparkPythonTask

    def validate(self, partial=False, convert=True, app_data=None, **kwargs):
        super().validate(partial=partial, convert=convert, app_data=app_data, **kwargs)

        exactly_one_of(
            'cluster',
            ['existing_cluster_id', 'new_cluster', 'cluster_pool'],
            self.existing_cluster_id, self.new_cluster, self.cluster_pool
        )

        exactly_one_of(
            'task',
            ['notebook_task', 'spark_jar_task', 'spark_python_task'],
            self.notebook_task, self.spark_jar_task, self.spark_python_task
        )

        if self.as_user and self.cluster_pool == ClusterPools.SYSTEM:
            raise DataError({
                'cluster_pool': "Can only run 'as_user' on an ACLs cluster"
            })

    def to_databricks_safe_primitive(self):

        db_request = self.to_primitive()

        if 'cluster_pool' in db_request:
            del db_request['cluster_pool']

        if 'instance_pool' in db_request:
            del db_request['instance_pool']

        if 'as_user' in db_request:
            del db_request['as_user']

        return db_request


class RunSubmitResponse(LooseModel):
    run_id = types.IntType(required=True)  # type: int
    submitted = types.DateTimeType(
        tzd='reject', required=True, default=datetime.utcnow
    )  # type: datetime


class RunStatusRequest(StrictModel):
    run_id = types.IntType(required=True)


class JobTask(LooseModel):
    notebook_task = types.ModelType(NotebookTask, default=None)  # type: NotebookTask
    spark_jar_task = types.ModelType(SparkJarTask, default=None)  # type: SparkJarTask
    spark_python_task = types.ModelType(SparkPythonTask, default=None)  # type: SparkPythonTask

    def validate(self, partial=False, convert=True, app_data=None, **kwargs):
        super().validate(partial=partial, convert=convert, app_data=app_data, **kwargs)

        exactly_one_of(
            'task',
            ['notebook_task', 'spark_jar_task', 'spark_python_task'],
            self.notebook_task, self.spark_jar_task, self.spark_python_task
        )

    def task(self) -> Union[NotebookTask, SparkJarTask, SparkPythonTask]:
        return self.notebook_task or self.spark_jar_task or self.spark_python_task


class JobsRunsGetResponse(LooseModel):
    job_id = types.IntType(default=None)  # type: int
    run_id = types.IntType(required=True)  # type: int
    number_in_job = types.IntType(default=None)  # type: int
    run_name = types.StringType(default=None)  # type: str
    run_page_url = types.StringType(default=None)  # type: str
    queried = types.DateTimeType(
        tzd='reject', required=True, default=datetime.utcnow
    )  # type: datetime

    state = types.DictType(types.UnionType([types.StringType, types.BooleanType]))  # type: Dict[str, Union[str, bool]]
    cluster_instance = types.DictType(types.StringType)  # type: Dict[str, str]

    start_time = types.DateTimeType(tzd='reject', required=False, default=None)  # type: datetime
    setup_duration = types.IntType(default=0)  # type: int
    execution_duration = types.IntType(default=0)  # type: int
    cleanup_duration = types.IntType(default=0)  # type: int
    trigger = types.StringType(default=None)  # type: str

    task = types.ModelType(JobTask)  # type: JobTask
    overriding_parameters = types.DictType(types.UnionType([
        types.ListType(types.StringType), types.DictType(types.StringType)
    ]))  # type: Dict[str, Union[List[str], Dict[str, str]]]

    @classmethod
    def from_dict(cls, response: Dict[str, Any], databricks_host: str = None):

        if not response:
            return JobsRunsGetResponse({})

        if databricks_host and response.get('run_page_url'):
            response['run_page_url'] = (
                response['run_page_url'].replace('databricks-webapp', 'https://{}'.format(databricks_host))
            )

        keys = [k for k in response.keys()]
        for key in keys:
            if key not in cls._valid_input_keys:  # pylint: disable=no-member
                del response[key]

            if key == 'start_time' and type(response[key]) == int:
                response[key] = datetime.fromtimestamp(response[key] / 1e3)

        return cls(response)


class JobsRunsGetOutputResponse(LooseModel):
    job_id = types.IntType(default=None)  # type: int
    run_id = types.IntType(required=True)  # type: int
    number_in_job = types.IntType(default=None)  # type: int
    run_page_url = types.StringType(default=None)  # type: str
    queried = types.DateTimeType(
        tzd='reject', required=True, default=datetime.utcnow
    )  # type: datetime

    state = types.DictType(types.UnionType([types.StringType, types.BooleanType]))  # type: Dict[str, Union[str, bool]]
    cluster_instance = types.DictType(types.StringType)  # type: Dict[str, str]

    start_time = types.DateTimeType(tzd='reject', required=True)  # type: datetime
    setup_duration = types.IntType(default=0)  # type: int
    execution_duration = types.IntType(default=0)  # type: int
    cleanup_duration = types.IntType(default=0)  # type: int
    trigger = types.StringType(default=None)  # type: str

    notebook_output = types.DictType(
        types.UnionType([types.StringType, types.BooleanType]))  # type: Dict[str, Union[str, bool]]

    @classmethod
    def from_dict(cls, response: Dict[str, Any], databricks_host: str = None):

        if not response:
            return JobsRunsGetResponse({})

        decoded = response['metadata']
        if 'notebook_output' in response:
            decoded['notebook_output'] = response.get('notebook_output', {})

        if databricks_host and decoded.get('run_page_url'):
            decoded['run_page_url'] = (
                decoded['run_page_url'].replace('databricks-webapp', 'https://{}'.format(databricks_host))
            )

        keys = [k for k in decoded.keys()]
        for key in keys:
            if key not in cls._valid_input_keys:  # pylint: disable=no-member
                del decoded[key]

            if key == 'start_time':
                decoded[key] = datetime.fromtimestamp(decoded[key] / 1e3)

        return cls(decoded)

    @property
    def metadata(self) -> Dict[str, Any]:

        primitive = self.to_primitive()

        if 'notebook_output' in primitive:
            del primitive['notebook_output']

        return primitive


class CancelRunRequest(StrictModel):
    run_id = types.IntType(required=True)  # type: int


class GetRunRequest(StrictModel):
    run_id = types.IntType(required=True)  # type: int


class GroupMember(LooseModel):
    value = types.StringType(default=None)
    display = types.StringType(default=None)


class GetGroupResponse(LooseModel):
    display_name = types.StringType(default=None)
    members = types.ListType(types.ModelType(GroupMember))


class GetUserResponse(LooseModel):
    id = types.IntType(required=True)
    user_name = types.StringType(default=None)


class JobsRunsListRequest(StrictModel):
    job_id = types.IntType(default=None)  # type: int
    offset = types.IntType(default=None)  # type: int
    limit = types.IntType(default=None)  # type: int
    active_only = types.BooleanType(default=None)  # type: bool
    completed_only = types.BooleanType(default=None)  # type: bool


class JobsRunsListResponse(LooseModel):
    has_more = types.BooleanType(default=False)  # type: bool
    runs = types.ListType(types.ModelType(JobsRunsGetResponse), default=[])  # type: List[JobsRunsGetResponse]

    @classmethod
    def from_dict(cls, response: Dict[str, Any], databricks_host: str = None):

        if not response:
            return JobsRunsListResponse({})

        decoded = response

        runs = decoded.get('runs')
        if runs:
            decoded['runs'] = [JobsRunsGetResponse.from_dict(run, databricks_host) for run in runs]

        keys = [k for k in response.keys()]
        for key in keys:
            if key not in cls._valid_input_keys:  # pylint: disable=no-member
                del decoded[key]

        return cls(decoded)


_DEFAULT_DRIVER_TYPE = 'r6id.xlarge'
_DEFAULT_WORKER_TYPE = 'r6id.xlarge'
_DEFAULT_INSTANCE_PROFILE = 'db-default-worker'

STATE_RUNNING = 'RUNNING'
STATE_RESIZING = 'RESIZING'
STATE_RESTARTING = 'RESTARTING'
STATE_PENDING = 'PENDING'
STATE_TERMINATED = 'TERMINATED'

ACTIVE_STATES = (STATE_RUNNING, STATE_RESIZING, STATE_RESTARTING, STATE_PENDING)


class ClusterState(StrictModel):
    cluster_id = types.StringType(required=True)  # type: str
    name = types.StringType(required=True)  # type: str
    state = types.StringType(required=True)  # type: str
    run_ids = types.ListType(types.IntType, default=[])  # type: List[int]
    last_job_run = types.DateTimeType(default=datetime.now())  # type: datetime

    @staticmethod
    def from_cluster_info(cluster: Dict[str, Any], active_runs: List[int]):
        return ClusterState(
            dict(
                cluster_id=cluster['cluster_id'],
                name=cluster['cluster_name'],
                state=cluster['state'],
                run_ids=active_runs
            )
        )


class ClusterSpec(StrictModel):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._instance_profile_arn = None

    acls = types.BooleanType(default=True)  # type: bool
    docker_image = types.StringType(default=None)  # type: str
    provision_libraries = types.BooleanType(default=False)  # type: bool
    init_scripts = types.ListType(types.StringType, default=[])  # type: List[str]
    instance_profile = types.StringType(default=_DEFAULT_INSTANCE_PROFILE)  # type: str
    autotermination_minutes = types.IntType(default=10, min_value=10)  # type: int
    availability = types.StringType(default='ON_DEMAND')  # type: str
    spark_version = types.StringType(default=DEFAULT_SPARK_VERSION)  # type: str
    driver_type = types.StringType(default=_DEFAULT_DRIVER_TYPE)  # type: str
    worker_type = types.StringType(default=_DEFAULT_WORKER_TYPE)  # type: str
    min_nodes = types.IntType(default=4)  # type: int
    max_nodes = types.IntType(default=None)  # type: int

    ebs_volume_count = types.IntType(default=None)  # type: int
    ebs_volume_size = types.IntType(default=None)  # type: int

    spark_conf = types.DictType(types.StringType, default=dict())
    custom_tags = types.DictType(types.StringType, default=dict())
    docker_tag = types.StringType(default=None)

    def is_compatible(self, cluster: Dict[str, Any]) -> bool:

        spark_conf = cluster.get('spark_conf', {})

        if (spark_conf.get('spark.databricks.acl.dfAclsEnabled') == 'true') != self.acls:
            return False

        tags = cluster.get('custom_tags', {})
        if (
                (self.provision_libraries or self.docker_image) and tags.get('code_version') != FULL_VERSION_STRING
        ):
            return False

        if self.instance_profile_arn != cluster.get("aws_attributes", {}).get("instance_profile_arn"):
            return False

        return True

    @property
    def instance_profile_arn(self):

        if not self._instance_profile_arn:
            self._instance_profile_arn = instance_profile_to_arn(self.instance_profile)

        return self._instance_profile_arn


class ScalingPolicy(StrictModel):
    name = types.StringType(required=True)  # type: str

    min_size = types.IntType(default=0, min_value=0)  # type: int

    active = types.BooleanType(required=True, default=True)  # type: bool

    active_environments = types.ListType(types.StringType, required=True, default=[])  # type: List[str]

    active_days = types.StringType(required=True, default='*')  # type: str
    active_hours = types.StringType(required=True, default='*')  # type: str

    at_runs_per_cluster = types.IntType(default=0, min_value=0)  # type: int
    at_less_than_free_clusters = types.IntType(default=0, min_value=0)  # type: int

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._active_days = self._decode_active_days()
        self._active_hours = self._decode_active_hours()

    def is_active(self, as_at: datetime = None, env: str = 'local'):

        as_at = as_at or datetime.today()
        day = as_at.isoweekday()
        hour = as_at.hour

        if not self.active:
            return False

        if env not in self.active_environments:
            return False

        if day not in self._active_days:
            return False

        if hour not in self._active_hours:
            return False

        return True

    def _decode_active_days(self) -> AbstractSet[int]:
        active_days = (self.active_days or '').strip()
        if not active_days:
            return frozenset()

        if active_days == '*':
            return frozenset(range(1, 8))

        parts = active_days.split(',')
        days = []
        for part in parts:
            if '-' in part:
                min_day, max_day = part.split('-')
                min_day = int(min_day)
                max_day = int(max_day)
            else:
                min_day = int(part)
                max_day = int(part)
            min_day = max(min_day, 0)
            max_day = min(max_day, 7)
            days.extend(range(min_day, max_day + 1))

        return frozenset(d for d in days)

    def _decode_active_hours(self) -> AbstractSet[int]:
        active_hours = (self.active_hours or '').strip()
        if not active_hours:
            return frozenset()

        if active_hours == '*':
            return frozenset(range(0, 24))

        parts = active_hours.split(',')
        hours = []
        for part in parts:
            if '-' in part:
                min_hour, max_hour = part.split('-')
                min_hour = int(min_hour)
                max_hour = int(max_hour)
            else:
                min_hour = int(part)
                max_hour = int(part)
            min_hour = max(min_hour, 0)
            max_hour = min(max_hour, 23)
            hours.extend(range(min_hour, max_hour + 1))

        return frozenset(h for h in hours)


class ClusterPool(StrictModel, VersionedModel):
    CURRENT_VERSION = 0

    name = types.StringType(required=True)  # type: str
    max_size = types.IntType(default=10)  # type: int

    spec = types.ModelType(ClusterSpec, required=True)  # type: ClusterSpec

    clusters = types.DictType(types.ModelType(ClusterState), default=OrderedDict())  # type: Dict[str, ClusterState]

    scaling = types.ListType(types.ModelType(ScalingPolicy), default=[])  # type: List[ScalingPolicy]

    max_runs_per_cluster = types.IntType(default=1)  # type: int

    override_min_pool_size = types.IntType(default=None)  # type: int

    retained = types.BooleanType(default=False)  # type: bool

    def next_available_cluster(self, allowed_states=ACTIVE_STATES):

        for state in allowed_states:
            for cluster_id, cluster_state in self.clusters.items():  # pylint: disable=no-member

                if cluster_state.state != state:
                    continue

                if len(cluster_state.run_ids) >= self.max_runs_per_cluster:
                    continue

                return cluster_id

        return None

    def _filter_available_clusters(
            self, predicate: Callable[[ClusterState, int], bool] = None
    ) -> Generator[ClusterState, None, None]:

        for cluster_id, state in self.clusters.items():

            runs = len(state.run_ids)
            if runs >= self.max_runs_per_cluster:
                continue

            if predicate and not predicate(state, runs):
                continue

            yield state

    def should_auto_scale(self, env: str) -> bool:

        clusters = len(self.clusters)

        if self.override_min_pool_size is not None:
            return clusters < self.override_min_pool_size

        if not self.scaling:
            return False

        if self.max_size and clusters >= self.max_size:
            return False

        for policy in self.scaling:

            if not policy.is_active(env=env):
                continue

            if policy.min_size and clusters < policy.min_size:
                return True

            if not policy.at_less_than_free_clusters and not policy.at_runs_per_cluster:
                continue

            if policy.at_less_than_free_clusters and clusters < policy.at_less_than_free_clusters:
                return True

            if not clusters:
                continue

            available_clusters = len(
                [
                    c for c in (
                    self._filter_available_clusters() if not policy.at_runs_per_cluster
                    else self._filter_available_clusters(lambda _c, runs: runs < policy.at_runs_per_cluster)
                )
                ]
            )

            if policy.at_less_than_free_clusters and available_clusters < policy.at_less_than_free_clusters:
                return True

            if available_clusters < 1:
                return True

        return False

    def get_cluster_to_keep_alive(self, env: str) -> Optional[str]:
        if self.retained and not self.scaling:
            max_min_size = 1  # Modify once you know what to set this to
        else:
            if not self.scaling or self.override_min_pool_size == 0:
                return None

            max_min_size = (
                    self.override_min_pool_size or
                    max([0] + [policy.min_size for policy in self.scaling if
                               policy.min_size and policy.is_active(env=env)])
            )

        if not max_min_size:
            return None

        mins_ago = self.spec.autotermination_minutes - 1

        keepalive_if_last_run_before_than = datetime.now() - relativedelta(minutes=mins_ago)

        top_n_keepalive_and_last_runs = sorted([
            (self.clusters[cluster_id].last_job_run, cluster_id)
            for ix, cluster_id in enumerate(self.clusters.keys())
            if ix < max_min_size and self.clusters[cluster_id].last_job_run < keepalive_if_last_run_before_than
        ])

        if not top_n_keepalive_and_last_runs:
            return None

        _, cluster_id = top_n_keepalive_and_last_runs[0]

        return cluster_id


class DeIdJobStatus:
    Pending = 'pending'  # Job is created
    Ready = 'ready'  # Job is ready to be processed
    Processing = 'processing'  # Job is being processed
    Failed = 'failed'  # Job has failed to be processed
    Success = 'success'  # Job was successfully processed
    Cancelled = 'cancelled'  # Job was cancelled before being processed

    @staticmethod
    def all_statuses() -> List[str]:
        return [
            DeIdJobStatus.Pending,
            DeIdJobStatus.Ready,
            DeIdJobStatus.Processing,
            DeIdJobStatus.Failed,
            DeIdJobStatus.Success,
            DeIdJobStatus.Cancelled,
        ]


class DeIdJobSourceFormat:
    Parquet = 'parquet'
    JSON = 'json'


class DeIdJob(StrictModel, VersionedModel):
    CURRENT_VERSION = 1

    id = types.IntType(required=True)  # type: int
    runs = types.ListType(types.ModelType(Run), default=[])  # type: List
    submission_id = types.IntType(required=True)  # type: int
    status = types.StringType(
        choices=common.enumlike_values(DeIdJobStatus),
        required=True
    )  # type: str
    created = types.DateTimeType(required=True)  # type: datetime
    finished = types.DateTimeType()  # type: datetime
    domain = types.StringType(required=True)  # type: str
    source = types.StringType(required=True)  # type: str
    source_format = types.StringType(
        choices=common.enumlike_values(DeIdJobSourceFormat),
        required=True
    )  # type: str
    test_scope = types.StringType(required=False)  # type: str
    extract_id = types.StringType(required=False)  # type: str
    resume_deid_submission = types.BooleanType(required=False)  # type: bool
    complete_after_retokenisation = types.BooleanType(required=False, default=False)  # type: bool

    def get_block_group(self):  # pylint; disable:no-self-use
        sender_id = 'dsp-de-id'  # not really used at the moment
        if not self.test_scope:
            return sender_id
        return "{}:{}".format(self.test_scope, sender_id)


class ExportReceiverStatus:
    ACKNOWLEDGED = 'ACKNOWLEDGED'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    ERROR = 'ERROR'


class ExportReceiverMeta(StrictModel):
    export_id = types.StringType(required=True)
    status = types.StringType(
        choices=enumlike_values(ExportReceiverStatus),
        required=True
    )
    result_filename = types.StringType()
    error_filename = types.StringType()

    class Options:
        serialize_when_none = False


class ExportReceiverError(StrictModel):
    export_id = types.StringType(required=True)
    message = types.StringType(required=True)
    status_code = types.StringType()
    exception = types.StringType()

    class Options:
        serialize_when_none = False


class TaskStatus:
    Pending = 'pending'
    Running = 'running'
    Failed = 'failed'
    Success = 'success'
    Archived = 'archived'

    @staticmethod
    def all_statuses() -> List[str]:
        return [
            TaskStatus.Pending,
            TaskStatus.Running,
            TaskStatus.Failed,
            TaskStatus.Success,
            TaskStatus.Archived,
        ]


BasicTypes = [
    types.BooleanType, types.IntType, types.DecimalType, types.StringType, types.FloatType, types.DateTimeType
]


class Task(StrictModel):
    id = types.StringType(required=True)
    name = types.StringType(required=True)
    status = types.StringType(
        choices=enumlike_values(TaskStatus),
        required=True
    )
    complete = types.BooleanType(default=False)
    parent_id = types.StringType(required=False)
    created = types.DateTimeType(required=True, tzd='reject')
    finished = types.DateTimeType(required=False, tzd='reject')
    last_updated = types.DateTimeType(required=True, tzd='reject')
    result = types.UnionType([
        *BasicTypes,
        types.ListType(
            types.UnionType(
                [
                    *BasicTypes, types.ListType(types.UnionType(BasicTypes))
                ]
            )
        ),
        types.DictType(types.UnionType(
            [*BasicTypes, types.DictType(types.UnionType(BasicTypes)), types.ListType(types.UnionType(BasicTypes))]))
    ], default=dict, required=False)


class GPDataSubmissionChannel:
    Mesh = 'mesh'
    Aws = 'aws'


class GPDataJobStatus:
    Pending = 'pending'  # Submission is ready to be processed
    Running = 'running'  # Submission is currently being processed
    Failed = 'failed'  # Submission has failed to be processed
    Success = 'success'  # Submission was successfully processed
    Rejected = 'rejected'  # Submission was rejected due to validation failures
    Archived = 'archived'  # Submission was archived

    @staticmethod
    def all_statuses() -> List[str]:
        return [
            GPDataJobStatus.Pending,
            GPDataJobStatus.Running,
            GPDataJobStatus.Failed,
            GPDataJobStatus.Success,
            GPDataJobStatus.Rejected,
            GPDataJobStatus.Archived
        ]

    @staticmethod
    def blocking_statuses() -> List[str]:
        return [
            GPDataJobStatus.Running,
            GPDataJobStatus.Failed,
        ]


class GPDataSubmission(StrictModel, VersionedModel):
    """
    A single GP Data Submission

    Attributes:
        id: ID of the submission
        filename: submission filename
        filesize: submission file size
        working_folder: submission working folder
        status: submission status
        dataset_id: one of GP Data datasets
        dataset_version: optional version of the dataset (for multi version support)
        effective_date: date for which the record was generated
        submission_channel: MESH or AWS
        sender_id: supplier name if channel is AWS, mailbox if channel is MESH
        version_id: oldest version of the submission file
        received: datetime when the record was written to S3 bucket
        finished: datetime when the submission processing finished
        runs: list of Runs related to this submission;
        test_scope: test scope (used to manage feature test submissions)
    """

    CURRENT_VERSION = 1

    # id used internally by DPS
    id = types.IntType(required=True)  # type: int
    # id provided by sender
    message_id = types.StringType(required=False)  # type: str
    filename = types.StringType(required=True)  # type: str
    filesize = types.IntType(required=True)  # type: int
    working_folder = types.StringType(required=True)  # type: str
    status = types.StringType(choices=common.enumlike_values(
        GPDataJobStatus), required=True)  # type: str
    dataset_id = types.StringType(choices=list(SUBMISSION_DATASET_IDS.values()) + GP_DATA_ADHOC_DATASETS,
                                  required=True)  # type: str
    dataset_version = types.StringType(required=False)  # type: str
    effective_date = types.DateType(required=True, default=datetime.today())  # type: date
    submission_channel = types.StringType(choices=common.enumlike_values(GPDataSubmissionChannel),
                                          required=True)  # type: str
    sender_id = types.StringType(required=True)  # type: str
    version_id = types.StringType(required=False)  # type: str
    received = types.DateTimeType(required=True, tzd='reject',
                                  default=datetime.utcnow)  # type: datetime
    finished = types.DateTimeType(tzd='reject')  # type: datetime
    runs = types.ListType(types.ModelType(Run), default=[])  # type: List[Run]
    test_scope = types.StringType(required=False)  # type: str

    def file_path(self, gp_data_bucket_path: str) -> str:
        return ("" if self.working_folder.startswith("s3://") else f"{gp_data_bucket_path}/") + \
               f"{self.working_folder}/{self.filename}"

    def source_folder(self) -> str:
        """ Root folder containing all files that were uploaded to S3 from/for a particular supplier, or MESH """
        return self.sender_id if self.submission_channel == GPDataSubmissionChannel.Aws else "mesh"

    def channel_id(self) -> str:
        gp_data_file = parse_filename(self.filename)
        return gp_data_file.chan_id

    def mailbox_from(self) -> Union[str, None]:
        """gets mailbox_from if mesh submission"""
        return self.sender_id if self.submission_channel == GPDataSubmissionChannel.Mesh else None

    def get_block_group(self) -> str:
        channel_id = self.channel_id()
        if not self.test_scope:
            return channel_id

        return f"{self.test_scope}:{channel_id}"


def api_error(errors: List = None, status: int = 500, filename: str = None) -> Tuple[ApiResponseError, int]:
    return ApiResponseError(dict(
        errors=errors or [],
        filename=filename
    )), status


def forbidden(errors: List = None) -> Tuple[ApiResponseError, int]:
    return api_error(errors=errors, status=403)


def unauthorized(errors: List = None) -> Tuple[ApiResponseError, int]:
    return api_error(errors=errors, status=401)


def bad_request(errors: List = None) -> Tuple[ApiResponseError, int]:
    return api_error(errors=errors, status=400)


def not_found(errors: List = None) -> Tuple[ApiResponseError, int]:
    return api_error(errors=errors, status=404)


def unprocessable_entity(errors: List = None) -> Tuple[ApiResponseError, int]:
    return api_error(errors=errors, status=422)


@dataclass(frozen=True)
class TableACL:
    _acl_re = re.compile(r'^/((?:\w+)+)(/`(?:\w+_*)+`)?(?:/DATABASE/(`(?:\w+_*)+`))?(?:/TABLE/(`(?:\w+_?)+`))?$')

    action: str = field(init=False)
    target: str = field(init=False)
    is_deny: bool = field(init=False)
    is_ownership: bool = field(init=False)
    grant: str = field(init=False)
    deny: str = field(init=False)
    revoke: str = field(init=False)
    target_type: str = field(init=False)
    action_raw: str
    path: str

    def _decode_acl_path(self, path) -> Tuple[Optional[str], Optional[str]]:
        match = self._acl_re.match(path)
        if not match:
            return None, None

        root_obj, catalog, database, table = match.groups()
        if database is None:
            return root_obj, root_obj.replace('_', ' ')

        if table is None:
            return 'DATABASE', f'DATABASE {database}'

        return 'TABLE', f'{database}.{table}'

    def __post_init__(self):

        target_type, target = self._decode_acl_path(self.path)

        object.__setattr__(self, 'target', target)
        object.__setattr__(self, 'target_type', target_type)
        action = self.action_raw
        is_deny = action.startswith('DENIED_')
        if is_deny:
            action = action.replace('DENIED_', '')

        object.__setattr__(self, 'is_deny', is_deny)
        object.__setattr__(self, 'action', action)
        object.__setattr__(self, 'is_ownership', action == 'OWN')

        object.__setattr__(self, 'grant', self._grant())
        object.__setattr__(self, 'deny', self._deny())
        object.__setattr__(self, 'revoke', self._revoke())

    def _grant(self):
        if self.is_ownership:
            return f"ALTER {self.target} OWNER TO `%s`"

        return f"GRANT {self.action} ON {self.target} TO `%s`"

    def _deny(self):
        if self.is_ownership:
            return None
        return f"DENY {self.action} ON {self.target} TO `%s`"

    def _revoke(self):
        if self.is_ownership:
            return None
        return f"REVOKE {self.action} ON {self.target} FROM `%s`"


class S3Submission(StrictModel, VersionedModel):
    """
    A single Submission from S3.

    Attributes:
        id: ID of the s3 submission
        status: one of PipelineStatus
        received: datetime when the record was received by dsp core
        finished: datetime when the Submission read a terminal PipelineStatus - e.g. it failed or succeeded
        dataset_id: one of constants.DS
        submission_id: the submission_id created for this
        metadata: S3SubmissionMetadata instance
    """

    # any schema changes in S3Submission's embedded models:
    # PipelineResult, Run, SubmissionStage, SubmissionMetadata, SubmittedMetadata
    # should trigger a CURRENT_VERSION update on Submission
    CURRENT_VERSION = 0

    id = types.IntType(required=True)  # type: int
    status = types.StringType(choices=common.enumlike_values(
        PipelineStatus), required=True)  # type: str
    received = types.DateTimeType(required=True, tzd='reject',
                                  default=datetime.utcnow)  # type: datetime
    finished = types.DateTimeType(tzd='reject')  # type: datetime
    dataset_id = types.StringType(choices=common.enumlike_values(constants.DS), required=True)  # type: str
    filename = types.StringType(required=True)  # type: str
    sub_directories = types.StringType(default='', required=False)  # type: str
    submission_id = types.IntType(required=False)  # type: int
    event_data = types.StringType(default='', required=False)  # type: str
    test_scope = types.StringType(required=False)  # type: str

    def get_block_group(self):  # pylint; disable:no-self-use
        sender_id = 'dsp-s3-submissions'  # not really used at the moment
        if not self.test_scope:
            return sender_id
        return "{}:{}".format(self.test_scope, sender_id)


class GenericFileTypeConfiguration(StrictModel, VersionedModel):
    CURRENT_VERSION = 0

    file_type = types.StringType(choices=[constants.FT.CSV, constants.FT.JSONL,
                                          constants.FT.XML, constants.FT.PARQUET], required=True)  # type: str
    delimiter = types.StringType(default=',', choices=[',', '|', '~', '\t'])
    quote = types.StringType(default='"', choices=['"', "'"])
    escape = types.StringType(default='\\', choices=['\\', '"'])
    rowtag = types.StringType(default='row', required=False)


class CpEmailStatus(Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class CpEmail(StrictModel):
    request_id = types.StringType(required=True)
    template_name = types.StringType(required=True)
    recipients = types.StringType(required=True)
    content_data = types.StringType(required=True, default="{}")
    requested = types.StringType(required=True)
    completed = types.StringType(required=True, default="-1")
    status = types.StringType(
        required=True,
        default=CpEmailStatus.PENDING.value,
        choices=[f.value for f in CpEmailStatus]
    )
    comment = types.StringType(required=True, default="-")

    @classmethod
    def from_dynamodb(cls, dynamodb_record: Dict[str, Any]):
        return cls(dict(
            request_id=dynamodb_record["request_id"]["S"],
            recipients=dynamodb_record["recipients"]["S"],
            template_name=dynamodb_record["template_name"]["S"],
            requested=dynamodb_record["requested"]["S"],
            content_data=dynamodb_record["content_data"]["S"],
            status=dynamodb_record["status"]["S"],
        ))


class GenericPipelineConfig(StrictModel, VersionedModel):
    CURRENT_VERSION = 0

    pipeline_name = types.StringType(required=True)  # type: str
    created = types.DateTimeType(
        required=True,
        tzd='reject',
        default=datetime.utcnow
    )  # type: datetime
    workflow = types.StringType(required=True)  # type: str
    database = types.StringType(required=True)  # type: str
    table = types.StringType(required=True)  # type: str
    file_type_config = types.ModelType(GenericFileTypeConfiguration, required=True)
    allowed_mailboxes = types.ListType(types.StringType(), required=True)
    receiver_mailboxes = types.ListType(types.StringType(), required=False)
    target_email = types.StringType(required=False)

    # allow model to be pickled so it can be stored in ingestion pipeline context
    def __getstate__(self):
        return self.to_native()

    def __setstate__(self, kwargs: Dict[str, any]):
        self.__init__(kwargs)


class UpdateDaeUserRequest(StrictModel):
    username = types.StringType(required=True)  # type: str
    groups = types.ListType(types.StringType(required=True))  # type: List[str]


class UpdateDaeUserResponse(StrictModel):
    successful = types.BooleanType(required=True)  # type: bool
    description = types.StringType(required=True)  # type: str
    user = types.ModelType(DatabricksUser, default=None)  # type: Optional[DatabricksUser]


class AddCollabWorkspaceRequest(StrictModel):
    workspace_name = types.StringType(required=True)  # type: str


class AddCollabWorkspaceResponse(StrictModel):
    successful = types.BooleanType(required=True)  # type: bool
    description = types.StringType(required=True)  # type: str


class AddPoolResponse(StrictModel):
    successful = types.BooleanType(required=True)  # type: bool
    pool_name = types.StringType(required=True)  # type: str


class S3TransferJobStatus:
    """ The different states a S3 Transfer job can move through """
    Ready = 'ready'
    Processing = 'processing'
    Failed = 'failed'
    Success = 'success'
    Archived = 'archived'


class DarsMessageType:
    ExtractRelease = "extract_release"


class DarsSender:
    DigiTrials = "digitrials"
    DataProduction = "data_production"


class DarsMessageStatus:
    """ The different states a dars message job can move through """
    Preparing = 'preparing'
    Ready = 'ready'
    Processing = 'processing'
    Failed = 'failed'
    Success = 'success'
    Archived = 'archived'

    @staticmethod
    def is_terminal(value: str) -> bool:
        return value.lower() in [
            MeshTransferStatus.Success,
            MeshTransferStatus.Archived,
        ]


class DarsMessageExtractReleaseMetadata(StrictModel):
    id = types.StringType(required=False)
    cps_fileref = types.StringType(regex="FILE\d+", required=True)
    cps_productionstage = types.IntType(min_value=923180000, max_value=923180011, required=True)
    # TODO - should this always be populated?
    # cps_dataproducer = types.StringType(required=True)
    cps_dataproducer = types.StringType(required=False)
    cps_productioncomplete = types.BooleanType(default=True, required=False)
    cps_productionmanager = types.StringType(required=False)
    cps_productionsignoffcomplete = types.BooleanType(required=False)
    cps_cycleid = types.StringType(required=False)
    cps_filelocationn = types.StringType(required=True)

    def _claim_polymorphic(data):
        return 'cps_filelocationn' in data


class DarsMessageJob(StrictModel, VersionedModel):
    CURRENT_VERSION = 0
    _message_type_metadata = {
        DarsMessageType.ExtractRelease: DarsMessageExtractReleaseMetadata
    }

    id = types.IntType(required=True)  # type: int
    created = types.DateTimeType(required=True, tzd='reject', default=datetime.utcnow)  # type: datetime
    completed = types.DateTimeType(tzd='reject', required=False)  # type: datetime
    status = types.StringType(choices=common.enumlike_values(DarsMessageStatus), required=True)  # type: str
    sender_id = types.StringType(choices=common.enumlike_values(DarsSender), required=False)  # type: str
    message_type = types.StringType(choices=common.enumlike_values(DarsMessageType), required=True)  # type: str
    message_metadata = types.PolyModelType(_message_type_metadata.values()) # type: dict

    def validate(self, partial=False, convert=True, app_data=None, **kwargs):
        super().validate(partial=partial, convert=convert, app_data=app_data, **kwargs)

        if self.message_type not in self._message_type_metadata:
            raise DataError(
                {'message_type': f'message_type {self.message_type} does not have a metadata mapping configured'}
            )
        
        try:
            self._message_type_metadata[self.message_type](self.message_metadata)
        except DataError as e:
            raise DataError(
                {'message_metadata': f'message_metadata model does not relate to message_type {self.message_type}'}
            ) from e
