import re
from typing import Optional, Tuple

from dsp.shared.constants import DS, MESH_WORKFLOW_ID, DigiTrialsPipelines
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from dsp.shared.logger import log_action, add_fields
from dsp.shared.models import PipelineStatus, MeshTransferStatus
from dsp.shared.store import MeshUploadJobs
from dsp.shared.store.submissions import Submissions

NIC_NUMBER_PATTERN = re.compile(r".*?(?P<nic_number>NIC-\d+-[a-zA-Z0-9]{5}).*")

def namespace_and_dataset(dataset_id: str) -> Tuple[str, Optional[str]]:
    if not dataset_id:
        raise ValueError('You must provide a dataset id')
    values = dataset_id.split(':')
    if 1 >= len(values) >= 2:
        raise ValueError('Invalid dataset_id: %s', dataset_id)
    # We have a two part dataset_id in the form <dataset_id>:<workspace_name>
    if len(values) == 2:
        return values[0], values[1]
    # We have one of our "hard coded" datasets (e.g. mhsds, msds, etc.)
    elif values[0] in [getattr(DS, a) for a in dir(DS) if not a.startswith('__')]:
        return values[0], None
    # Otherwise assume a DMS_UPLOAD with only the workspace_name passed
    return DS.DMS_UPLOAD, values[0]


def get_unique_month_id(rp_start_date: [date, str]):
    if type(rp_start_date) == str:
        rp_start_date = datetime.strptime(rp_start_date, '%Y-%m-%d')
    uniq_month_start_date = datetime(1900, 4, 1)
    time_diff = relativedelta(rp_start_date, uniq_month_start_date)
    return time_diff.years * 12 + time_diff.months + 1


@log_action(log_args=["s3_subdirectory", "filename"])
def parse_s3_subdirectory_for_nic_number(s3_subdirectory: str, filename: str = None):
    """
    Parses a subdirectory and optionally a filename as fallback for a valid NIC number, which is the following format:
        NIC-<number>-<5x alphanumeric characters>

    We make no assumption about the rest of the contents of the path, which may contain version information, or a
    suffix providing a more human-readable indication of the contents.
    """
    if not (s3_subdirectory or filename):
        return None

    if s3_subdirectory:
        search = re.search(NIC_NUMBER_PATTERN, s3_subdirectory)
        if search:
            return search.group('nic_number')

    add_fields(couldnt_parse_nic_from_subdirectory=s3_subdirectory)
    if filename:
        search = re.search(NIC_NUMBER_PATTERN, filename)
        if search:
            return search.group('nic_number')

    return None


@log_action(log_args=["nic_number", "datasets", "extra_pipeline_statuses", "current_submission_datetime"])
def any_non_terminal_for_nic_number(
        nic_number: str,
        datasets: list,
        extra_pipeline_statuses: list = None,
        current_submission_datetime: datetime = None
) -> bool:
    """ Determine if any submissions for any given datasets are non-terminal and match the given nic number """
    non_finished_statuses = [
        PipelineStatus.Scheduling, PipelineStatus.Suspended, PipelineStatus.Processed,
        PipelineStatus.Failed, PipelineStatus.Timedout, PipelineStatus.Cancelled
    ]
    if extra_pipeline_statuses:
        non_finished_statuses.extend(extra_pipeline_statuses)
    if current_submission_datetime:
        non_finished_statuses.append(PipelineStatus.Pending)

    for dataset_id in datasets:
        for submission in Submissions.get_by_dataset_status(dataset_id, PipelineStatus.Running, *non_finished_statuses):
            if submission.status == PipelineStatus.Pending and current_submission_datetime and \
                    current_submission_datetime <= submission.submitted:
                continue
            meta = submission.metadata
            if nic_number == parse_s3_subdirectory_for_nic_number(
                    meta.request.extra.get('s3_sub_directories'), meta.filename):
                add_fields(found=True)
                return True

    if any(d for d in [DigiTrialsPipelines.DIGITRIALS_OUT, DigiTrialsPipelines.DIGITRIALS_DCM] if d in datasets):
        non_terminal_mesh_statuses = set(MeshTransferStatus.all_statuses()) - {MeshTransferStatus.Success,
                                                                               MeshTransferStatus.Unprocessable,
                                                                               MeshTransferStatus.Archived}
        non_terminal_mesh_jobs = MeshUploadJobs.get_all_by_workflow_id_status(
            MESH_WORKFLOW_ID.DCM_PROCESSED_COHORT_EXTRACT,
            non_terminal_mesh_statuses)

        jobs_for_nic = (m.transfer_id for m in non_terminal_mesh_jobs
                        if m.mesh_metadata.subject in [nic_number, "FULL_EXTRACT"])
        if any(jobs_for_nic):
            add_fields(found=",".join(jobs_for_nic))
            return True

    add_fields(found=False)
    return False
