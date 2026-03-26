from datetime import datetime
from typing import Iterable

import boto3
import pytest
from botocore.exceptions import ClientError

from shared.constants import DS, JOBS
from shared.models import GPDataJobStatus, Run
from shared.models_tests import valid_gp_data_submission
from shared.store.blocks import BlockStore
from shared.store.gp_data_submissions import GPDataSubmissionsStore


@pytest.fixture(scope='function')
def temp_gp_data_submissions(temp_gp_data_submissions_table: boto3.session.Session.resource, temp_blocks: BlockStore
                             ) -> Iterable[GPDataSubmissionsStore]:
    store = GPDataSubmissionsStore(temp_gp_data_submissions_table, temp_blocks)
    yield store


def test_create_submission(temp_gp_data_submissions: GPDataSubmissionsStore):
    submission = valid_gp_data_submission()
    temp_gp_data_submissions.put(submission)

    record = temp_gp_data_submissions.get_by_id(submission.id)
    assert record == submission


def test_get_pending_submissions(temp_gp_data_submissions: GPDataSubmissionsStore):
    submission = valid_gp_data_submission()
    submission.status = GPDataJobStatus.Pending
    temp_gp_data_submissions.put(submission)

    result = [x for x in temp_gp_data_submissions.get_pending_by_dataset_id(submission.dataset_id)]

    assert 1 == len(result)
    assert result[0] == submission
    

@pytest.mark.parametrize("status", [GPDataJobStatus.Running, GPDataJobStatus.Failed])
def test_channel_is_blocked_if_submission_exists_with_blocking_status(
        status: str, temp_gp_data_submissions: GPDataSubmissionsStore):

    blocking_submission = valid_gp_data_submission(
        status=status, dataset_id=DS.GP_DATA_PATIENT,
        filename="GPDRP_PAT_EXTRACT_3a930ffd-81a6-4dae-a844-0cadb6a48282_"
                 "20200331160514_9f9799e3-1aac-4985-832b-a96cb4a30f6a_1.dat.gz")
    temp_gp_data_submissions.put(blocking_submission)

    # submission that is blocked by the blocking submission
    blocked_submission = valid_gp_data_submission(
        status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT,
        filename="GPDRP_PAT_EXTRACT_3a930ffd-81a6-4dae-a844-0cadb6a48282_"
                 "20200331160514_9f9799e3-1aac-4985-832b-a96cb4a30f6a_2.dat.gz"
    )
    temp_gp_data_submissions.put(blocked_submission)

    non_blocked_submisison = valid_gp_data_submission(
        status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT,
        filename="GPDRP_PAT_EXTRACT_4875380b-f639-487e-97f4-e6d53ee337e4_"
                 "20200331160302_5727f24a-7a79-47a7-b619-8af0381c7a7a_1.dat.gz"
    )
    temp_gp_data_submissions.put(non_blocked_submisison)

    non_blocked_pending_submissions = list(
        temp_gp_data_submissions.get_non_blocked_pending_by_dataset_id(DS.GP_DATA_PATIENT))
    assert non_blocked_pending_submissions == [non_blocked_submisison]


@pytest.mark.parametrize("status", [GPDataJobStatus.Success, GPDataJobStatus.Rejected, GPDataJobStatus.Archived])
def test_submissions_with_non_blocking_status_do_not_block_channels(
        status: str, temp_gp_data_submissions: GPDataSubmissionsStore):

    # both submissions have the same channel
    non_pending_submission = valid_gp_data_submission(
        status=status, dataset_id=DS.GP_DATA_PATIENT,
        filename="GPDRP_PAT_EXTRACT_3a930ffd-81a6-4dae-a844-0cadb6a48282_"
                 "20200331160514_9f9799e3-1aac-4985-832b-a96cb4a30f6a_1.dat.gz")
    temp_gp_data_submissions.put(non_pending_submission)

    pending_submission = valid_gp_data_submission(
        status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT,
        filename="GPDRP_PAT_EXTRACT_3a930ffd-81a6-4dae-a844-0cadb6a48282_"
                 "20200331160514_9f9799e3-1aac-4985-832b-a96cb4a30f6a_2.dat.gz"
    )
    temp_gp_data_submissions.put(pending_submission)

    non_blocked_pending_submissions = list(
        temp_gp_data_submissions.get_non_blocked_pending_by_dataset_id(DS.GP_DATA_PATIENT))
    assert non_blocked_pending_submissions == [pending_submission]


def test_first_pending_submission_blocks_further_submissions_from_same_channel(
        temp_gp_data_submissions: GPDataSubmissionsStore):

    # both pending submissions have the same channel
    first_pending_submission = valid_gp_data_submission(
        status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT, id=1,
        filename="GPDRP_PAT_EXTRACT_3a930ffd-81a6-4dae-a844-0cadb6a48282_"
                 "20200331160514_9f9799e3-1aac-4985-832b-a96cb4a30f6a_1.dat.gz")
    temp_gp_data_submissions.put(first_pending_submission)

    second_pending_submission = valid_gp_data_submission(
        status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT, id=2,
        filename="GPDRP_PAT_EXTRACT_3a930ffd-81a6-4dae-a844-0cadb6a48282_"
                 "20200331160514_9f9799e3-1aac-4985-832b-a96cb4a30f6a_2.dat.gz"
    )
    temp_gp_data_submissions.put(second_pending_submission)

    non_blocked_pending_submissions = list(
        temp_gp_data_submissions.get_non_blocked_pending_by_dataset_id(DS.GP_DATA_PATIENT))
    assert non_blocked_pending_submissions == [first_pending_submission]


def test_no_submissions_are_returned_if_entire_job_is_blocked(temp_gp_data_submissions: GPDataSubmissionsStore):
    for i in range(2):
        submission = valid_gp_data_submission(id=i, status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT)
        temp_gp_data_submissions.put(submission)

    temp_gp_data_submissions._blocks_store.add_super_block_for_job(JOBS.GP_DATA_INGESTION)

    non_blocked_pending = list(temp_gp_data_submissions.get_non_blocked_pending_by_dataset_id(DS.GP_DATA_PATIENT))
    assert non_blocked_pending == []


def test_create_submission_set_job_running(temp_gp_data_submissions: GPDataSubmissionsStore):
    submission = valid_gp_data_submission(status=GPDataJobStatus.Pending)
    submission.runs = []
    temp_gp_data_submissions.put(submission)

    result = temp_gp_data_submissions.mark_running(submission.id, Run(dict(
        run_id=1
    )))

    assert result is True

    record = temp_gp_data_submissions.get_by_id(submission.id)

    assert record.status == GPDataJobStatus.Running
    assert 1 == len(record.runs)
    assert record.runs[0].run_id == 1


def test_add_extra_run(temp_gp_data_submissions: GPDataSubmissionsStore):
    submission = valid_gp_data_submission()
    submission.runs = []
    temp_gp_data_submissions.put(submission)

    temp_gp_data_submissions.mark_running(submission.id, Run(dict(run_id=1)))
    temp_gp_data_submissions.update_status(submission.id, GPDataJobStatus.Pending)

    result = temp_gp_data_submissions.mark_running(submission.id, Run(dict(run_id=2)))

    assert result is True

    record = temp_gp_data_submissions.get_by_id(submission.id)

    assert record.status == GPDataJobStatus.Running
    assert len(record.runs) == 2
    assert record.runs[0].run_id == 1
    assert record.runs[1].run_id == 2


def test_get_running(temp_gp_data_submissions: GPDataSubmissionsStore):
    running_submission = valid_gp_data_submission(id=1, status=GPDataJobStatus.Running)
    pending_submission = valid_gp_data_submission(id=2, status=GPDataJobStatus.Pending)

    temp_gp_data_submissions.put(running_submission)
    temp_gp_data_submissions.put(pending_submission)

    assert list(temp_gp_data_submissions.get_running()) == [running_submission]


def test_get_patient_running(temp_gp_data_submissions: GPDataSubmissionsStore):
    running_submission = valid_gp_data_submission(id=1, status=GPDataJobStatus.Running, dataset_id=DS.GP_DATA_PATIENT)
    pending_submission = valid_gp_data_submission(id=2, status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT)

    temp_gp_data_submissions.put(running_submission)
    temp_gp_data_submissions.put(pending_submission)

    assert list(
        temp_gp_data_submissions.get_by_dataset_status(DS.GP_DATA_PATIENT, GPDataJobStatus.Running)
    ) == [running_submission]


def test_get_patient_pending_count_no_submissions(temp_gp_data_submissions: GPDataSubmissionsStore):
    assert temp_gp_data_submissions.get_by_dataset_status_count(DS.GP_DATA_PATIENT, GPDataJobStatus.Pending) == 0


def test_get_patient_pending_count_two_submissions(temp_gp_data_submissions: GPDataSubmissionsStore):
    submission_1 = valid_gp_data_submission(id=1, status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT)
    submission_2 = valid_gp_data_submission(id=2, status=GPDataJobStatus.Pending, dataset_id=DS.GP_DATA_PATIENT)
    temp_gp_data_submissions.put(submission_1)
    temp_gp_data_submissions.put(submission_2)
    assert temp_gp_data_submissions.get_by_dataset_status_count(DS.GP_DATA_PATIENT, GPDataJobStatus.Pending) == 2


def test_get_ids_by_key(temp_gp_data_submissions: GPDataSubmissionsStore):
    temp_gp_data_submissions.put(valid_gp_data_submission(id=1, filename="test", working_folder="supplier1/date1"))
    temp_gp_data_submissions.put(valid_gp_data_submission(id=2))
    temp_gp_data_submissions.put(valid_gp_data_submission(id=3, filename="test", working_folder="supplier1/date1"))
    temp_gp_data_submissions.put(valid_gp_data_submission(id=4, filename="test", working_folder="supplier2/date1"))
    temp_gp_data_submissions.put(valid_gp_data_submission(id=5, filename="test1", working_folder="supplier1/date1"))

    records = temp_gp_data_submissions.get_ids_by_key("supplier1/date1/test")
    assert records == [1, 3]


def test_get_ids_by_key_with_bucket(temp_gp_data_submissions: GPDataSubmissionsStore):
    temp_gp_data_submissions.put(valid_gp_data_submission(id=1, filename="test", working_folder="s3://local-testing/supplier1/date1"))
    temp_gp_data_submissions.put(valid_gp_data_submission(id=2))
    temp_gp_data_submissions.put(valid_gp_data_submission(id=3, filename="test", working_folder="s3://local-testing/supplier1/date1"))
    temp_gp_data_submissions.put(valid_gp_data_submission(id=4, filename="test", working_folder="s3://local-testing/supplier2/date1"))
    temp_gp_data_submissions.put(valid_gp_data_submission(id=5, filename="test1", working_folder="s3://local-testing/supplier1/date1"))

    records = temp_gp_data_submissions.get_ids_by_key("supplier1/date1/test", "local-testing")
    assert records == [1, 3]


def test_conditional_update_status(temp_gp_data_submissions: GPDataSubmissionsStore):
    submission = valid_gp_data_submission()
    temp_gp_data_submissions.put(submission)

    result = temp_gp_data_submissions.mark_running(submission.id, Run(dict(
        run_id=1
    )))

    assert result is True

    updated = temp_gp_data_submissions.update_status(submission.id, GPDataJobStatus.Failed)

    assert updated is True

    updated = temp_gp_data_submissions.update_status(submission.id, GPDataJobStatus.Pending,
                                                     check_existing_status_is=GPDataJobStatus.Failed)

    assert updated is True

    updated = temp_gp_data_submissions.update_status(
        submission.id, GPDataJobStatus.Pending, check_existing_status_is=GPDataJobStatus.Success,
        raise_if_condition_fails=False
    )

    assert updated is False

    with pytest.raises(ClientError):
        temp_gp_data_submissions.update_status(submission.id, GPDataJobStatus.Pending,
                                               check_existing_status_is=GPDataJobStatus.Success,
                                               raise_if_condition_fails=True)
