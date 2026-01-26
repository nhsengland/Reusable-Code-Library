from datetime import datetime, timedelta
from typing import Generator, List, Dict, Any, Tuple
from uuid import uuid4

import boto3
import pytest

from src.nhs_reusable_code_library.resuable_codes.shared import models
from src.nhs_reusable_code_library.resuable_codes.shared.constants import Blockables, BLOCKTYPE, MESH_WORKFLOW_ID
from src.nhs_reusable_code_library.resuable_codes.shared.models import MeshTransferStatus, MeshMetadata, Block
from src.nhs_reusable_code_library.resuable_codes.shared.store.base import ModelNotFound
from src.nhs_reusable_code_library.resuable_codes.shared.store.blocks import BlockStore
from src.nhs_reusable_code_library.resuable_codes.shared.store.mesh_download_queues import MeshDownloadQueuesStore, JobAlreadyExistsException


@pytest.fixture(scope='function')
def temp_mesh_download_queues(temp_mesh_download_queues_table: boto3.session.Session.resource,
                              temp_blocks) -> Generator[MeshDownloadQueuesStore, None, None]:
    """ Create a store object with the test dynamoDB table """
    store = MeshDownloadQueuesStore(temp_mesh_download_queues_table, temp_blocks)
    yield store


@pytest.fixture(scope='function')
def temp_mesh_download_data(temp_mesh_download_queues: MeshDownloadQueuesStore) -> Tuple[MeshDownloadQueuesStore,
                                                                                         List[Dict[Any, Any]]]:
    """
    Insert some test data into dynamoDB (directly, without using the store) and pass the data and store
    for the temp table to the test method.
    """
    store = temp_mesh_download_queues
    timestamp = datetime.now(datetime.timezone.utc)

    items = list()

    def add_item(created, status, workflow_id, s3_loc) -> None:
        item = dict(
            transfer_id=str(uuid4()),
            created=created,
            message_id=str(created),  # Use created so lexicographically sortable (like MESH message ids)
            status=status,
            workflow_id=workflow_id,
            mesh_metadata=dict(
                subject='Test subject',
            ),
            s3_locations=[s3_loc],
            mailbox_to='MB_TO'
        )
        store.table.put_item(
            Item=models.MeshTransferJob(item).to_primitive()
        )
        items.append(item)

    add_item(timestamp - timedelta(minutes=0), MeshTransferStatus.Preparing, 'WFID_A', 's3_a4')
    add_item(timestamp - timedelta(minutes=5), MeshTransferStatus.Pending, 'WFID_A', 's3_a3')
    add_item(timestamp - timedelta(minutes=35), MeshTransferStatus.Ready, 'WFID_A', 's3_a2')
    add_item(timestamp - timedelta(minutes=65), MeshTransferStatus.Preparing, 'WFID_A', 's3_a1')

    add_item(timestamp - timedelta(minutes=6), MeshTransferStatus.Ready, 'WFID_B', 's3_b4')
    add_item(timestamp - timedelta(minutes=24), MeshTransferStatus.Failed, 'WFID_B', 's3_b3')
    add_item(timestamp - timedelta(minutes=26), MeshTransferStatus.Success, 'WFID_B', 's3_b2')
    add_item(timestamp - timedelta(minutes=31), MeshTransferStatus.Success, 'WFID_B', 's3_b1')

    add_item(timestamp - timedelta(minutes=3), MeshTransferStatus.Pending, 'WFID_C', 's3_c4')
    add_item(timestamp - timedelta(minutes=19), MeshTransferStatus.Ready, 'WFID_C', 's3_c3')
    add_item(timestamp - timedelta(minutes=21), MeshTransferStatus.Processing, 'WFID_C', 's3_c2')
    add_item(timestamp - timedelta(minutes=32), MeshTransferStatus.Success, 'WFID_C', 's3_c1')

    add_item(timestamp - timedelta(minutes=14), MeshTransferStatus.Preparing, 'WFID_D', 's3_d4')
    add_item(timestamp - timedelta(minutes=20), MeshTransferStatus.Success, 'WFID_D', 's3_d3')
    add_item(timestamp - timedelta(minutes=22), MeshTransferStatus.Success, 'WFID_D', 's3_d2')
    add_item(timestamp - timedelta(minutes=39), MeshTransferStatus.Success, 'WFID_D', 's3_d1')

    add_item(timestamp - timedelta(minutes=15), MeshTransferStatus.Ready, 'WFID_E', 's3_e4')
    add_item(timestamp - timedelta(minutes=23), MeshTransferStatus.Pending, 'WFID_E', 's3_e3')
    add_item(timestamp - timedelta(minutes=36), MeshTransferStatus.Ready, 'WFID_E', 's3_e2')
    add_item(timestamp - timedelta(minutes=46), MeshTransferStatus.Success, 'WFID_E', 's3_e1')

    add_item(timestamp - timedelta(minutes=16), MeshTransferStatus.Ready, 'WFID_F', 's3_f4')
    add_item(timestamp - timedelta(minutes=34), MeshTransferStatus.Unprocessable, 'WFID_F', 's3_f3')
    add_item(timestamp - timedelta(minutes=46), MeshTransferStatus.Success, 'WFID_F', 's3_f2')
    add_item(timestamp - timedelta(minutes=51), MeshTransferStatus.Success, 'WFID_F', 's3_f1')

    add_item(timestamp - timedelta(minutes=46), MeshTransferStatus.Ready, MESH_WORKFLOW_ID.GP_DATA_PATIENT, 's3_f1')
    add_item(timestamp - timedelta(minutes=51), MeshTransferStatus.Ready, MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT, 's3_f2')

    return store, items


def test_get_next_ready_job_for_workflow(temp_mesh_download_data):
    """ Test that we can retrieve a ready job for the correct workflow """
    store, items = temp_mesh_download_data
    job = store.get_next_ready_for_workflow('WFID_A')
    assert not job  # as the top of the queue is still pending
    job = store.get_next_ready_for_workflow('WFID_B')
    assert not job  # as there is a failed job blocking
    job = store.get_next_ready_for_workflow('WFID_C')
    assert not job  # as there is a processing job blocking
    job = store.get_next_ready_for_workflow('WFID_D')
    assert not job  # as there are no jobs waiting, just a downloading job
    job = store.get_next_ready_for_workflow('WFID_E')
    assert job
    assert job.single_s3_location() == 's3_e2'
    job = store.get_next_ready_for_workflow('WFID_F')
    assert job
    assert job.single_s3_location() == 's3_f4'


def test_no_ready_job_for_workflow(temp_mesh_download_queues: MeshDownloadQueuesStore):
    """ Test that we receive None when no job is available """
    assert not temp_mesh_download_queues.get_next_ready_for_workflow('WFID_D')
    assert not temp_mesh_download_queues.get_next_ready_for_workflow('dummy')


def test_get_next_ready_job_for_workflow_with_block(temp_mesh_download_data: MeshDownloadQueuesStore,
                                                    temp_blocks: BlockStore):
    store, items = temp_mesh_download_data

    temp_blocks.add_block(job_type=Blockables.RECEIVERS, block_group='WFID_E', dataset_id=Block.ALL,
                          block_type=BLOCKTYPE.GROUP)
    job = store.get_next_ready_for_workflow('WFID_E')
    assert not job

    temp_blocks.remove_block(job_type=Blockables.RECEIVERS, block_group='WFID_E', dataset_id=Block.ALL)
    job = store.get_next_ready_for_workflow('WFID_E')
    assert job

    temp_blocks.add_block(job_type=Blockables.RECEIVERS, block_group=Block.ALL, dataset_id=Block.ALL,
                          block_type=BLOCKTYPE.GROUP)
    job = store.get_next_ready_for_workflow('WFID_E')
    assert not job


def test_mark_preparing(temp_mesh_download_data):
    """ Test we can mark a pending job as ready """
    store, items = temp_mesh_download_data
    assert store.mark_preparing(items[8]['transfer_id'])

    # Test we can re-mark a preparing job as preparing (in the case of a failed download from MESH)
    assert store.mark_preparing(items[8]['transfer_id'])

    assert store.get_by_transfer_id(items[8]['transfer_id']).status == MeshTransferStatus.Preparing


def test_cant_mark_preparing_incorrectly(temp_mesh_download_data):
    """ Test we can't mark a non-pending job as preparing """
    store, items = temp_mesh_download_data
    # check we can't move from the wrong states
    assert not store.mark_preparing(items[2]['transfer_id'])
    assert not store.mark_preparing(items[5]['transfer_id'])
    assert not store.mark_preparing(items[6]['transfer_id'])

    # check we can't move from the right state twice
    assert store.mark_failed(items[10]['transfer_id'])
    assert not store.mark_failed(items[10]['transfer_id'])


def test_mark_ready(temp_mesh_download_data):
    """ Test we can mark a pending job as ready """
    store, items = temp_mesh_download_data
    assert store.mark_ready(items[3]['transfer_id'])
    assert store.get_by_transfer_id(items[3]['transfer_id']).status == MeshTransferStatus.Ready


def test_cant_mark_ready_incorrectly(temp_mesh_download_data):
    """
    Test we can't mark a pending or finished job as completed (including one that is already successfully completed)
    """
    store, items = temp_mesh_download_data
    # check we can't move from the wrong states
    assert not store.mark_ready(items[1]['transfer_id'])
    assert not store.mark_ready(items[5]['transfer_id'])
    assert not store.mark_ready(items[6]['transfer_id'])
    # check we can't move from the right state twice
    assert store.mark_ready(items[3]['transfer_id'])
    assert not store.mark_ready(items[3]['transfer_id'])


def test_mark_processing(temp_mesh_download_data):
    """ Test that we can mark a ready job as processing """
    store, items = temp_mesh_download_data
    assert store.mark_processing(items[18]['transfer_id'])
    assert store.get_by_transfer_id(items[18]['transfer_id']).status == MeshTransferStatus.Processing


def test_cant_mark_processing_incorrectly(temp_mesh_download_data):
    """ Test that we can mark a ready job as processing """
    store, items = temp_mesh_download_data
    # check we can't mark processing when not at the top of the queue
    assert not store.mark_processing(items[0]['transfer_id'])
    assert not store.mark_processing(items[1]['transfer_id'])
    assert not store.mark_processing(items[2]['transfer_id'])
    assert not store.mark_processing(items[3]['transfer_id'])
    assert store.mark_ready(items[3]['transfer_id'])
    assert store.mark_processing(items[3]['transfer_id'])

    # check we can't move from the wrong states
    assert not store.mark_processing(items[5]['transfer_id'])  # failed
    assert not store.mark_processing(items[6]['transfer_id'])  # success
    assert not store.mark_processing(items[10]['transfer_id'])  # already processing


def test_mark_completed(temp_mesh_download_data):
    """ Test we can mark a processing job as completed """
    store, items = temp_mesh_download_data
    assert store.mark_success(items[10]['transfer_id'])
    assert store.get_by_transfer_id(items[10]['transfer_id']).status == MeshTransferStatus.Success


def test_cant_mark_completed_incorrectly(temp_mesh_download_data):
    """
    Test we can't mark a pending or finished job as completed (including one that is already successfully completed)
    """
    store, items = temp_mesh_download_data
    # check we can't move from the wrong states
    assert not store.mark_success(items[0]['transfer_id'])
    assert not store.mark_success(items[5]['transfer_id'])
    assert not store.mark_success(items[6]['transfer_id'])
    # check we can't move from the right state twice
    assert store.mark_success(items[10]['transfer_id'])
    assert not store.mark_success(items[10]['transfer_id'])


def test_mark_failed(temp_mesh_download_data):
    """ Test we can mark a processing job as failed """
    store, items = temp_mesh_download_data
    assert store.mark_failed(items[10]['transfer_id'])
    assert store.get_by_transfer_id(items[10]['transfer_id']).status == MeshTransferStatus.Failed


def test_cant_mark_failed_incorrectly(temp_mesh_download_data):
    """
    Test we can't mark a pending or finished job as failed (including one that is already successfully completed)
    """
    store, items = temp_mesh_download_data
    # check we can't move from the wrong states
    assert not store.mark_failed(items[0]['transfer_id'])
    assert not store.mark_failed(items[5]['transfer_id'])  # already failed
    assert not store.mark_failed(items[9]['transfer_id'])  # pending
    assert not store.mark_failed(items[11]['transfer_id'])  # success
    # check we can't move from the right state twice
    assert store.mark_failed(items[10]['transfer_id'])  # processing
    assert not store.mark_failed(items[10]['transfer_id'])


def test_get_count_for_status(temp_mesh_download_data):
    """ Test we can get the counts for the different statuses """
    store, items = temp_mesh_download_data
    assert store.get_status_count(MeshTransferStatus.Pending) == 3
    assert store.get_status_count(MeshTransferStatus.Preparing) == 3
    assert store.get_status_count(MeshTransferStatus.Ready) == 8
    assert store.get_status_count(MeshTransferStatus.Processing) == 1
    assert store.get_status_count(MeshTransferStatus.Success) == 9
    assert store.get_status_count(MeshTransferStatus.Failed) == 1
    assert store.get_status_count(MeshTransferStatus.Unprocessable) == 1
    # change the state of one entry and check that the counts reflect this
    assert store.mark_processing(items[18]['transfer_id'])
    assert store.get_status_count(MeshTransferStatus.Pending) == 3
    assert store.get_status_count(MeshTransferStatus.Preparing) == 3
    assert store.get_status_count(MeshTransferStatus.Ready) == 7
    assert store.get_status_count(MeshTransferStatus.Processing) == 2
    assert store.get_status_count(MeshTransferStatus.Success) == 9
    assert store.get_status_count(MeshTransferStatus.Failed) == 1
    assert store.get_status_count(MeshTransferStatus.Unprocessable) == 1


def test_get_count_for_status_by_workflow_id(temp_mesh_download_data):
    store, items = temp_mesh_download_data
    assert store.get_status_count(MeshTransferStatus.Pending, 'WFID_A') == 1
    assert store.get_status_count(MeshTransferStatus.Preparing, 'WFID_A') == 2
    assert store.get_status_count(MeshTransferStatus.Ready, 'WFID_A') == 1
    assert store.get_status_count(MeshTransferStatus.Processing, 'WFID_A') == 0
    assert store.get_status_count(MeshTransferStatus.Success, 'WFID_A') == 0
    assert store.get_status_count(MeshTransferStatus.Failed, 'WFID_A') == 0

    assert store.get_status_count(MeshTransferStatus.Pending, 'WFID_B') == 0
    assert store.get_status_count(MeshTransferStatus.Preparing, 'WFID_B') == 0
    assert store.get_status_count(MeshTransferStatus.Ready, 'WFID_B') == 1
    assert store.get_status_count(MeshTransferStatus.Processing, 'WFID_B') == 0
    assert store.get_status_count(MeshTransferStatus.Success, 'WFID_B') == 2
    assert store.get_status_count(MeshTransferStatus.Failed, 'WFID_B') == 1

    assert store.get_status_count(MeshTransferStatus.Pending, 'WFID_C') == 1
    assert store.get_status_count(MeshTransferStatus.Preparing, 'WFID_C') == 0
    assert store.get_status_count(MeshTransferStatus.Ready, 'WFID_C') == 1
    assert store.get_status_count(MeshTransferStatus.Processing, 'WFID_C') == 1
    assert store.get_status_count(MeshTransferStatus.Success, 'WFID_C') == 1
    assert store.get_status_count(MeshTransferStatus.Failed, 'WFID_C') == 0

    assert store.get_status_count(MeshTransferStatus.Pending, 'WFID_D') == 0
    assert store.get_status_count(MeshTransferStatus.Preparing, 'WFID_D') == 1
    assert store.get_status_count(MeshTransferStatus.Ready, 'WFID_D') == 0
    assert store.get_status_count(MeshTransferStatus.Processing, 'WFID_D') == 0
    assert store.get_status_count(MeshTransferStatus.Success, 'WFID_D') == 3
    assert store.get_status_count(MeshTransferStatus.Failed, 'WFID_D') == 0

    assert store.get_status_count(MeshTransferStatus.Pending, 'WFID_E') == 1
    assert store.get_status_count(MeshTransferStatus.Preparing, 'WFID_E') == 0
    assert store.get_status_count(MeshTransferStatus.Ready, 'WFID_E') == 2
    assert store.get_status_count(MeshTransferStatus.Processing, 'WFID_E') == 0
    assert store.get_status_count(MeshTransferStatus.Success, 'WFID_E') == 1
    assert store.get_status_count(MeshTransferStatus.Failed, 'WFID_E') == 0


def test_get_count_for_incorrect_status(temp_mesh_download_data):
    """ Test we can get a 0 for an incorrect status count """
    store, items = temp_mesh_download_data
    assert store.get_status_count('not a status') == 0


def test_get_by_status(temp_mesh_download_data):
    """ Test we can retrieve rows by their status """
    store, items = temp_mesh_download_data
    ready_count = 0
    ready_ids = [items[2]['transfer_id'], items[4]['transfer_id'], items[9]['transfer_id'],
                 items[16]['transfer_id'], items[18]['transfer_id'], items[20]['transfer_id']]

    def is_workflow_allowed_test(workflow_id, mailbox_to):
        return workflow_id not in (MESH_WORKFLOW_ID.GP_DATA_PATIENT, MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT) \
               and mailbox_to == "MB_TO"

    for job in store.get_all_by_status(MeshTransferStatus.Ready, None, is_workflow_allowed_test):
        ready_count += 1
        assert job.transfer_id in ready_ids
    assert ready_count == 6


def test_get_all_pending(temp_mesh_download_data):
    store, items = temp_mesh_download_data
    ready_count = 0
    ready_ids = [items[1]['transfer_id'], items[8]['transfer_id'], items[17]['transfer_id']]
    for job in store.get_all_pending():
        ready_count += 1
        assert job.transfer_id in ready_ids
    assert ready_count == 3


def test_get_all_processing_for_workflow(temp_mesh_download_data):
    store, items = temp_mesh_download_data
    processing_count = 0
    processing_ids = [items[10]['transfer_id']]
    for job in store.get_all_processing('WFID_C'):
        processing_count += 1
        assert job.transfer_id in processing_ids
    assert processing_count == 1


def test_get_all_processing_for_workflow_no_entries(temp_mesh_download_data):
    store, items = temp_mesh_download_data
    processing_count = 0
    processing_ids = []
    for job in store.get_all_processing('WFID_A'):
        processing_count += 1
        assert job.transfer_id in processing_ids
    assert processing_count == 0


def test_get_by_transfer_id(temp_mesh_download_data):
    """ Test that we can get individual snapshots by their unique IDs """
    store, items = temp_mesh_download_data
    item = store.get_by_transfer_id(items[0]['transfer_id'])
    assert item.created == items[0]['created']
    item = store.get_by_transfer_id(items[3]['transfer_id'])
    assert item.created == items[3]['created']


def test_get_by_id_failure(temp_mesh_download_data):
    """ Test that retrieving an item that doesn't exist yields None """
    store, items = temp_mesh_download_data
    with pytest.raises(ModelNotFound):
        store.get_by_transfer_id('not a valid id')


def test_create_mesh_download_job(temp_mesh_download_queues):
    """ Test that we can create a new item on the queue """
    metadata = MeshMetadata(dict(
        filename='myfile.txt',
        subject='mysubject',
    ))
    workflow_id = 'WFID_X'
    s3_loc = 's3://bucket/path/file.txt'
    message_id = 'myid'
    mailbox_from = 'MB_FROM'
    mailbox_to = 'MB_TO'
    transfer_id = temp_mesh_download_queues.create_mesh_download_job(workflow_id=workflow_id,
                                                                     message_id=message_id,
                                                                     s3_location=s3_loc,
                                                                     mesh_metadata=metadata,
                                                                     mailbox_from=mailbox_from,
                                                                     mailbox_to=mailbox_to)
    job = temp_mesh_download_queues.get_by_transfer_id(transfer_id)
    assert transfer_id == job.transfer_id
    assert job.workflow_id == workflow_id
    assert job.status == MeshTransferStatus.Pending
    assert job.single_s3_location() == s3_loc
    assert job.mesh_metadata.filename == metadata.filename
    assert job.mesh_metadata.subject == metadata.subject
    assert job.mailbox_from == mailbox_from
    assert job.mailbox_to == mailbox_to


def test_get_by_workflow_id(temp_mesh_download_data):
    """ Test that we get all items for a given workflow id """
    store, items = temp_mesh_download_data
    wfid_a_count = 0
    wfid_a_ids = [items[0]['transfer_id'], items[1]['transfer_id'], items[2]['transfer_id'], items[3]['transfer_id']]
    for job in store.get_all_by_workflow_id('WFID_A'):
        wfid_a_count += 1
        assert job.transfer_id in wfid_a_ids
    assert wfid_a_count == 4


def test_get_workflow_id_count(temp_mesh_download_data):
    """ Test that we can count all items for a given workflow id """
    store, items = temp_mesh_download_data
    assert store.get_workflow_id_count('WFID_A') == 4


def test_get_workflow_id_count_for_non_existent_workflow_id(temp_mesh_download_data):
    """ Test that we get all items for a non-existent workflow id """
    store, items = temp_mesh_download_data
    assert store.get_workflow_id_count('not a workflow ID') == 0


def test_delete_by_workflow_id_for_tests(temp_mesh_download_data):
    """ Test that we can remove items with a given workflow id """
    store, items = temp_mesh_download_data
    assert store.get_workflow_id_count('WFID_A') == 4
    store.delete_by_workflow_id_for_tests('WFID_A')
    assert store.get_workflow_id_count('WFID_A') == 0


def test_create_duplicate_mesh_download_job(temp_mesh_download_queues):
    metadata = MeshMetadata(dict())
    workflow_id = 'WFID_X'
    s3_loc = 's3://bucket/path/file.txt'
    message_id = 'myid'
    mailbox_from = 'MB_FROM'
    mailbox_to = 'MB_TO'
    transfer_id = temp_mesh_download_queues.create_mesh_download_job(workflow_id=workflow_id,
                                                                     message_id=message_id,
                                                                     s3_location=s3_loc,
                                                                     mesh_metadata=metadata,
                                                                     mailbox_from=mailbox_from,
                                                                     mailbox_to=mailbox_to)
    job = temp_mesh_download_queues.get_by_transfer_id(transfer_id)
    assert job
    assert transfer_id == job.transfer_id

    with pytest.raises(JobAlreadyExistsException):
        _ = temp_mesh_download_queues.create_mesh_download_job(workflow_id=workflow_id,
                                                               message_id=message_id,
                                                               s3_location=s3_loc,
                                                               mesh_metadata=metadata,
                                                               mailbox_from=mailbox_from,
                                                               mailbox_to=mailbox_to)


def test_create_mesh_download_job_with_empty_message_id(temp_mesh_download_queues):
    metadata = MeshMetadata(dict())
    workflow_id = 'WFID_X'
    s3_loc = 's3://bucket/path/file.txt'
    message_id = ''
    mailbox_from = 'MB_FROM'
    mailbox_to = 'MB_TO'

    with pytest.raises(ValueError):
        _ = temp_mesh_download_queues.create_mesh_download_job(workflow_id=workflow_id,
                                                               message_id=message_id,
                                                               s3_location=s3_loc,
                                                               mesh_metadata=metadata,
                                                               mailbox_from=mailbox_from,
                                                               mailbox_to=mailbox_to)


def test_get_transfer_id_by_message_id(temp_mesh_download_data):
    store, items = temp_mesh_download_data
    message_id = items[0]['message_id']
    expected_transfer_id = items[0]['transfer_id']
    transfer_id = store.get_transfer_id_by_message_id(message_id)
    assert expected_transfer_id == transfer_id


def test_get_transfer_id_by_message_id_with_non_existent_message_id(temp_mesh_download_data):
    store, items = temp_mesh_download_data
    transfer_id = store.get_transfer_id_by_message_id('msg_id')
    assert not transfer_id


def _not_raise_error(*args, **kwargs):
    return True


def _raise_error(*args, **kwargs):
    raise Exception


def test_process_job_with_success(temp_mesh_download_data):
    store, items = temp_mesh_download_data

    job = store.get_next_ready_for_workflow('WFID_E')
    transfer_id = job.transfer_id

    store._process_job(job, False, _not_raise_error)
    item = store.get_by_transfer_id(transfer_id)
    assert item.status == MeshTransferStatus.Success


def test_process_job_with_blocked_error(temp_mesh_download_data):
    store, items = temp_mesh_download_data

    job = store.get_next_ready_for_workflow('WFID_E')
    transfer_id = job.transfer_id

    with pytest.raises(Exception):
        store._process_job(job, True, _raise_error)
    item = store.get_by_transfer_id(transfer_id)
    assert item.status == MeshTransferStatus.Failed


def test_process_job_with_non_blocked_error(temp_mesh_download_data):
    store, items = temp_mesh_download_data

    job = store.get_next_ready_for_workflow('WFID_E')
    transfer_id = job.transfer_id

    with pytest.raises(Exception):
        store._process_job(job, False, _raise_error)
    item = store.get_by_transfer_id(transfer_id)
    assert item.status == MeshTransferStatus.Unprocessable
