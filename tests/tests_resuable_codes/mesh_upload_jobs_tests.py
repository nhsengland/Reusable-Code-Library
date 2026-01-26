from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Any
from uuid import uuid4

import boto3
import pytest

from src.nhs_reusable_code_library.resuable_codes.shared.common.test_helpers import smart_open
from src.nhs_reusable_code_library.resuable_codes.shared.constants import Blockables, BLOCKTYPE, Senders
from src.nhs_reusable_code_library.resuable_codes.shared.models import MeshMetadata, MeshTransferStatus, MeshTransferJob, Block
from src.nhs_reusable_code_library.resuable_codes.shared.store.mesh_upload_jobs import MeshUploadJobsStore, MeshTransferJobPriority


@pytest.fixture(scope='function')
def temp_mesh_upload_jobs(temp_mesh_upload_jobs_table: boto3.session.Session.resource, temp_blocks) \
        -> MeshUploadJobsStore:
    return MeshUploadJobsStore(temp_mesh_upload_jobs_table, temp_blocks)


@pytest.fixture(scope='function')
def temp_mesh_upload_data(temp_mesh_upload_jobs: MeshUploadJobsStore) -> Tuple[MeshUploadJobsStore,
List[Dict[Any, Any]]]:
    """
    Insert some test data into Dynamo DB (directly, without using the store) and pass the data and the store for the
    temp table to the test method
    """
    store = temp_mesh_upload_jobs
    items = []
    timestamp = datetime.now(datetime.timezone.utc)

    def add_item(created, status, workflow_id, s3_locs, mailbox_to: str = 'MB_TO', mailbox_from: str = 'MB_FROM'):
        item = dict(
            transfer_id=str(uuid4()),
            created=created,
            status=status,
            workflow_id=workflow_id,
            mesh_metadata=dict(
                subject='Test subject',
            ),
            s3_locations=s3_locs,
            mailbox_from=mailbox_from,
            mailbox_to=mailbox_to
        )
        store.table.put_item(
            Item=MeshTransferJob(item).to_primitive()
        )
        items.append(item)

    add_item(timestamp - timedelta(0), MeshTransferStatus.Preparing, 'WFID_A', ['s3_a4_1', 's3_a4_2'])
    add_item(timestamp - timedelta(5), MeshTransferStatus.Pending, 'WFID_A', ['s3_a3_1', 's3_a3_2'])
    add_item(timestamp - timedelta(39), MeshTransferStatus.Ready, 'WFID_A', ['s3_a2_1', 's3_a2_2'])
    add_item(timestamp - timedelta(65), MeshTransferStatus.Preparing, 'WFID_A', ['s3_a1_1', 's3_a1_2'])

    add_item(timestamp - timedelta(6), MeshTransferStatus.Ready, 'WFID_B', ['s3_b4_1', 's3_b4_2'])
    add_item(timestamp - timedelta(24), MeshTransferStatus.Failed, 'WFID_B', ['s3_b3_1', 's3_b3_2'])
    add_item(timestamp - timedelta(26), MeshTransferStatus.Success, 'WFID_B', ['s3_b2_1', 's3_b2_2'])
    add_item(timestamp - timedelta(31), MeshTransferStatus.Success, 'WFID_B', ['s3_b1_1', 's3_b1_2'])

    add_item(timestamp - timedelta(3), MeshTransferStatus.Pending, 'WFID_C', ['s3_c4_1', 's3_c4_2'])
    add_item(timestamp - timedelta(19), MeshTransferStatus.Ready, 'WFID_C', ['s3_c3_1', 's3_c3_2'])
    add_item(timestamp - timedelta(21), MeshTransferStatus.Processing, 'WFID_C', ['s3_c2_1', 's3_c2_2'])
    add_item(timestamp - timedelta(32), MeshTransferStatus.Success, 'WFID_C', ['s3_c1_1', 's3_c1_2'])

    add_item(timestamp - timedelta(14), MeshTransferStatus.Preparing, 'WFID_D', ['s3_d4_1', 's3_d4_2'])
    add_item(timestamp - timedelta(20), MeshTransferStatus.Success, 'WFID_D', ['s3_d3_1', 's3_d3_2'])
    add_item(timestamp - timedelta(22), MeshTransferStatus.Success, 'WFID_D', ['s3_d2_1', 's3_d2_2'])
    add_item(timestamp - timedelta(39), MeshTransferStatus.Success, 'WFID_D', ['s3_d1_1', 's3_d1_2'])

    add_item(timestamp - timedelta(15), MeshTransferStatus.Ready, 'WFID_E', ['s3_e4_1', 's3_e4_2'])
    add_item(timestamp - timedelta(23), MeshTransferStatus.Pending, 'WFID_E', ['s3_e3_1', 's3_e3_2'])
    add_item(timestamp - timedelta(40), MeshTransferStatus.Ready, 'WFID_E', ['s3_e2_1', 's3_e2_2'])
    add_item(timestamp - timedelta(46), MeshTransferStatus.Success, 'WFID_E', ['s3_e1_1', 's3_e1_2'])

    add_item(timestamp - timedelta(37), MeshTransferStatus.Ready, 'WFID_F', ['s3_f1_1', 's3_f1_2'],
             mailbox_to='MB_TO_2')
    add_item(timestamp - timedelta(36), MeshTransferStatus.Ready, 'WFID_F', ['s3_f2_1', 's3_f2_2'],
             mailbox_from='MB_FROM_2')

    return store, items


@pytest.fixture(scope='function')
def temp_mesh_upload_data_with_priority(temp_mesh_upload_jobs: MeshUploadJobsStore) -> Tuple[MeshUploadJobsStore, List[Dict[Any, Any]]]:
    """
    Insert some test data into Dynamo DB.
    Add some priorities to test correct ordering.
    """
    store = temp_mesh_upload_jobs
    items = []
    timestamp = datetime.utcnow()

    def add_item(created, status, workflow_id, s3_locs, priority=MeshTransferJobPriority.Normal) -> None:
        item = dict(
            transfer_id=str(uuid4()),
            created=created,
            status=status,
            workflow_id=workflow_id,
            mesh_metadata=dict(
                subject='Test subject',
            ),
            s3_locations=s3_locs,
            mailbox_from='MB_FROM',
            mailbox_to='MB_TO',
            priority=priority
        )
        store.table.put_item(
            Item=MeshTransferJob(item).to_primitive()
        )
        items.append(item)

    custom_super_high_priority = 100
    add_item(timestamp - timedelta(60), MeshTransferStatus.Ready, 'WFID_A', ['s3_a2_1', 's3_a2_2'])
    add_item(timestamp - timedelta(40), MeshTransferStatus.Ready, 'WFID_C', ['s3_c3_1', 's3_c3_2'])
    add_item(timestamp - timedelta(20), MeshTransferStatus.Ready, 'WFID_E', ['s3_e4_1', 's3_e4_2'])
    add_item(timestamp - timedelta(20), MeshTransferStatus.Ready, 'WFID_E', ['s3_e2_1', 's3_e2_2'],
             custom_super_high_priority)
    add_item(timestamp - timedelta(10), MeshTransferStatus.Ready, 'WFID_E', ['s3_e2_1', 's3_e2_2'],
             custom_super_high_priority)
    add_item(timestamp - timedelta(5), MeshTransferStatus.Ready, 'WFID_B', ['s3_b4_1', 's3_b4_2'])

    return store, items


def test_create_mesh_upload_job(temp_mesh_upload_jobs: MeshUploadJobsStore):
    workflow_id = 'WFID'

    file1 = f's3://local-testing/{uuid4().hex}.txt'
    file2 = f's3://local-testing/{uuid4().hex}.txt'

    with smart_open(uri=file1, mode='w') as f:
        f.write("*" * 17)
    with smart_open(uri=file2, mode='w') as f:
        f.write("*" * 32)

    s3_locations = [file1, file2]
    mesh_metadata = MeshMetadata(dict(
        filename='myfile.txt',
        subject='mysubject'
    ))
    mailbox_from = 'MB_FROM'
    mailbox_to = 'MB_TO'

    transfer_id = temp_mesh_upload_jobs.create_mesh_upload_job(
        workflow_id=workflow_id,
        s3_locations=s3_locations,
        mesh_metadata=mesh_metadata,
        mailbox_from=mailbox_from,
        mailbox_to=mailbox_to
    )

    job = temp_mesh_upload_jobs.get_by_transfer_id(transfer_id)
    assert job.transfer_id == transfer_id
    assert not job.message_id
    assert job.status == MeshTransferStatus.Pending
    assert job.workflow_id == workflow_id
    assert job.s3_locations == s3_locations
    assert job.mesh_metadata == mesh_metadata
    assert job.mailbox_from == mailbox_from
    assert job.mailbox_to == mailbox_to
    assert job.content_length == 49


def test_get_by_status(temp_mesh_upload_data):
    """ Test we can retrieve rows by their status """
    store, items = temp_mesh_upload_data
    ready_count = 0
    ready_ids = [items[2]['transfer_id'], items[4]['transfer_id'], items[9]['transfer_id'],
                 items[16]['transfer_id'], items[18]['transfer_id'], items[20]['transfer_id'],
                 items[21]['transfer_id']]
    for job in store.get_all_by_status(MeshTransferStatus.Ready):
        ready_count += 1
        assert job.transfer_id in ready_ids
    assert ready_count == 7


def test_get_all_pending(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data
    pending_count = 0
    pending_ids = [items[1]['transfer_id'], items[8]['transfer_id'], items[17]['transfer_id']]
    for job in store.get_all_pending():
        pending_count += 1
        assert job.transfer_id in pending_ids
    assert pending_count == 3


def test_get_all_processing_for_workflow(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data
    processing_count = 0
    processing_ids = [items[10]['transfer_id']]
    for job in store.get_all_processing('WFID_C'):
        processing_count += 1
        assert job.transfer_id in processing_ids
    assert processing_count == 1


def test_get_all_processing_for_workflow_no_entries(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data
    assert list(store.get_all_processing('WFID_A')) == []


def test_mark_preparing(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data

    # check we can't move from the wrong states
    assert not store.mark_preparing(items[2]['transfer_id'])  # ready
    assert not store.mark_preparing(items[5]['transfer_id'])  # failed
    assert not store.mark_preparing(items[6]['transfer_id'])  # success
    assert not store.mark_preparing(items[10]['transfer_id'])  # processing

    # check we can move from the right state
    assert store.mark_preparing(items[1]['transfer_id'])  # pending
    assert store.get_by_transfer_id(items[1]['transfer_id']).status == MeshTransferStatus.Preparing

    # check we can't move from the right state twice
    assert store.mark_failed(items[10]['transfer_id'])
    assert not store.mark_failed(items[10]['transfer_id'])


def test_mark_ready(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data

    # check we can't move from the wrong states
    assert not store.mark_ready(items[1]['transfer_id'])  # pending
    assert not store.mark_ready(items[2]['transfer_id'])  # ready
    assert not store.mark_ready(items[5]['transfer_id'])  # failed
    assert not store.mark_ready(items[6]['transfer_id'])  # success
    assert store.mark_ready(items[10]['transfer_id'])  # processing

    # check we can move from the right state
    assert store.mark_ready(items[3]['transfer_id'])  # preparing
    assert store.get_by_transfer_id(items[3]['transfer_id']).status == MeshTransferStatus.Ready

    # check we can't move from the right state twice
    assert not store.mark_ready(items[3]['transfer_id'])  # ready


def test_mark_processing(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data

    # check we can't move from the wrong states
    assert not store.mark_processing(items[1]['transfer_id'])  # pending
    assert not store.mark_processing(items[3]['transfer_id'])  # preparing
    assert not store.mark_processing(items[5]['transfer_id'])  # failed
    assert not store.mark_processing(items[6]['transfer_id'])  # success
    assert not store.mark_processing(items[10]['transfer_id'])  # processing

    # check we can move from the right state
    assert store.mark_processing(items[2]['transfer_id'])  # ready
    assert store.get_by_transfer_id(items[2]['transfer_id']).status == MeshTransferStatus.Processing

    # check we can't move from the right state twice
    assert not store.mark_processing(items[2]['transfer_id'])  # processing


def test_mark_archived(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data
    assert store.mark_archived(items[1]['transfer_id'])  # pending
    assert store.mark_archived(items[3]['transfer_id'])  # preparing
    assert store.mark_archived(items[5]['transfer_id'])  # failed
    assert store.mark_archived(items[6]['transfer_id'])  # success
    assert store.mark_archived(items[10]['transfer_id'])  # processing


def test_mark_processing_and_set_mailbox_from(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data

    mailbox_from = 'DSP_CORE'

    # check we can move from the right state
    assert store.mark_processing(items[2]['transfer_id'], mailbox_from)  # ready
    job = store.get_by_transfer_id(items[2]['transfer_id'])
    assert job.status == MeshTransferStatus.Processing
    assert job.mailbox_from == mailbox_from

    # check we can't move from the wrong state
    assert not store.mark_processing(items[1]['transfer_id'], mailbox_from)  # pending
    assert not store.get_by_transfer_id(items[1]['transfer_id']).mailbox_from == mailbox_from


def test_mark_completed(temp_mesh_upload_data):
    """ Test we can mark a processing job as completed """
    store, items = temp_mesh_upload_data
    message_id = 'MSG_ID'

    # check we can't move from the wrong states and message id is not set if state is not set
    assert not store.mark_success(items[1]['transfer_id'], message_id)  # pending
    assert not store.get_by_transfer_id(items[1]['transfer_id']).message_id
    assert not store.mark_success(items[2]['transfer_id'], message_id)  # ready
    assert not store.get_by_transfer_id(items[2]['transfer_id']).message_id
    assert not store.mark_success(items[3]['transfer_id'], message_id)  # preparing
    assert not store.get_by_transfer_id(items[3]['transfer_id']).message_id
    assert not store.mark_success(items[5]['transfer_id'], message_id)  # failed
    assert not store.get_by_transfer_id(items[5]['transfer_id']).message_id
    assert not store.mark_success(items[6]['transfer_id'], message_id)  # success
    assert not store.get_by_transfer_id(items[6]['transfer_id']).message_id

    assert store.mark_success(items[10]['transfer_id'], message_id)
    assert store.get_by_transfer_id(items[10]['transfer_id']).status == MeshTransferStatus.Success
    assert store.get_by_transfer_id(items[10]['transfer_id']).message_id == message_id

    assert not store.mark_success(items[10]['transfer_id'], message_id)


def test_mark_success_where_message_id_is_set(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data
    transfer_id = str(uuid4())
    initial_message_id = 'MSG_ID1'
    new_message_id = 'MSG_ID2'
    item = dict(
        transfer_id=transfer_id,
        created=datetime.utcnow(),
        status=MeshTransferStatus.Processing,
        workflow_id='WFID_X',
        mesh_metadata=dict(
            subject='Test subject',
        ),
        s3_locations=['s3://location'],
        mailbox_from='MB_FROM',
        mailbox_to='MB_TO',
        message_id=initial_message_id
    )
    store.table.put_item(
        Item=MeshTransferJob(item).to_primitive()
    )

    store.mark_success(transfer_id, new_message_id)
    assert store.get_by_transfer_id(transfer_id).message_id == initial_message_id


def test_mark_failed(temp_mesh_upload_data):
    """ Test we can mark a processing job as failed """
    store, items = temp_mesh_upload_data

    # check we can't move from the wrong states
    assert not store.mark_failed(items[1]['transfer_id'])  # pending
    assert not store.mark_failed(items[2]['transfer_id'])  # ready
    assert not store.mark_failed(items[3]['transfer_id'])  # preparing
    assert not store.mark_failed(items[5]['transfer_id'])  # failed
    assert not store.mark_failed(items[6]['transfer_id'])  # success

    # check we can move from the right state
    assert store.mark_failed(items[10]['transfer_id'])  # processing
    assert store.get_by_transfer_id(items[10]['transfer_id']).status == MeshTransferStatus.Failed

    # check we can't move from the right state twice
    assert not store.mark_failed(items[10]['transfer_id'])  # failed


def test_get_count_for_status(temp_mesh_upload_data):
    """ Test we can get the counts for the different statuses """
    store, items = temp_mesh_upload_data
    assert store.get_status_count(MeshTransferStatus.Pending) == 3
    assert store.get_status_count(MeshTransferStatus.Preparing) == 3
    assert store.get_status_count(MeshTransferStatus.Ready) == 7
    assert store.get_status_count(MeshTransferStatus.Processing) == 1
    assert store.get_status_count(MeshTransferStatus.Success) == 7
    assert store.get_status_count(MeshTransferStatus.Failed) == 1
    # change the state of one entry and check that the counts reflect this
    assert store.mark_processing(items[18]['transfer_id'])
    assert store.get_status_count(MeshTransferStatus.Pending) == 3
    assert store.get_status_count(MeshTransferStatus.Preparing) == 3
    assert store.get_status_count(MeshTransferStatus.Ready) == 6
    assert store.get_status_count(MeshTransferStatus.Processing) == 2
    assert store.get_status_count(MeshTransferStatus.Success) == 7
    assert store.get_status_count(MeshTransferStatus.Failed) == 1


def test_get_count_for_status_by_workflow_id(temp_mesh_upload_data):
    store, items = temp_mesh_upload_data
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


def test_get_count_for_incorrect_status(temp_mesh_upload_data):
    """ Test we can get a 0 for an incorrect status count """
    store, items = temp_mesh_upload_data
    assert store.get_status_count('not a status') == 0


def test_get_next_ready(temp_mesh_upload_data):
    """ Test we can get next ready """
    store, items = temp_mesh_upload_data
    next_ready = store.get_next_ready()
    assert next_ready.transfer_id == items[18]['transfer_id']


def test_get_next_ready_with_sender_superblock(temp_mesh_upload_data, temp_blocks):
    store, items = temp_mesh_upload_data
    assert store.get_next_ready().transfer_id == items[18]['transfer_id']

    temp_blocks.add_block(job_type=Blockables.SENDERS, dataset_id=Block.ALL, block_group=Block.ALL,
                          block_type=BLOCKTYPE.SUPER)

    next_ready = store.get_next_ready()
    assert next_ready is None


def test_get_next_ready_with_all_mesh_blocked(temp_mesh_upload_data, temp_blocks):
    store, items = temp_mesh_upload_data
    assert store.get_next_ready().transfer_id == items[18]['transfer_id']

    temp_blocks.add_block(job_type=Blockables.SENDERS, dataset_id=Block.ALL, block_group=Senders.MESH_ALL,
                          block_type=BLOCKTYPE.GROUP)

    next_ready = store.get_next_ready()
    assert next_ready is None


def test_get_next_ready_with_mesh_workflow_id_blocked(temp_mesh_upload_data, temp_blocks):
    store, items = temp_mesh_upload_data
    assert store.get_next_ready().transfer_id == items[18]['transfer_id']

    temp_blocks.add_block(job_type=Blockables.SENDERS, dataset_id=Block.ALL,
                          block_group=Block.create_composite_block_group(Senders.MESH_WORKFLOW, 'WFID_E'),
                          block_type=BLOCKTYPE.GROUP)

    next_ready = store.get_next_ready()

    assert next_ready.transfer_id == items[2]['transfer_id']


def test_get_next_ready_with_mesh_mailbox_to_blocked(temp_mesh_upload_data, temp_blocks):
    store, items = temp_mesh_upload_data
    assert store.get_next_ready().transfer_id == items[18]['transfer_id']

    temp_blocks.add_block(job_type=Blockables.SENDERS, dataset_id=Block.ALL,
                          block_group=Block.create_composite_block_group(Senders.MESH_MAILBOX_TO, 'MB_TO'),
                          block_type=BLOCKTYPE.GROUP)

    next_ready = store.get_next_ready()
    assert next_ready.transfer_id == items[20]['transfer_id']


def test_get_next_ready_with_mesh_mailbox_from_blocked(temp_mesh_upload_data, temp_blocks):
    store, items = temp_mesh_upload_data
    assert store.get_next_ready().transfer_id == items[18]['transfer_id']

    temp_blocks.add_block(job_type=Blockables.SENDERS, dataset_id=Block.ALL,
                          block_group=Block.create_composite_block_group(Senders.MESH_MAILBOX_FROM, 'MB_FROM'),
                          block_type=BLOCKTYPE.GROUP)

    next_ready = store.get_next_ready()
    assert next_ready.transfer_id == items[21]['transfer_id']


def test_get_next_ready_with_priority(temp_mesh_upload_data_with_priority):
    """ Test that the highest priority with the oldest time is selected """
    store, items = temp_mesh_upload_data_with_priority
    next_ready = store.get_next_ready()
    assert next_ready.transfer_id == items[3]['transfer_id']


@pytest.mark.parametrize(
    ["file_sizes", "sender_min_content_length", "sender_max_content_length", "expected"], [
        ([13], None, None, True),  # single file default range
        ([13, 29], None, None, True),  # two files default range
        ([1, 2, 3], None, None, True),  # within default range
        ([1, 2, 3], 0, 10, True),  # within specified range
        ([1, 2, 3], 6, 6, True),  # precise range
        ([1, 2, 3], 0, 5, False),  # too large
        ([1, 2, 3], 7, 10, False),  # too small
    ])
def test_get_next_ready_with_content_lengths(
        temp_mesh_upload_jobs: MeshUploadJobsStore, file_sizes: List[int],
        sender_min_content_length: int, sender_max_content_length: int,
        expected: True
):
    s3_locations = [f's3://local-testing/file{i}.txt' for i in range(len(file_sizes))]
    for s3_loc, file_size in zip(s3_locations, file_sizes):
        with smart_open(uri=s3_loc, mode='w') as f:
            f.write("*" * file_size)

    transfer_id = temp_mesh_upload_jobs.create_mesh_upload_job(
        'WFID', s3_locations, MeshMetadata(), 'MAILBOX_TO', status=MeshTransferStatus.Ready
    )

    orig_job = temp_mesh_upload_jobs.get_by_transfer_id(transfer_id)
    assert orig_job.content_length == sum(file_sizes)

    if sender_min_content_length is not None and sender_max_content_length is not None:
        job = temp_mesh_upload_jobs.get_next_ready(sender_min_content_length, sender_max_content_length)
    elif sender_min_content_length is not None:
        job = temp_mesh_upload_jobs.get_next_ready(min_content_length=sender_min_content_length)
    elif sender_max_content_length is not None:
        job = temp_mesh_upload_jobs.get_next_ready(max_content_length=sender_max_content_length)
    else:
        job = temp_mesh_upload_jobs.get_next_ready()

    if expected:
        assert job
        assert job.transfer_id == orig_job.transfer_id
    else:
        assert not job


def test_uplift_v1_to_latest(temp_mesh_upload_jobs: MeshUploadJobsStore):
    store = temp_mesh_upload_jobs
    transfer_id = str(uuid4())
    created = str(datetime.utcnow())

    item = dict(
        version=1,
        transfer_id=transfer_id,
        created=created,
        status=MeshTransferStatus.Ready,
        workflow_id='WFID_A',
        mesh_metadata=dict(
            subject='Test subject',
        ),
        s3_locations=['s3_a2_1', 's3_a2_2'],
        mailbox_from='MB_FROM',
        mailbox_to='MB_TO'
    )
    store.table.put_item(
        Item=item
    )
    uplifted_item = store.get_by_transfer_id(transfer_id)
    assert uplifted_item.priority == MeshTransferJobPriority.Normal


def test_get_all_by_workflow_status(temp_mesh_upload_data):
    """ Test we can get next ready """
    store, items = temp_mesh_upload_data

    found = [rec for rec in store.get_all_by_workflow_id_status('WFID_B')]
    expected = sum(1 for rec in items if rec['workflow_id'] == 'WFID_B' and rec['status'] != 'archived')
    assert expected > 0
    assert len(found) == expected, (found, items)

    found = [rec for rec in store.get_all_by_workflow_id_status('WFID_B', ['failed'])]
    expected = sum(1 for rec in items if rec['workflow_id'] == 'WFID_B' and rec['status'] == 'failed')
    assert expected > 0
    assert len(found) == expected, (found, items)

    found = [rec for rec in store.get_all_by_workflow_id_status('WFID_B', 'failed')]
    expected = sum(1 for rec in items if rec['workflow_id'] == 'WFID_B' and rec['status'] == 'failed')
    assert expected > 0
    assert len(found) == expected, (found, items)
