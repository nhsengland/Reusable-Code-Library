from typing import Any, Dict, List, Tuple, Optional, Iterable

import boto3
import pytest

from dsp.shared.models import MpsManifestStatus, MpsManifest
from dsp.shared.store.mps_manifests import MpsManifestsStore


TempMpsManifestsData = Tuple[MpsManifestsStore, List[Dict[str, Any]]]


@pytest.fixture(scope='function')
def temp_mps_manifests(temp_mps_manifests_table: boto3.session.Session.resource) -> MpsManifestsStore:
    """ Create a store with the test DynamoDB table """
    return MpsManifestsStore(temp_mps_manifests_table)


@pytest.fixture(scope='function')
def temp_mps_manifests_data(temp_mps_manifests: MpsManifestsStore) -> TempMpsManifestsData:
    store = temp_mps_manifests

    items = list()

    def add_item(
            submission_id: str,
            all_chunks: List[str],
            remaining_chunks: List[str],
            status: str,
    ) -> None:
        item = dict(
            submission_id=submission_id,
            all_chunks=all_chunks,
            remaining_chunks=remaining_chunks,
            status=status,
        )
        store.table.put_item(Item=MpsManifest(item).to_primitive())
        items.append(item)

    add_item(
        '00001',
        ['e77e9aa5-4d43-4a44-a67e-d9aaf6f9b3c6', '4d0be362-9856-44c4-af38-eee72994cb9a'],
        ['4d0be362-9856-44c4-af38-eee72994cb9a'],
        MpsManifestStatus.Pending,
    )
    add_item(
        '00002',
        ['4b88ff7b-b4b0-4e82-82b4-b404163835cc', '67c0ccb3-2744-45e5-8131-06fe01397e62'],
        ['67c0ccb3-2744-45e5-8131-06fe01397e62'],
        MpsManifestStatus.Pending,
    )
    add_item(
        '00003',
        ['f6977a25-d996-450c-929b-622498604a90', 'acc13c7a-d856-49fb-ba4a-9bd6d1a3966b'],
        ['acc13c7a-d856-49fb-ba4a-9bd6d1a3966b'],
        MpsManifestStatus.Pending,
    )
    add_item(
        '00004',
        ['e22558c7-2f5c-44ad-a692-d2eb8d7fda16', 'c8942667-4e20-4118-bda4-f800bc190d9e'],
        ['c8942667-4e20-4118-bda4-f800bc190d9e'],
        MpsManifestStatus.Pending,
    )
    add_item(
        '00005',
        ['ce4dfcca-fa03-443e-aa46-2190b20d801f', '971fe986-edb2-4ec7-9408-94e8f9c050da'],
        ['971fe986-edb2-4ec7-9408-94e8f9c050da'],
        MpsManifestStatus.Success,
    )
    add_item(
        '00006',
        ['c4173c54-e798-4e7f-a2d0-f1a80b142562', 'f939ff92-3a4a-4540-9976-88d745e2db4b'],
        ['f939ff92-3a4a-4540-9976-88d745e2db4b'],
        MpsManifestStatus.Success,
    )

    return store, items


def test_create_mps_manifest(temp_mps_manifests: MpsManifestsStore):
    temp_mps_manifests.create_mps_manifest(
        submission_id='00001',
        request_chunks=[
            '74a67c90-dbbd-4b34-bd6f-7d7b66b85b7c',
            'f22cfa01-cb8a-4720-abae-76e8ac69c51d'
        ],
        response_chunks=['f28a3b2a-c07a-46b9-87c8-587ff91ea418']
    )

    mps_manifest = temp_mps_manifests.get_by_submission_id('00001')

    assert mps_manifest.submission_id == '00001'
    assert mps_manifest.all_chunks == ['74a67c90-dbbd-4b34-bd6f-7d7b66b85b7c',
                                       'f22cfa01-cb8a-4720-abae-76e8ac69c51d',
                                       'f28a3b2a-c07a-46b9-87c8-587ff91ea418']
    assert mps_manifest.remaining_chunks == ['74a67c90-dbbd-4b34-bd6f-7d7b66b85b7c',
                                             'f22cfa01-cb8a-4720-abae-76e8ac69c51d']
    assert mps_manifest.status == MpsManifestStatus.Pending


def test_create_mps_manifest_without_response_chunk(temp_mps_manifests: MpsManifestsStore):
    temp_mps_manifests.create_mps_manifest(submission_id='00001',
                                           request_chunks=['74a67c90-dbbd-4b34-bd6f-7d7b66b85b7c',
                                                           'f22cfa01-cb8a-4720-abae-76e8ac69c51d'],
                                           response_chunks=[],
                                           )
    mps_manifest = temp_mps_manifests.get_by_submission_id('00001')

    assert mps_manifest.submission_id == '00001'
    assert mps_manifest.all_chunks == ['74a67c90-dbbd-4b34-bd6f-7d7b66b85b7c',
                                       'f22cfa01-cb8a-4720-abae-76e8ac69c51d']
    assert mps_manifest.remaining_chunks == ['74a67c90-dbbd-4b34-bd6f-7d7b66b85b7c',
                                             'f22cfa01-cb8a-4720-abae-76e8ac69c51d']
    assert mps_manifest.status == MpsManifestStatus.Pending


def test_create_mps_manifest_without_request_chunk_raises_value_error(temp_mps_manifests: MpsManifestsStore):
    with pytest.raises(ValueError):
        temp_mps_manifests.create_mps_manifest(submission_id='00001',
                                               request_chunks=[],
                                               response_chunks=['f28a3b2a-c07a-46b9-87c8-587ff91ea418'],
                                               )


def test_get_all_by_status(temp_mps_manifests_data: TempMpsManifestsData):
    store, items = temp_mps_manifests_data
    pending_submission_ids = [items[0]['submission_id'], items[1]['submission_id'], items[2]['submission_id'],
                              items[3]['submission_id']]
    pending_mps_manifests = store.get_all_by_status(MpsManifestStatus.Pending)

    pending_count = 0
    for manifest in pending_mps_manifests:
        assert manifest.submission_id in pending_submission_ids
        pending_count += 1
    assert pending_count == len(pending_submission_ids)


def test_get_all_pending(temp_mps_manifests_data: TempMpsManifestsData):
    store, items = temp_mps_manifests_data
    pending_submission_ids = [items[0]['submission_id'], items[1]['submission_id'], items[2]['submission_id'],
                              items[3]['submission_id']]
    pending_mps_manifests = store.get_all_by_status(MpsManifestStatus.Pending)

    pending_count = 0
    for manifest in pending_mps_manifests:
        assert manifest.submission_id in pending_submission_ids
        pending_count += 1
    assert pending_count == len(pending_submission_ids)


def test_get_all_success(temp_mps_manifests_data: TempMpsManifestsData):
    store, items = temp_mps_manifests_data
    success_submission_ids = [items[4]['submission_id'], items[5]['submission_id']]
    success_pds_manifests = store.get_all_success()

    success_count = 0
    for manifest in success_pds_manifests:
        assert manifest.submission_id in success_submission_ids
        success_count += 1
    assert success_count == len(success_submission_ids)


def test_mark_success(temp_mps_manifests_data: TempMpsManifestsData):
    store, items = temp_mps_manifests_data
    submission_id = items[0]['submission_id']
    mps_manifest = store.get_by_submission_id(submission_id)
    assert mps_manifest.status == MpsManifestStatus.Pending
    store.mark_success(submission_id)
    mps_manifest = store.get_by_submission_id(submission_id)
    assert mps_manifest.status == MpsManifestStatus.Success


def test_cannot_mark_success_incorrectly(temp_mps_manifests_data: TempMpsManifestsData):
    store, items = temp_mps_manifests_data
    submission_id = items[4]['submission_id']
    mps_manifest = store.get_by_submission_id(submission_id)
    assert mps_manifest.status == MpsManifestStatus.Success
    assert not store.mark_success(submission_id)


def test_remove_from_remaining_chunks(temp_mps_manifests_data: TempMpsManifestsData):
    store, items = temp_mps_manifests_data
    submission_id = items[0]['submission_id']
    mps_manifest = store.get_by_submission_id(submission_id)
    chunk_to_remove = mps_manifest.remaining_chunks[0]
    store.remove_from_remaining_chunks(submission_id, chunk_to_remove)
    mps_manifest = store.get_by_submission_id(submission_id)
    assert chunk_to_remove not in mps_manifest.remaining_chunks


def test_remove_non_present_chunk_from_remaining_chunks_returns_false(temp_mps_manifests_data: TempMpsManifestsData):
    store, items = temp_mps_manifests_data
    submission_id = items[0]['submission_id']
    non_present_chunk = 'c8de9667-8da9-4837-b0d0-b7162e5bf20c'
    assert not store.remove_from_remaining_chunks(submission_id, non_present_chunk)
