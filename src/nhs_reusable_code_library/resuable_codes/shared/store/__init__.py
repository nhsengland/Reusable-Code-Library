from typing import Dict

from src.nhs_reusable_code_library.resuable_codes.shared.store.agents import AgentsStore, Agents
from src.nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore
from src.nhs_reusable_code_library.resuable_codes.shared.store.blocks import BlockStore, Blocks
from src.nhs_reusable_code_library.resuable_codes.shared.store.dars_message_jobs import DarsMessageJobsStore, DarsMessageJobs
from src.nhs_reusable_code_library.resuable_codes.shared.store.deid_jobs import DeIdJobStore, DeIdJobs
from src.nhs_reusable_code_library.resuable_codes.shared.store.delta_merge import DeltaMergeStore, DeltaMerges
from src.nhs_reusable_code_library.resuable_codes.shared.store.extract_requests import ExtractRequestsStore, ExtractRequests
from src.nhs_reusable_code_library.resuable_codes.shared.store.last_submissions import LastSubmissionsStore, LastSubmissions
from src.nhs_reusable_code_library.resuable_codes.shared.store.last_sequence_numbers import LastSequenceNumbersStore, LastSequenceNumbers
from src.nhs_reusable_code_library.resuable_codes.shared.store.merge_queues import MergeQueueStore, MergeQueues
from src.nhs_reusable_code_library.resuable_codes.shared.store.mesh_download_queues import MeshDownloadQueuesStore, MeshDownloadQueues
from src.nhs_reusable_code_library.resuable_codes.shared.store.mesh_upload_jobs import MeshUploadJobsStore, MeshUploadJobs
from src.nhs_reusable_code_library.resuable_codes.shared.store.submission_hashes import SubmissionHashesStore, SubmissionHashes
from src.nhs_reusable_code_library.resuable_codes.shared.store.submissions import SubmissionsStore, Submissions
from src.nhs_reusable_code_library.resuable_codes.shared.store.s3_submissions import S3SubmissionsStore, S3Submissions
from src.nhs_reusable_code_library.resuable_codes.shared.store.tasks import TasksStore, Tasks
from src.nhs_reusable_code_library.resuable_codes.shared.store.generic_pipelines_config import GenericPipelinesConfigStore, GenericPipelinesConfig
from src.nhs_reusable_code_library.resuable_codes.shared.store.unsuspend_jobs import UnsuspendJobStore, UnsuspendJobs

class Stores:

    def __init__(self, stores: Dict[str, BaseStore]):
        self._stores = stores

    class Names:
        agents = AgentsStore.table_name
        blocks = BlockStore.table_name
        delta_merges = DeltaMergeStore.table_name
        deid_jobs = DeIdJobStore.table_name
        extract_requests = ExtractRequestsStore.table_name
        last_submissions = LastSubmissionsStore.table_name
        last_sequence_numbers = LastSequenceNumbersStore.table_name
        merge_queues = MergeQueueStore.table_name
        mesh_download_queues = MeshDownloadQueuesStore.table_name
        mesh_upload_jobs = MeshUploadJobsStore.table_name
        submissions = SubmissionsStore.table_name
        submission_hashes = SubmissionHashesStore.table_name
        tasks = TasksStore.table_name
        s3_submissions = S3SubmissionsStore.table_name
        generic_pipelines_config = GenericPipelinesConfigStore.table_name
        unsuspend_jobs = UnsuspendJobStore.table_name
        dars_message_jobs = DarsMessageJobsStore.table_name

    @property
    def agents(self) -> AgentsStore:
        store = self._stores[self.Names.agents]
        assert isinstance(store, AgentsStore)
        return store

    @property
    def blocks(self) -> BlockStore:
        store = self._stores[self.Names.blocks]
        assert isinstance(store, BlockStore)
        return store

    @property
    def delta_merges(self) -> DeltaMergeStore:
        store = self._stores[self.Names.delta_merges]
        assert isinstance(store, DeltaMergeStore)
        return store

    @property
    def deid_jobs(self) -> DeIdJobStore:
        store = self._stores[self.Names.deid_jobs]
        assert isinstance(store, DeIdJobStore)
        return store

    @property
    def extract_requests(self) -> ExtractRequestsStore:
        store = self._stores[self.Names.extract_requests]
        assert isinstance(store, ExtractRequestsStore)
        return store

    @property
    def last_submissions(self) -> LastSubmissionsStore:
        store = self._stores[self.Names.last_submissions]
        assert isinstance(store, LastSubmissionsStore)
        return store

    @property
    def last_sequence_numbers(self) -> LastSequenceNumbersStore:
        store = self._stores[self.Names.last_sequence_numbers]
        assert isinstance(store, LastSequenceNumbersStore)
        return store

    @property
    def mesh_download_queues(self) -> MeshDownloadQueuesStore:
        store = self._stores[self.Names.mesh_download_queues]
        assert isinstance(store, MeshDownloadQueuesStore)
        return store

    @property
    def mesh_upload_jobs(self) -> MeshUploadJobsStore:
        store = self._stores[self.Names.mesh_upload_jobs]
        assert isinstance(store, MeshUploadJobsStore)
        return store

    @property
    def merge_queues(self) -> MergeQueueStore:
        store = self._stores[self.Names.merge_queues]
        assert isinstance(store, MergeQueueStore)
        return store

    @property
    def submissions(self) -> SubmissionsStore:
        store = self._stores[self.Names.submissions]
        assert isinstance(store, SubmissionsStore)
        return store

    @property
    def submission_hashes(self) -> SubmissionHashesStore:
        store = self._stores[self.Names.submission_hashes]
        assert isinstance(store, SubmissionHashesStore)
        return store

    @property
    def tasks(self) -> TasksStore:
        store = self._stores[self.Names.tasks]
        assert isinstance(store, TasksStore)
        return store

    @property
    def s3submissions(self) -> S3SubmissionsStore:
        store = self._stores[self.Names.s3_submissions]
        assert isinstance(store, S3SubmissionsStore)
        return store

    @property
    def generic_pipelines_config(self) -> GenericPipelinesConfigStore:
        store = self._stores[self.Names.generic_pipelines_config]
        assert isinstance(store, GenericPipelinesConfigStore)
        return store

    @property
    def unsuspend_jobs(self) -> UnsuspendJobStore:
        store = self._stores[self.Names.unsuspend_jobs]
        assert isinstance(store, UnsuspendJobStore)
        return store

    @property
    def dars_message_jobs(self) -> DarsMessageJobsStore:
        store = self._stores[self.Names.dars_message_jobs]
        assert isinstance(store, DarsMessageJobsStore)
        return store

    @classmethod
    def all(cls):
        return Stores({
            Stores.Names.agents: Agents,
            Stores.Names.blocks: Blocks,
            Stores.Names.delta_merges: DeltaMerges,
            Stores.Names.deid_jobs: DeIdJobs,
            Stores.Names.extract_requests: ExtractRequests,
            Stores.Names.last_submissions: LastSubmissions,
            Stores.Names.last_sequence_numbers: LastSequenceNumbers,
            Stores.Names.mesh_download_queues: MeshDownloadQueues,
            Stores.Names.mesh_upload_jobs: MeshUploadJobs,
            Stores.Names.merge_queues: MergeQueues,
            Stores.Names.submissions: Submissions,
            Stores.Names.submission_hashes: SubmissionHashes,
            Stores.Names.tasks: Tasks,
            Stores.Names.s3_submissions: S3Submissions,
            Stores.Names.generic_pipelines_config: GenericPipelinesConfig,
            Stores.Names.unsuspend_jobs: UnsuspendJobs,
            Stores.Names.dars_message_jobs: DarsMessageJobs
        })
