import os
from abc import abstractmethod, ABC
from typing import MutableMapping, Any, List

from dsp.shared.common import format_submission_id
from dsp.shared.config import config_item
from dsp.shared.constants import FT
from dsp.shared.extracts.extract_constants import ExtractDetailsFields, ExtractValidationTypes
from dsp.shared.logger import log_action, add_fields
from dsp.shared.models import ExtractRequest


class BaseExtract:
    extract_type = None  # type :str
    dataset_id = None  # type :str

    @abstractmethod
    def can_request(self, request_parameters: MutableMapping[str, Any]) -> List[str]:
        return ['request extract not supported for {}'.format(self.extract_type)]

    def prepare_extract_parameters(self, request_parameters: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        return request_parameters

    @abstractmethod
    def can_execute(self, extract_request: ExtractRequest) -> List[str]:
        pass


class BaseSubmissionFolderExtract(ABC, BaseExtract):

    def prepare_extract_parameters(self, request_parameters: MutableMapping[str, Any]) -> MutableMapping[str, Any]:

        raw_bucket = config_item('raw_bucket')

        request_parameters['submission_working_folder'] = os.path.join(
            's3://', raw_bucket, 'submissions', format_submission_id(request_parameters['submission_id'])
        )

        return request_parameters

    @log_action()
    def can_execute(self, extract_request: ExtractRequest) -> List[str]:

        issues = []

        if not extract_request.request.get(ExtractDetailsFields.SUBMISSION_ID):
            issues.append(ExtractValidationTypes.SUBMISSION_ID_UNDEFINED)

        if not extract_request.request.get(ExtractDetailsFields.SUBMISSION_WORKING_FOLDER):
            issues.append(ExtractValidationTypes.SUBMISSION_WORKING_FOLDER_EXPECTED)

        if issues:
            add_fields(issues=issues)

        return issues


class BaseSummaryExtract(BaseSubmissionFolderExtract):

    def __init__(self, extract_type: str, dataset_id: str, allow_request: bool):
        self.extract_type = extract_type
        self.dataset_id = dataset_id
        self.allow_request = allow_request

    @log_action()
    def can_request(self, request_parameters: MutableMapping[str, Any]) -> List[str]:
        issues = []

        if not request_parameters.get(ExtractDetailsFields.SUBMISSION_ID):
            issues.append(ExtractValidationTypes.SUBMISSION_ID_UNDEFINED)

        dq_file_format = request_parameters.get(ExtractDetailsFields.DQ_FILE_FORMAT)
        if dq_file_format and dq_file_format not in [FT.JSON, FT.PARQUET]:
            issues.append(ExtractValidationTypes.DQ_FILE_UNEXPECTED_FORMAT)

        if issues:
            add_fields(issues=issues)

        return issues
