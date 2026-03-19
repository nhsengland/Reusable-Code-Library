from typing import List, MutableMapping, Any

from dsp.shared.extracts.base import BaseSubmissionFolderExtract
from dsp.shared.models import ExtractType


class ValidationSummaryExtract(BaseSubmissionFolderExtract):

    def can_request(self, request_parameters: MutableMapping[str, Any]) -> List[str]:
        return ['request extract not supported for {}'.format(self.extract_type)]

    extract_type = ExtractType.ValidationSummary
    dataset_id = 'common'







