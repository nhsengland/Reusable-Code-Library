from typing import List, MutableMapping, Any

import re

from dsp.shared.extracts.base import BaseExtract
from dsp.shared.logger import log_action
from dsp.shared.models import ExtractRequest, ExtractType
from dsp.shared.extracts.extract_constants import ExtractDetailsFields, ExtractValidationTypes

class DidsNhseExtract(BaseExtract):
    extract_type = ExtractType.DidsNhseExtract
    allow_request = False

    @log_action()
    def can_request(self, request_parameters: MutableMapping[str, Any]) -> List[str]:
        return ['request extract not supported for {}'.format(self.extract_type)]

    @log_action()
    def can_execute(self, extract_request: ExtractRequest) -> List[str]:
        issues = []
        year = extract_request.request.get(ExtractDetailsFields.YEAR)
        month = extract_request.request.get(ExtractDetailsFields.MONTH)
        if not year:
            issues.append(ExtractValidationTypes.YEAR_EXPECTED)
        if not month:
            issues.append(ExtractValidationTypes.MONTH_EXPECTED)

        return issues

class DidsNcrasDailyExtract(BaseExtract):
    extract_type = ExtractType.DidsNcrasDailyExtract
    allow_request = False

    @log_action()
    def can_request(self, request_parameters: MutableMapping[str, Any]) -> List[str]:
        return ['request extract not supported for {}'.format(self.extract_type)]

    @log_action()
    def can_execute(self, extract_request: ExtractRequest) -> List[str]:
        issues = []        
        return issues

class DidsNcrasMonthlyExtract(BaseExtract):
    extract_type = ExtractType.DidsNcrasMonthlyExtract
    allow_request = False

    @log_action()
    def can_request(self, request_parameters: MutableMapping[str, Any]) -> List[str]:
        return ['request extract not supported for {}'.format(self.extract_type)]

    @log_action()
    def can_execute(self, extract_request: ExtractRequest) -> List[str]:
        issues = []
        return issues
