import os
from typing import Any, MutableMapping, NewType

import schematics
from schematics import types


DEFAULT_SAVE_MODE = 'error'


Metadata = NewType('Metadata', MutableMapping[str, Any])


class ValidationResult(schematics.Model):
    """
    An entity making a Submission.

    Attributes:
        total_records (int): total records parsed during loading phase
        dq_error_count (int): total number of rows that had any DQ errors
        dq_warning_count (int): total number of rows that had any DQ warnings
    """
    def __init__(self, **kwargs):
        super(ValidationResult, self).__init__(
            raw_data=kwargs, validate=True
        )

    total_records = types.IntType(required=True, min_value=0, default=0)  # type: int
    dq_error_count = types.IntType(required=True, min_value=0, default=0)  # type: int
    dq_warning_count = types.IntType(required=True, min_value=0, default=0)  # type: int

    @property
    def dq_pass_count(self) -> int:
        return max(self.total_records - self.dq_error_count, 0)

    def to_primitive(self, role=None, app_data=None, **kwargs):
        return dict(
            dq_pass_count=self.dq_pass_count,
            **super(ValidationResult, self).to_primitive(role=role, app_data=app_data, **kwargs)
        )


def get_ingestion_output_location(metadata: Metadata) -> str:
    working_folder = metadata['working_folder']

    result_scope = metadata.get('result_scope')

    return os.path.join(working_folder, result_scope) if result_scope else working_folder
