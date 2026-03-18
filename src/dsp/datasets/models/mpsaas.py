from schematics import types

from dsp.shared.models import StrictModel
from dsp.datasets.validations.mpsaas import (
    request_reference_validator,
    workflow_id_validator,
    number_of_records_validator
)


class MPSRequestHeaderRecord(StrictModel):
    request_reference = types.StringType(validators=[request_reference_validator])
    workflow_id = types.StringType(validators=[workflow_id_validator])
    request_timestamp = types.DateTimeType(formats=['%Y%m%d%H%M%S'])
    no_of_data_records = types.IntType(validators=[number_of_records_validator])
