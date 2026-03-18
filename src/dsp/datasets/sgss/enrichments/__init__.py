from typing import Dict, Callable

from pyspark.sql import DataFrame

from dsp.shared.constants import DS
from .mps import mps_request


def get_dataset_mps_requests() -> Dict[str, Callable[[DataFrame], DataFrame]]:
    return {DS.SGSS: mps_request, DS.SGSS_DELTA: mps_request}
