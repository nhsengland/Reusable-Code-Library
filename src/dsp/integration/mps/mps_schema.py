from collections import OrderedDict
from datetime import datetime
from typing import Dict, Any

from pyspark.sql.types import StructField, StructType, BooleanType, IntegerType, StringType, DecimalType

from dsp.datasets.mps.request import Fields as MPSRequest, Order as MPSRequestOrder
from dsp.datasets.mps.request_header import Fields as mps_request_header
from dsp.datasets.mps.response import Fields as MPSResponse, Order as MPSResponseOrder
from dsp.datasets.mps.response_header import Fields as mps_response_header
from dsp.integration.mps.constants import SpineResponseCodes
from dsp.shared.constants import MESH_WORKFLOW_ID

mps_request_schema_attributes = MPSRequestOrder
mps_response_schema_attributes = MPSResponseOrder

# @TODO: this is a schema of what? request or response?
#        (should use dedicated schemas for req and resp as the docs imply they're different)
mps_schema = StructType(
    [
        StructField("MATCH", BooleanType(), False),
        StructField(MPSRequest.UNIQUE_REFERENCE, StringType(), True),
        StructField(MPSRequest.NHS_NO, StringType(), True),  # only in Request
        StructField(MPSRequest.FAMILY_NAME, StringType(), True),
        StructField(MPSRequest.GIVEN_NAME, StringType(), True),
        StructField(MPSRequest.OTHER_GIVEN_NAME, StringType(), True),
        StructField(MPSRequest.GENDER, StringType(), True),
        StructField(MPSRequest.DATE_OF_BIRTH, IntegerType(), True),
        StructField(MPSRequest.POSTCODE, StringType(), True),
        StructField(MPSRequest.DATE_OF_DEATH, IntegerType(), True),
        StructField(MPSRequest.ADDRESS_LINE1, StringType(), True),
        StructField(MPSRequest.ADDRESS_LINE2, StringType(), True),
        StructField(MPSRequest.ADDRESS_LINE3, StringType(), True),
        StructField(MPSRequest.ADDRESS_LINE4, StringType(), True),
        StructField(MPSRequest.ADDRESS_LINE5, StringType(), True),
        StructField(MPSRequest.ADDRESS_DATE, IntegerType(), True),
        StructField(MPSRequest.GP_PRACTICE_CODE, StringType(), True),
        StructField(MPSRequest.NHAIS_POSTING_ID, StringType(), True),
        StructField(MPSRequest.AS_AT_DATE, IntegerType(), True),
        StructField(MPSRequest.LOCAL_PATIENT_ID, StringType(), True),
        StructField(MPSRequest.INTERNAL_ID, StringType(), True),
        StructField(MPSRequest.TELEPHONE_NUMBER, StringType(), True),
        StructField(MPSRequest.MOBILE_NUMBER, StringType(), True),
        StructField(MPSRequest.EMAIL_ADDRESS, StringType(), True),
        StructField(MPSResponse.REQ_NHS_NUMBER, StringType(), True),
        StructField(MPSResponse.SENSITIVTY_FLAG, StringType(), True),
        StructField(MPSResponse.MPS_ID, StringType(), True),
        StructField(MPSResponse.ERROR_SUCCESS_CODE, StringType(), True),
        StructField(MPSResponse.MATCHED_NHS_NO, StringType(), True),
        StructField(MPSResponse.MATCHED_ALGORITHM_INDICATOR, IntegerType(), True),
        StructField(MPSResponse.MATCHED_CONFIDENCE_PERCENTAGE, DecimalType(5, 2), True),
        StructField(MPSResponse.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC, DecimalType(5, 2), True),
        StructField(MPSResponse.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC, DecimalType(5, 2), True),
        StructField(MPSResponse.ALGORITHMIC_TRACE_DOB_SCORE_PERC, DecimalType(5, 2), True),
        StructField(MPSResponse.ALGORITHMIC_TRACE_GENDER_SCORE_PERC, DecimalType(5, 2), True),
        StructField(MPSResponse.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC, DecimalType(5, 2), True)
    ]
)

mps_request_schema = StructType(
    [
        StructField(MPSRequest.UNIQUE_REFERENCE, StringType(), True),
        StructField(MPSRequest.NHS_NO, StringType(), True),
        StructField(MPSRequest.FAMILY_NAME, StringType(), True),
        StructField(MPSRequest.GIVEN_NAME, StringType(), True),
        StructField(MPSRequest.OTHER_GIVEN_NAME, StringType(), True),
        StructField(MPSRequest.GENDER, StringType(), True),
        StructField(MPSRequest.DATE_OF_BIRTH, IntegerType(), True),
        StructField(MPSRequest.POSTCODE, StringType(), True),
        StructField(MPSRequest.DATE_OF_DEATH, IntegerType(), True),
        StructField(MPSRequest.ADDRESS_LINE1, StringType(), True),
        StructField(MPSRequest.ADDRESS_LINE2, StringType(), True),
        StructField(MPSRequest.ADDRESS_LINE3, StringType(), True),
        StructField(MPSRequest.ADDRESS_LINE4, StringType(), True),
        StructField(MPSRequest.ADDRESS_LINE5, StringType(), True),
        StructField(MPSRequest.ADDRESS_DATE, IntegerType(), True),
        StructField(MPSRequest.GP_PRACTICE_CODE, StringType(), True),
        StructField(MPSRequest.NHAIS_POSTING_ID, StringType(), True),
        StructField(MPSRequest.AS_AT_DATE, IntegerType(), True),
        StructField(MPSRequest.LOCAL_PATIENT_ID, StringType(), True),
        StructField(MPSRequest.INTERNAL_ID, StringType(), True),
        StructField(MPSRequest.TELEPHONE_NUMBER, StringType(), True),
        StructField(MPSRequest.MOBILE_NUMBER, StringType(), True),
        StructField(MPSRequest.EMAIL_ADDRESS, StringType(), True),
    ]
)

mps_response_schema = StructType([
    StructField(MPSResponse.UNIQUE_REFERENCE, StringType()),
    StructField(MPSResponse.REQ_NHS_NUMBER, StringType(), True),
    StructField(MPSResponse.FAMILY_NAME, StringType(), True),
    StructField(MPSResponse.GIVEN_NAME, StringType(), True),
    StructField(MPSResponse.OTHER_GIVEN_NAME, StringType(), True),
    StructField(MPSResponse.GENDER, StringType(), True),
    StructField(MPSResponse.DATE_OF_BIRTH, IntegerType(), True),
    StructField(MPSResponse.DATE_OF_DEATH, IntegerType(), True),
    StructField(MPSResponse.ADDRESS_LINE1, StringType(), True),
    StructField(MPSResponse.ADDRESS_LINE2, StringType(), True),
    StructField(MPSResponse.ADDRESS_LINE3, StringType(), True),
    StructField(MPSResponse.ADDRESS_LINE4, StringType(), True),
    StructField(MPSResponse.ADDRESS_LINE5, StringType(), True),
    StructField(MPSResponse.POSTCODE, StringType(), True),
    StructField(MPSResponse.GP_PRACTICE_CODE, StringType(), True),
    StructField(MPSResponse.NHAIS_POSTING_ID, StringType(), True),
    StructField(MPSResponse.AS_AT_DATE, IntegerType(), True),
    StructField(MPSResponse.LOCAL_PATIENT_ID, StringType(), True),
    StructField(MPSResponse.INTERNAL_ID, StringType(), True),
    StructField(MPSResponse.TELEPHONE_NUMBER, StringType(), True),
    StructField(MPSResponse.MOBILE_NUMBER, StringType(), True),
    StructField(MPSResponse.EMAIL_ADDRESS, StringType(), True),
    StructField(MPSResponse.SENSITIVTY_FLAG, StringType(), True),
    StructField(MPSResponse.MPS_ID, StringType(), True),
    StructField(MPSResponse.ERROR_SUCCESS_CODE, StringType(), True),
    StructField(MPSResponse.MATCHED_NHS_NO, StringType(), True),
    StructField(MPSResponse.MATCHED_ALGORITHM_INDICATOR, IntegerType(), True),
    StructField(MPSResponse.MATCHED_CONFIDENCE_PERCENTAGE, DecimalType(5, 2), True),
    StructField(MPSResponse.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MPSResponse.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MPSResponse.ALGORITHMIC_TRACE_DOB_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MPSResponse.ALGORITHMIC_TRACE_GENDER_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MPSResponse.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC, DecimalType(5, 2), True)
])


def generate_mps_request_header_fields():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    mps_request_header_fields = OrderedDict()
    mps_request_header_fields[mps_request_header.REQUEST_REFERENCE] = "MPTREQ_{}".format(timestamp)
    mps_request_header_fields[mps_request_header.WORKFLOW_ID] = MESH_WORKFLOW_ID.MPS_REQUEST
    mps_request_header_fields[mps_request_header.REQUEST_TIMESTAMP] = timestamp
    mps_request_header_fields[mps_request_header.NO_OF_DATA_RECORDS] = 0

    return mps_request_header_fields


def generate_mps_response_header_fields(response_reference: str = None, number_of_data_records: int = 0,
                                        file_response_code: str = SpineResponseCodes.SUCCESS) -> Dict[str, Any]:
    """
    Construct a dictionary representing the header of an MPS response

    Args:
        response_reference: The response reference; if unspecified, will be set to MPTREQ_%Y%m%d%H%M%S, for the current
            timestamp
        number_of_data_records: The number of records in the data file associated with this header
        file_response_code: The res

    Returns:

    """
    if not response_reference:
        response_reference = "MPTREQ_{}".format(datetime.now().strftime("%Y%m%d%H%M%S"))

    mps_response_header_fields = OrderedDict()
    mps_response_header_fields[mps_response_header.RESPONSE_REFERENCE] = response_reference
    mps_response_header_fields[mps_response_header.WORKFLOW_ID] = MESH_WORKFLOW_ID.MPS_RESPONSE
    mps_response_header_fields[mps_response_header.NO_OF_DATA_RECORDS] = number_of_data_records
    mps_response_header_fields[mps_response_header.FILE_RESPONSE_CODE] = file_response_code

    return mps_response_header_fields
