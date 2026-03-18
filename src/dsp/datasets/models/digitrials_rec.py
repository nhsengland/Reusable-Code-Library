from datetime import datetime
from typing import List

from pyspark.sql.types import ArrayType, StringType, IntegerType, DateType

from dsp.common.structured_model import SubmittedAttribute, BaseTableModel


class DigitrialsRecClinicalCriteria(BaseTableModel):
    __table__ = "clinical_criteria"
    __concrete__ = True

    nic_number = SubmittedAttribute('NIC_NUMBER', str, spark_type=StringType())
    icd_codes = SubmittedAttribute("ICD_CODES", List[str], spark_type=ArrayType(StringType()))
    criteria_version_number = SubmittedAttribute('CRITERIA_VERSION_NUMBER', int, spark_type=IntegerType())


class DigitrialsRecParticipants(BaseTableModel):
    __table__ = "participants"
    __concrete__ = True

    nic_number = SubmittedAttribute('NIC_NUMBER', str, spark_type=StringType())
    batch_id = SubmittedAttribute('BATCH_ID', int, spark_type=IntegerType())
    batch_timestamp = SubmittedAttribute('BATCH_TIMESTAMP', datetime)
    unique_reference = SubmittedAttribute('UNIQUE_REFERENCE', str, spark_type=StringType())
    nhs_no = SubmittedAttribute('NHS_NO', str, spark_type=StringType())
    date_of_birth = SubmittedAttribute('DATE_OF_BIRTH', datetime, spark_type=DateType())
    criteria_version_number = SubmittedAttribute('CRITERIA_VERSION_NUMBER', int, spark_type=IntegerType())
    comms_template = SubmittedAttribute("COMMS_TEMPLATE", str, spark_type=StringType())
    submission_filename = SubmittedAttribute("SUBMISSION_FILENAME", str, spark_type=StringType())
    submission_row_number = SubmittedAttribute("SUBMISSION_ROW_NUMBER", int, spark_type=IntegerType())
