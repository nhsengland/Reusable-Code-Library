from datetime import date, datetime
from dsp.common.structured_model import (
    SubmittedAttribute,
    BaseTableModel,
    DerivedAttribute
)


class DigitrialsOutAggregateSummaryModel(BaseTableModel):
    __table__ = 'aggregate_summary'
    __concrete__ = True

    BATCH_ID = DerivedAttribute('BATCH_ID', int, 'submission_id')
    TRIAL_NAME = SubmittedAttribute('TRIAL_NAME', str)
    DSA_START_DATE = SubmittedAttribute('DSA_START_DATE', date)
    DSA_END_DATE = SubmittedAttribute('DSA_END_DATE', date)
    NIC_NUMBER = SubmittedAttribute('NIC_NUMBER', str)
    FILENAME = SubmittedAttribute('FILENAME', str)
    STATUS = SubmittedAttribute('STATUS', str)
    COHORTED = SubmittedAttribute('COHORTED', bool)
    COHORT_REFERENCE = SubmittedAttribute('COHORT_REFERENCE', str)
    SIZE_OF_COHORT = SubmittedAttribute('SIZE_OF_COHORT', int)
    DEID = SubmittedAttribute('DEID', bool)
    CPOS = SubmittedAttribute('CPOS', bool)
    DAE = SubmittedAttribute('DAE', bool)
    NUMBER_OF_HES_AE_EXTRACTS = SubmittedAttribute('NUMBER_OF_HES_AE_EXTRACTS', int)
    NUMBER_OF_HES_APC_EXTRACTS = SubmittedAttribute('NUMBER_OF_HES_APC_EXTRACTS', int)
    NUMBER_OF_HES_CC_EXTRACTS = SubmittedAttribute('NUMBER_OF_HES_CC_EXTRACTS', int)
    NUMBER_OF_HES_OP_EXTRACTS = SubmittedAttribute('NUMBER_OF_HES_OP_EXTRACTS', int)
    NUMBER_OF_CIVREG_MORT_EXTRACTS = SubmittedAttribute('NUMBER_OF_CIVREG_MORT_EXTRACTS', int)
    NUMBER_OF_CANCER_EXTRACTS = SubmittedAttribute('NUMBER_OF_CANCER_EXTRACTS', int)
    NUMBER_OF_DEMOGRAPHICS_EXTRACTS = SubmittedAttribute('NUMBER_OF_DEMOGRAPHICS_EXTRACTS', int)
    NUMBER_OF_PRIMCARE_MEDS_EXTRACTS = SubmittedAttribute('NUMBER_OF_PRIMCARE_MEDS_EXTRACTS', int)
    SUBMITTED_TIMESTAMP_UTC = SubmittedAttribute('SUBMITTED_TIMESTAMP_UTC', datetime)
    COMPLETION_TIMESTAMP_UTC = SubmittedAttribute('COMPLETION_TIMESTAMP_UTC', datetime)
