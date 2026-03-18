import re
from collections import namedtuple

from dsp.shared.constants import MESH_WORKFLOW_ID, GP_DATA_SUBMISSION_WORKFLOWS, DS

SUBMISSION_DATASET_IDS = {
    MESH_WORKFLOW_ID.GP_DATA_PATIENT: DS.GP_DATA_PATIENT,
    MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT: DS.GP_DATA_APPOINTMENT,
    MESH_WORKFLOW_ID.GP_DATA_PATIENT_REPORT: DS.GP_DATA_PATIENT_REPORT,
    MESH_WORKFLOW_ID.GP_DATA_APPOINTMENT_REPORT: DS.GP_DATA_APPOINTMENT_REPORT,
    MESH_WORKFLOW_ID.GP_DATA_PREM_STOP: DS.GP_DATA_PREM_STOP,
    MESH_WORKFLOW_ID.GP_DATA_PREM_INVALID: DS.GP_DATA_PREM_INVALID,
    MESH_WORKFLOW_ID.GP_DATA_PREM_EXTRACT_ACK: DS.GP_DATA_PREM_EXTRACT_ACK
}
GPDataFile = namedtuple("GPDataFile", "workflow_id message_id timestamp chan_id seq ext mailbox_from")

UUID_PAT = '[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}'
FILENAME_FILTER = re.compile(
    f'^(?P<workflow_id>{"|".join(SUBMISSION_DATASET_IDS.keys())})'
    f'_?(?P<message_id>(?<=_){UUID_PAT})?'
    r'_(?P<timestamp>\d{14})'
    f'_?(?P<chan_id>(?<=_){UUID_PAT})?'
    r'_?(?P<seq>(?<=_)\d+)?'
    '_?(?P<mailbox_from>(?<=_)[a-zA-Z0-9]+)?'  # Only check if "_" is present, otherwise None
    r'\.(?P<ext>.*)$'
)


def parse_filename(filename: str) -> GPDataFile:
    value_error = f"'{filename}' is not a valid GP Data Filename"
    match = FILENAME_FILTER.match(filename)
    if not match:
        raise ValueError(value_error)

    gp_data_file = GPDataFile(match["workflow_id"], match["message_id"], match["timestamp"], match["chan_id"], match["seq"],
                              match["ext"], match["mailbox_from"])

    if gp_data_file.workflow_id in GP_DATA_SUBMISSION_WORKFLOWS:
        if not gp_data_file.message_id or not gp_data_file.chan_id or not gp_data_file.seq:
            raise ValueError(value_error)
    elif gp_data_file.message_id or gp_data_file.chan_id or gp_data_file.seq:
        raise ValueError(value_error)

    return gp_data_file
