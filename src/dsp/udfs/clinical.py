from datetime import date, datetime
from typing import Tuple, Union

from dsp.validations.common import DateDelta
from dsp.dq_files.formats import string_is_valid_date_format
from dsp.model.clinical_records import ClinicalMappingTableModel
from testdata.ref_data import providers, as_point_in_time, DEFAULT_MAX_END_DATE
from testdata.ref_data.generators.organisations import normalize_date
from dsp.shared.content_types import STANDARD_DATE_FORMAT, NO_SEPARATOR_DATE_FORMAT


def transform_date(point_in_time: Union[str, int, date, datetime]) -> int:
    if isinstance(point_in_time, str):
        if not string_is_valid_date_format(point_in_time, STANDARD_DATE_FORMAT):
            return None
        point_in_time = datetime.strptime(point_in_time, STANDARD_DATE_FORMAT)

    if isinstance(point_in_time, int):
        point_in_time = str(point_in_time)
        if not string_is_valid_date_format(point_in_time, NO_SEPARATOR_DATE_FORMAT):
            return None
        point_in_time = datetime.strptime(point_in_time, NO_SEPARATOR_DATE_FORMAT)

    return point_in_time


def is_nicip_valid(nicip_id, point_in_time: Union[str, int, date, datetime], offset: int = None) -> bool:
    if not nicip_id or not point_in_time:
        return False  # failed in validation as nothing to check against

    nicip_id = (nicip_id or '').strip().upper()
    point_in_time = transform_date(point_in_time)
    nicip_rec = providers.NicipMappingTableProvider().get(nicip_id)

    if not nicip_rec:
        return False  # NICIP not present in reference data

    if offset:
        return nicip_rec.was_active_at(point_in_time, DateDelta(months=offset))

    return nicip_rec.was_active_at(point_in_time)


def is_valid_nicip_active_with_offset(nicip_id, point_in_time: Union[str, int, date, datetime], offset: int) -> bool:
    if not nicip_id or not point_in_time or not offset:
        return False

    nicip_id = (nicip_id or '').strip().upper()
    point_in_time = transform_date(point_in_time)
    nicip_rec = providers.NicipMappingTableProvider().get(nicip_id)

    if not nicip_rec:
        return True  # Invalid nicip so skip validation

    if nicip_rec.was_active_at(point_in_time):
        return True
    elif nicip_rec.was_active_at(point_in_time, DateDelta(months=offset)):
        return False

    return True


def is_snomed_valid(snomed_id, point_in_time: Union[str, int, date, datetime], offset: int = None) -> bool:
    if not snomed_id or not point_in_time:
        return False  # failed in validation as nothing to check against

    snomed_id = (snomed_id or '').strip().upper()
    point_in_time = transform_date(point_in_time)
    snomed_rec = providers.SnomedMappingTableProvider().get(snomed_id)

    if not snomed_rec:
        return False  # SNOMED not present in reference data

    if offset:
        return snomed_rec.was_active_at(point_in_time, DateDelta(months=offset))

    return snomed_rec.was_active_at(point_in_time)


def is_valid_snomed_active_with_offset(snomed_id, point_in_time: Union[str, int, date, datetime], offset: int) -> bool:
    if not snomed_id or not point_in_time or not offset:
        return False

    snomed_id = (snomed_id or '').strip().upper()
    point_in_time = transform_date(point_in_time)
    snomed_rec = providers.SnomedMappingTableProvider().get(snomed_id)

    if not snomed_rec:
        return True  # Invalid snomed so skip validation

    if snomed_rec.was_active_at(point_in_time):
        return True
    elif snomed_rec.was_active_at(point_in_time, DateDelta(months=offset)):
        return False

    return True


def valid_nicip_maps_to_valid_snomed(nicip_id: str, snomed_id: Union[str, int],
                         point_in_time: Union[str, int, date, datetime]=None) -> bool:

    if not (nicip_id and snomed_id and point_in_time):
        return False # No Nicip or Snomed so fail validation

    point_in_time = transform_date(point_in_time)
    mapping_from_snomed, mapping_from_nicip, = clinical_mapping_records(snomedct_id=snomed_id, nicip_id=nicip_id, point_in_time=point_in_time)

    if not (mapping_from_nicip and mapping_from_snomed):
        return True # Invalid nicip or snomed so skip validation

    return int(snomed_id) == mapping_from_nicip.record_dict['snomedct_id']


def get_snomed(
        snomedct_id: str, point_in_time: Union[str, int, date, datetime] = None) -> \
        Union[ClinicalMappingTableModel, None]:
    mapping_rec = providers.SnomedMappingTableProvider().get(snomedct_id)

    if not mapping_rec or not mapping_rec.was_active_at(point_in_time):
        return None

    record_dict = next((tp[1] for tp in mapping_rec.clinical_map if tp[0] <= as_point_in_time(point_in_time)))

    if not record_dict:
        return None

    return ClinicalMappingTableModel(record_dict)


def get_nicip(nicip_id: str, point_in_time: Union[str, int, date, datetime] = None) -> \
        Union[ClinicalMappingTableModel, None]:
    mapping_rec = providers.NicipMappingTableProvider().get(nicip_id)

    if not mapping_rec or not mapping_rec.was_active_at(point_in_time):
        return None

    record_dict = next((tp[1] for tp in mapping_rec.clinical_map if tp[0] <= as_point_in_time(point_in_time)))

    if not record_dict or 'nicip_description' not in record_dict:
        return None

    record_dict['nicip_id'] = nicip_id
    snomedct_id = record_dict['snomedct_id']
    clinical_snomed_record = get_snomed(snomedct_id, point_in_time)

    if clinical_snomed_record:
        clinical_snomed_record.nicip_id = nicip_id
        clinical_snomed_record.nicip_description = record_dict['nicip_description']
        return clinical_snomed_record

    return ClinicalMappingTableModel(record_dict)


def clinical_mapping_records(nicip_id: str = None, snomedct_id: str = None,
                             point_in_time: Union[str, int, date, datetime] = None):
    mapping_nicip_id = None
    mapping_snomed_ct = None
    if snomedct_id:
        mapping_snomed_ct = get_snomed(snomedct_id, point_in_time)

        if mapping_snomed_ct is not None and nicip_id:
            mapping_snomed_ct.nicip_id = nicip_id

    if nicip_id:
        mapping_nicip_id = get_nicip(nicip_id, point_in_time)

        if mapping_nicip_id is not None and snomedct_id:
            mapping_nicip_id.snomedct_id = snomedct_id

    if mapping_snomed_ct is not None and mapping_nicip_id is not None:
        mapping_snomed_ct.nicip_description = mapping_nicip_id.nicip_description
        mapping_nicip_id.snomedct_description = mapping_snomed_ct.snomedct_description

    return mapping_snomed_ct, mapping_nicip_id


def clinical_struct_from_nicip_or_snomedct(nicip_id: str = None, snomedct_id: str = None,
                                           point_in_time: Union[str, int, date, datetime] = None) -> Tuple:
    if not (nicip_id or snomedct_id):
        raise ValueError('Either nicip_code or snomedct_id need to be provided.')

    mapping_snomed_ct, mapping_nicip_id = clinical_mapping_records(nicip_id, snomedct_id, point_in_time)
    mapping = mapping_snomed_ct if mapping_snomed_ct is not None else mapping_nicip_id
    if mapping is None:
        raise ValueError('No mapping records found for nicip_code or snomedct_id')

    return (
        mapping.nicip_id,
        mapping.nicip_description,
        mapping.snomedct_id,
        mapping.snomedct_description,
        mapping.modality_id,
        mapping.modality,
        mapping.sub_modality_id,
        mapping.sub_modality,
        mapping.region_id,
        mapping.region,
        mapping.sub_region_id,
        mapping.sub_region,
        mapping.system_id,
        mapping.system,
        mapping.sub_system_id,
        mapping.sub_system,
        mapping.sub_system_component_id,
        mapping.sub_system_component,
        mapping.morphology_id,
        mapping.morphology,
        mapping.fetal_id,
        mapping.fetal,
        mapping.early_diagnosis_of_cancer,
        mapping.sub_early_diagnosis_of_cancer,
    )
