import pytest

from dsp.udfs import clinical


@pytest.mark.parametrize("nicip_code, point_in_time, offset, expected", [
    (None, '2012-06-01', 0, False),  # invalid as code is None
    ('$$$$$', '2012-06-01', 0, False),  # invalid nicip
    ('CABWC', None, None, False),  # invalid as point in time is none
    ('CABWC', '2012-06-01', 0, True),  # active nicip since 20120401
    ('CABWC', '2012-04-01', None, True),  # active nicip since 20120401
    ('CABWC', '2012-03-01', 6, True),  # inactive nicip and no offset(grace period)
    ('CABWC', '2012-03-01', None, False),  # inactive nicip and no offset(grace period)
    ('CABWC', '2011-10-01', 6, True),  # active nicip at offset
    ('CABWC', '2012-06-01', 3, True)  # active nicip at point in time but inactive at offset (should return True))
])
def test_is_nicip_valid(nicip_code, point_in_time, offset, expected):
    assert expected == clinical.is_nicip_valid(nicip_code, point_in_time, offset)


@pytest.mark.parametrize("nicip_code, point_in_time, offset, expected", [
    ('$$$$$', '2012-06-01', 1, True),  # invalid nicip should pass (skip validation)
    ('$$$$$', '2012-06-01', None, False),  # offset is mandatory
    (None, '2012-06-01', None, False),  # code is mandatory
    ('$$$$$', None, None, False),  # point-in-time is mandatory
    ('CABWC', '2012-06-01', 6, True),  # active nicip so should return True skip offset
    ('CABWC', '2011-10-01', 6, False),  # inactive nicip but active at offset
    ('CABWC', '2011-10-01', 1, True),  # nicip inactive and inactive at offset
])
def test_is_valid_nicip_active_with_offset(nicip_code, point_in_time, offset, expected):
    assert expected == clinical.is_valid_nicip_active_with_offset(nicip_code, point_in_time, offset)


@pytest.mark.parametrize("snomed_code, point_in_time, offset, expected", [
    (None, '2018-01-01', 0, False), # invalid code so return False
    ('$$$$', '2021-07-22', None, False), # invalid SNOMED so return False
    ('1095291000000109', None, None, False), # SNOMED invalid as point in time is None
    ('1095291000000109', '2020-10-01', None, True), # SNOMED active at time
    ('1095291000000109', '2018-04-01', None, True), # SNOMED active at time
    ('418782007', '2017-10-01', None, False), # inactive SNOMED by end date so return False
    ('418782007', '2017-10-01', 1, True), # inactive SNOMED by end date but active with offset so return True
    ('418782007', '2013-03-01', None, False),  # inactive SNOMED by start date so return False
    ('418782007', '2013-03-01', 1, True),  # inactive SNOMED by start date but active with offset so return True
])
def test_is_snomed_valid(snomed_code, point_in_time, offset, expected):
    assert expected == clinical.is_snomed_valid(snomed_code, point_in_time, offset)


@pytest.mark.parametrize("snomed_code, point_in_time, offset, expected", [
    ('$$$$$', '2012-06-01', 1, True),  # invalid snomed should pass (skip validation)
    ('$$$$$', '2012-06-01', None, False),  # offset is mandatory
    (None, '2012-06-01', None, False),  # code is mandatory
    ('$$$$$', None, None, False),  # point-in-time is mandatory
    ('1095291000000109', '2020-10-01', None, False), # SNOMED active at time but no offset so False
    ('1095291000000109', '2018-04-01', 0, False), # zero or None offset returns False
    ('1095291000000109', '2018-03-01', 1, False), # SNOMED only active at offset so returns False
    ('1095291000000109', '2018-04-01', 1, True), # SNOMED active at point_in_time so returns True
    ('1084881000000108', '2018-09-30', 1, True),  # SNOMED active at point_in_time so returns True
    ('1084881000000108', '2018-10-01', 1, False),  # SNOMED only active at offset so returns False
    ('418782007', '2017-10-01', 1, False),  # inactive SNOMED by end date but active with offset so return True
    ('418782007', '2013-03-01', 1, False),  # inactive SNOMED by start date but active with offset so return True
    ('1065981000000102', '2018-04-30', 6, False)
])
def test_is_valid_snomed_active_with_offset(snomed_code, point_in_time, offset, expected):
    x = clinical.is_valid_snomed_active_with_offset(snomed_code, point_in_time, offset)
    assert expected == clinical.is_valid_snomed_active_with_offset(snomed_code, point_in_time, offset)


@pytest.mark.parametrize("snomedct_id, nicip_id, point_in_time, expected", [
    ('609168009', None, 20151101, [
        None, None, 609168009,
        None,
        77477000, 'Computerized axial tomography (procedure)', None, None, 113345001,
        'Abdominal structure (body structure)', None, None, 21514008,
        'Structure of genitourinary system (body structure)', 122489005,
        'Urinary system structure (body structure)', 64033007,
        'Kidney structure (body structure)', 118956008,
        'Body structure, altered from its original anatomical structure (morphologic abnormality)',
        None, None, None, None
    ]),
    ('609168009', None, 20150901, None),
    ('752621000000107', None, 20130501, [
        None, None, 752621000000107,
        None,
        77477000, 'Computerized axial tomography (procedure)', None, None, 12921003,
        'Pelvic structure (body structure)', None, None, 21514008,
        'Structure of genitourinary system (body structure)', 122489005,
        'urinary system structure (body structure)', 64033007,
        'kidney structure (body structure)', None, None, None, None, None, None
    ]),
    ('752621000000107', None, 20131101, [
        None, None, 752621000000107,
        None,
        77477000, 'Computerized axial tomography (procedure)', None, None, 113345001,
        'Abdominal structure (body structure)', None, None, 21514008,
        'Structure of genitourinary system (body structure)', 122489005,
        'urinary system structure (body structure)', 64033007,
        'kidney structure (body structure)', None, None, None, None, None, None
    ])
])
def test_clinical_mapping_snomed_records(snomedct_id, nicip_id, point_in_time, expected):
    mapping, nicip = clinical.clinical_mapping_records(snomedct_id=snomedct_id, nicip_id=nicip_id, point_in_time=point_in_time)
    if mapping:
        result = [
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
            mapping.sub_early_diagnosis_of_cancer]
        assert expected == result
    else:
        assert expected is None


@pytest.mark.parametrize("snomedct_id, nicip_id, point_in_time, expected", [
    (None, 'CCKILA', 20131101, [
        'CCKILA', 'CT Pc cryotherapy tumour Abl kidney Left', 752621000000107,
        None,
        77477000, 'Computerized axial tomography (procedure)', None, None, 113345001,
        'Abdominal structure (body structure)', None, None, 21514008,
        'Structure of genitourinary system (body structure)', 122489005,
        'urinary system structure (body structure)', 64033007,
        'kidney structure (body structure)', None, None, None, None, None, None
    ]),
    (None, 'CCKILA', 20111101, None),
    (None, 'CCKILA', 20151101, [
        'CCKILA', 'CT Pc cryotherapy tumour Abl kidney Left', 609168009,
        None,
        77477000, 'Computerized axial tomography (procedure)', None, None, 113345001,
        'Abdominal structure (body structure)', None, None, 21514008,
        'Structure of genitourinary system (body structure)', 122489005,
        'Urinary system structure (body structure)', 64033007,
        'Kidney structure (body structure)', 118956008,
        'Body structure, altered from its original anatomical structure (morphologic abnormality)',
        None, None, None, None
    ])
])
def test_clinical_mapping_nicip_records(snomedct_id, nicip_id, point_in_time, expected):
    snomed, mapping = clinical.clinical_mapping_records(snomedct_id=snomedct_id, nicip_id=nicip_id, point_in_time=point_in_time)
    if mapping:
        result = [
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
            mapping.sub_early_diagnosis_of_cancer]
        assert expected == result
    else:
        assert expected is None


@pytest.mark.parametrize("snomedct_id, nicip_id, point_in_time, expected", [
    (752621000000107, 'CCKILA', 20131101, [
        'CCKILA', 'CT Pc cryotherapy tumour Abl kidney Left', 752621000000107,
        None,
        77477000, 'Computerized axial tomography (procedure)', None, None, 113345001,
        'Abdominal structure (body structure)', None, None, 21514008,
        'Structure of genitourinary system (body structure)', 122489005,
        'urinary system structure (body structure)', 64033007,
        'kidney structure (body structure)', None, None, None, None, None, None
    ]),
])
def test_clinical_mapping_snomed_and_nicip_records(snomedct_id, nicip_id, point_in_time, expected):
    snomed, mapping = clinical.clinical_mapping_records(snomedct_id=snomedct_id, nicip_id=nicip_id, point_in_time=point_in_time)
    if mapping:
        result = [
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
            mapping.sub_early_diagnosis_of_cancer]
        assert expected == result
    else:
        assert expected is None


@pytest.mark.parametrize("nicip_code, snomed_code, point_in_time, expected", [
    ('CCKILA', 609168009, '2015-11-01', True),  # True, Nicip valid and maps to Snomed
    ('CCKILA', 609168009, '2012-03-31', True),  # Invalid Nicip so validation skipped
    ('CCKILA', 12345, '2015-11-01', True),  # Invalid SNOMED so validation skipped
    ('1234%^', '6£9168$$9', '2015-11-01', True),  # Invalid NICIP so validation skipped
    (None, None, None, False), # No codes or point in time so fail validation
    ('CCKILA', None, '2015-11-01', False), # No SNOMED so validation will fail
    ('CCKILA', 609168009, None, False), # No point in time so validation will fail
    ('CSHRB', 865651000000103, '2021-07-01', False), # Valid NICIP and valid SNOMED but not mapped so validation failed
])
def test_valid_nicip_maps_to_snomed(nicip_code, snomed_code, point_in_time, expected):
    assert expected == clinical.valid_nicip_maps_to_valid_snomed(nicip_code, snomed_code, point_in_time)


@pytest.mark.parametrize("nicip_code,snomed_code,point_in_time,expected", [
    # With NICIP Code and Snomed Code
    ('CCKILA', '609168009', '2015-11-01', ('CCKILA', 'CT Pc cryotherapy tumour Abl kidney Left',
 609168009, None, 77477000, 'Computerized axial tomography (procedure)', None, None, 113345001,
 'Abdominal structure (body structure)', None, None, 21514008, 'Structure of genitourinary system (body structure)',
 122489005, 'Urinary system structure (body structure)', 64033007, 'Kidney structure (body structure)', 118956008,
 'Body structure, altered from its original anatomical structure (morphologic abnormality)', None, None, None, None)),
    # With only Snomed Code
    (None, '609168009', '2015-11-01', (None, None, 609168009, None, 77477000, 'Computerized axial tomography (procedure)',
 None, None, 113345001, 'Abdominal structure (body structure)', None, None, 21514008, 'Structure of genitourinary system (body structure)',
 122489005, 'Urinary system structure (body structure)', 64033007, 'Kidney structure (body structure)', 118956008,
 'Body structure, altered from its original anatomical structure (morphologic abnormality)', None, None, None, None)),
    # With only NICIP Code
    ('CCKILA', None, '2015-11-01', ('CCKILA', 'CT Pc cryotherapy tumour Abl kidney Left', 609168009, None,
 77477000, 'Computerized axial tomography (procedure)', None, None, 113345001, 'Abdominal structure (body structure)',
 None, None, 21514008, 'Structure of genitourinary system (body structure)', 122489005,
 'Urinary system structure (body structure)', 64033007, 'Kidney structure (body structure)', 118956008,
 'Body structure, altered from its original anatomical structure (morphologic abnormality)', None, None, None, None)),
])
def test_clinical_struct_from_nicip_or_snomedct(nicip_code, snomed_code, point_in_time, expected):
    assert expected == clinical.clinical_struct_from_nicip_or_snomedct(nicip_code, snomed_code, point_in_time)
