from datetime import date
from typing import Optional, Any, Type

import pytest

from dsp.datasets.models.msds import CareActivity, CareActivityBaby, CareActivityLabourAndDelivery, AnonFindings, \
    FindingAndObservationMother, PregnancyAndBookingDetails
from dsp.common.structured_model import DSPStructuredModel, DerivedAttributePlaceholder, DerivedAttribute


def setup_structured_model(model_class: Type[DSPStructuredModel], root=None, parent=None, **fields):
    """ Utility function to setup a new structure model instance populated with the specified field values.

    The types of the field values are checked against the model metadata, if any are incorrect an assertion error is
    raised.
    """
    model_attributes = dict(model_class.get_model_attributes())

    record_dict = {}
    directly_assignable_fields = {}

    for field_name, field_value in fields.items():
        assert field_name in model_attributes, 'Model does not contain field: {}'.format(field_name)

        field_type = model_attributes[field_name]
        value_type = field_type.value_type
        assert field_value is None or isinstance(field_value, value_type), \
            'Field {} should have type {} but was {}'.format(field_name, value_type, type(field_value))

        # Some ModelAttribute sub-types cannot be set directly - these are added to the record_dict
        if isinstance(field_type, DerivedAttribute):
            record_dict[field_name] = field_value
        elif isinstance(field_type, DerivedAttributePlaceholder):
            directly_assignable_fields[field_name] = field_value
        else:
            record_dict[field_name] = field_value

    model = model_class(record_dict, root=root, parent=parent or root)
    for field_name, field_value in directly_assignable_fields.items():
        setattr(model, field_name, field_value)

    return model


def assert_derivation_result(model: DSPStructuredModel, field_name: str, expected: Optional[Any]):
    """ Asserts that the derivation field has the expected value and that the type of the value matches the
    type declared in the model metadata.
    """
    actual = getattr(model, field_name)
    assert actual == expected

    model_class = type(model)
    model_attributes = dict(model_class.get_model_attributes())
    value_type = model_attributes[field_name].value_type

    if actual is not None:
        assert isinstance(actual, value_type), \
            'Field {} should have type {} but was {}'.format(field_name, value_type, type(actual))


class TestCareActivitySnomedDerivations(object):
    """ Unit tests for SNOMED mappings / derivations for CareActivity """

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('06', '27113001', None, '27113001'),  # original was SNOMED
        ('06', '27113001;27113001', None, '27113001;27113001'),  # original was SNOMED encoded list
        ('06', None, None, None),  # original was SNOMED, code is missing

        ('04', None, None, None),  # original was not SNOMED, code was missing
        ('04', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('04', '1234', 27113001, '27113001'),  # original was not SNOMED, code was mapped as int

        ('05', None, None, None),  # original was not SNOMED, code was missing
        ('05', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('05', '1234', 27113001, '27113001'),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_procedure_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivity,
            ProcedureScheme=original_scheme,
            ProcedureCode=original_code,
            MapSnomedCTProcedureCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTProcedureCode', expected)

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('04', '27113001', None, 27113001),  # original was SNOMED
        ('04', None, None, None),  # original was SNOMED, code is missing

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('03', None, None, None),  # original was not SNOMED, code was missing
        ('03', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('03', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_finding_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivity,
            FindingScheme=original_scheme,
            FindingCode=original_code,
            MapSnomedCTFindingCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingCode', expected)

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('03', '27113001', None, 27113001),  # original was SNOMED
        ('03', None, None, None),  # original was SNOMED, code is missing

        ('01', None, None, None),  # original was not SNOMED, code was missing
        ('01', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('01', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_obs_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivity,
            ObsScheme=original_scheme,
            ObsCode=original_code,
            MapSnomedCTObsCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTObsCode', expected)

    @staticmethod
    @pytest.mark.parametrize('master_obs_code, obs_value, expected', [
        (None, None, None),  # obs code and value both missing
        (27113001, None, None),  # Obs Code not matching, obs value is None , result is None
        (160573003, '20', '20'),  # Obs Code matching result is same as obs_value
        (27113001, '20', None),  # Obs Code not matching obs value not used , result is None
        (160573003, '0', '0'),  # Zero measurement
        (160573003, '0.5', '0.5'),  # Floating point
        (160573003, '-0.5', None),  # Negative measurement
        (160573003, 'not-a-number', None),  # Non-numeric value
    ])
    def test_alcohol_units_per_week(master_obs_code, obs_value, expected):
        record = setup_structured_model(
            CareActivity,
            MasterSnomedCTObsCode=master_obs_code,
            ObsValue=obs_value
        )

        assert_derivation_result(record, 'AlcoholUnitsPerWeek', expected)

    @staticmethod
    @pytest.mark.parametrize('master_obs_code, ucum_units, obs_value, expected', [
        (None, 'cm', None, None),  # obs code and value both missing
        (27113001, 'cm', None, None),  # Obs Code not matching, obs value is None , result is None
        (50373000, 'cm', '155', '155'),  # Obs Code and cm matching result is same as obs_value
        (50373000, 'cm', '155.5', '155.5'),  # Original value in cm with decimal point
        (50373000, 'Cm', '155.5', '155.5'),  # Unit is case insensitive
        (50373000, 'm', '1.55', '155.00'),  # Original value in meters - multiply by 100
        (50373000, 'M', '1.55', '155.00'),  # Unit is case insensitive
        (160573003, 'cm', '165', None),  # Obs Code not matching obs value not used , result is None
        (50373000, 'cm', '-1', None),  # Negative number
        (50373000, 'cm', 'not-numeric', None),  # Non-numeric value
    ])
    def test_person_height(master_obs_code, ucum_units, obs_value, expected):
        record = setup_structured_model(
            CareActivity,
            MasterSnomedCTObsCode=master_obs_code,
            UCUMUnit=ucum_units,
            ObsValue=obs_value
        )

        assert_derivation_result(record, 'PersonHeight', expected)

    @staticmethod
    @pytest.mark.parametrize('master_obs_code, ucum_units, obs_value, expected', [
        (None, None, None, None),  # obs code and value both missing
        (27113001, 'kg', None, None),  # Obs Code not matching, obs value is None , result is None
        (50373000, 'kg', '65', None),  # Obs Code not matching result is none
        (27113001, 'kg', '70', '70'),  # Obs Code matching, result is same as obs_value
        (27113001, 'KG', '70', '70'),  # Unit is case insensitive
        (27113001, 'kilograms', '70', '70'),  # Alternate unit value for kg
        (27113001, 'KILOgRaMS', '70', '70'),  # Unit is case insensitive
        (27113001, 'kg', '70.2', '70.2'),  # Measurement with decimal point
        (27113001, 'kg', '-1', None),  # Negative measurement
        (27113001, 'kg', 'not-a-number', None),  # Non-numeric measurement
    ])
    def test_person_weight(master_obs_code, ucum_units, obs_value, expected):
        record = setup_structured_model(
            CareActivity,
            MasterSnomedCTObsCode=master_obs_code,
            UCUMUnit=ucum_units,
            ObsValue=obs_value
        )

        assert_derivation_result(record, 'PersonWeight', expected)

    @staticmethod
    @pytest.mark.parametrize('master_obs_code, obs_value, expected', [
        (None, None, None),  # obs code and value both missing
        (27113001, None, None),  # Obs Code not matching, obs value is None , result is None
        (50373000, '6', None),  # Obs Code not matching result is none
        (230056004, '9', '9'),  # Obs Code matching, result is same as obs_value
        (230056004, '9.5', '9.5'),  # Measurement with decimal point
        (230056004, '-1', None),  # Negative measurement
        (230056004, 'not-a-number', None),  # Non-numeric measurement
    ])
    def test_cigarettes_per_day(master_obs_code, obs_value, expected):
        record = setup_structured_model(
            CareActivity,
            MasterSnomedCTObsCode=master_obs_code,
            ObsValue=obs_value
        )

        assert_derivation_result(record, 'CigarettesPerDay', expected)

    @staticmethod
    @pytest.mark.parametrize('master_obs_code, ucum_units, obs_value, finding_code, expected', [
        (None, None, None, None, None),  # obs code and value both missing
        (27113001, None, None, None, None),  # Obs Code not matching , obs value is None, Finding code None
        (251900003, None, '6', 1110631000000106, 'Test Declined'),  # Obs code matching, null ucum_units, finding code matching
        (251900003, None, '6', 50373000, None),  # Obs code matching, null ucum_units, finding code not matching
        (50373000, 'coppm', '6', 50373000, None),  # Obs Code not matching , finding code also not matching
        (251900003, 'coppm', '3', 50373000, '3'),  # Obs Code matching, result is same as obs_value
        (50373000, 'coppm', '3', 1110631000000106, 'Test Declined'),  # Obs Code not matching,Finding matching
        (251900003, 'coppm', '3', 1110631000000106, '3'),  # Obs Code and Finding code matching but obs take priority
        (251900003, 'cOpPM ', '3', 1110631000000106, '3'),  # Unit is case insensitive
        (None, 'coppm', None, 1110631000000106, 'Test Declined'),  # Obs Code not matching, Finding matching
        (251900003, 'wrong_unit', '3', 1110631000000106, 'Test Declined'),  # Multiple matches - obs takes priority
        (251900003, 'coppm', '3.14', 1110631000000106, '3.14'),  # floating point measurement
        (251900003, 'coppm', '0', 1110631000000106, '0'),  # zero measurement
        (251900003, 'coppm', '-1', 1110631000000106, 'Test Declined'),  # negative measurement
    ])
    def test_com_reading(master_obs_code, ucum_units, obs_value, finding_code, expected):
        record = setup_structured_model(
            CareActivity,
            MasterSnomedCTObsCode=master_obs_code,
            UCUMUnit=ucum_units,
            ObsValue=obs_value,
            MasterSnomedCTFindingCode=finding_code
        )

        assert_derivation_result(record, 'COMonReading', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        ('1650200', date(2019, 1, 1), None),  # snomed_code not match
        ('77176002', date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_procedure_term(snomed_code: str, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivity,
            root,
            MasterSnomedCTProcedureCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTProcedureTerm', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_finding_term(snomed_code: int, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivity,
            root,
            MasterSnomedCTFindingCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingTerm', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_obs_term(snomed_code: int, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivity,
            root,
            MasterSnomedCTObsCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTObsTerm', expected)


class TestCareActivityLabourAndDeliverySnomedDerivations(object):
    """ Unit tests for SNOMED mappings / derivations for CareActivityLabourAndDelivery """

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('06', '27113001', None, '27113001'),  # original was SNOMED
        ('06', '27113001;27113001', None, '27113001;27113001'),  # original was SNOMED encoded list
        ('06', None, None, None),  # original was SNOMED, code is missing

        ('04', None, None, None),  # original was not SNOMED, code was missing
        ('04', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('04', '1234', 27113001, '27113001'),  # original was not SNOMED, code was mapped as int

        ('05', None, None, None),  # original was not SNOMED, code was missing
        ('05', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('05', '1234', 27113001, '27113001'),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_procedure_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivityLabourAndDelivery,
            ProcedureScheme=original_scheme,
            ProcedureCode=original_code,
            MapSnomedCTProcedureCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTProcedureCode', expected)

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('04', '27113001', None, 27113001),  # original was SNOMED
        ('04', None, None, None),  # original was SNOMED, code is missing

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('03', None, None, None),  # original was not SNOMED, code was missing
        ('03', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('03', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_finding_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivityLabourAndDelivery,
            FindingScheme=original_scheme,
            FindingCode=original_code,
            MapSnomedCTFindingCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingCode', expected)

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('03', '27113001', None, 27113001),  # original was SNOMED
        ('03', None, None, None),  # original was SNOMED, code is missing

        ('01', None, None, None),  # original was not SNOMED, code was missing
        ('01', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('01', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_obs_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivityLabourAndDelivery,
            ObsScheme=original_scheme,
            ObsCode=original_code,
            MapSnomedCTObsCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTObsCode', expected)

    @staticmethod
    @pytest.mark.parametrize('procedure_code, finding_code, expected', [
        ('85548006', None, '85548006'),  # snomed code is matching
        ('364589006;85548006;364589007', None, None),  # only exact match is allowed - no partial matches
        ('364589006:85548006=364589007', None, None),  # special characters can be handled
        ('364589006;364589007', 249221003, '249221003'),  # procedure code not matching, finding code matching
        (None, None, None),  # snomed master code is None, code is not used
        ('364589006;364589007', 123456, None),  # None of the procedure code and finding code matching, code not used
        ('85548006', 249221003, '85548006'),  # procedure code take precedence
    ])
    def test_genital_tract_traumatic_lesion(procedure_code, finding_code, expected):
        record = setup_structured_model(
            CareActivityLabourAndDelivery,
            MasterSnomedCTProcedureCode=procedure_code,
            MasterSnomedCTFindingCode=finding_code
        )

        assert_derivation_result(record, 'GenitalTractTraumaticLesion', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        ('1650200', date(2019, 1, 1), None),  # snomed_code not match
        ('77176002', date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_procedure_term(snomed_code: str, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivityLabourAndDelivery,
            root,
            MasterSnomedCTProcedureCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTProcedureTerm', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_finding_term(snomed_code: int, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivityLabourAndDelivery,
            root,
            MasterSnomedCTFindingCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingTerm', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_obs_term(snomed_code: int, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivityLabourAndDelivery,
            root,
            MasterSnomedCTObsCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTObsTerm', expected)


class TestCareActivityBabySnomedDerivations(object):
    """ Unit tests for SNOMED mappings / derivations for CareActivityBaby """

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('06', '27113001', None, '27113001'),  # original was SNOMED
        ('06', '27113001;27113001', None, '27113001;27113001'),  # original was SNOMED encoded list
        ('06', None, None, None),  # original was SNOMED, code is missing

        ('04', None, None, None),  # original was not SNOMED, code was missing
        ('04', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('04', '1234', 27113001, '27113001'),  # original was not SNOMED, code was mapped as int

        ('05', None, None, None),  # original was not SNOMED, code was missing
        ('05', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('05', '1234', 27113001, '27113001'),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_procedure_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivityBaby,
            ProcedureScheme=original_scheme,
            ProcedureCode=original_code,
            MapSnomedCTProcedureCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTProcedureCode', expected)

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('04', '27113001', None, 27113001),  # original was SNOMED
        ('04', None, None, None),  # original was SNOMED, code is missing

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('03', None, None, None),  # original was not SNOMED, code was missing
        ('03', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('03', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_finding_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivityBaby,
            FindingScheme=original_scheme,
            FindingCode=original_code,
            MapSnomedCTFindingCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingCode', expected)

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('03', '27113001', None, 27113001),  # original was SNOMED
        ('03', None, None, None),  # original was SNOMED, code is missing

        ('01', None, None, None),  # original was not SNOMED, code was missing
        ('01', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('01', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_obs_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            CareActivityBaby,
            ObsScheme=original_scheme,
            ObsCode=original_code,
            MapSnomedCTObsCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTObsCode', expected)

    @staticmethod
    @pytest.mark.parametrize('master_code, source_code, expected', [
        (169909004, '305', '305'),  # master code is matching , source code used
        (None, None,  None),  # master code is None, source code is not used
        (169909005, '305',  None),  # master code is different , source code not used
    ])
    def test_apgar_score_code(master_code, source_code, expected):
        record = setup_structured_model(
            CareActivityBaby,
            MasterSnomedCTObsCode=master_code,
            ObsValue=source_code
        )

        assert_derivation_result(record, 'ApgarScore', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        ('1650200', date(2019, 1, 1), None),  # snomed_code not match
        ('77176002', date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_procedure_term(snomed_code: str, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivityBaby,
            root,
            MasterSnomedCTProcedureCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTProcedureTerm', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_finding_term(snomed_code: int, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivityBaby,
            root,
            MasterSnomedCTFindingCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingTerm', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_obs_term(snomed_code: int, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            CareActivityBaby,
            root,
            MasterSnomedCTObsCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTObsTerm', expected)


class TestAnonFindingsSnomedDerivations(object):
    """ Unit tests for SNOMED mappings / derivations for AnonFindings """
    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('04', '27113001', None, 27113001),  # original was SNOMED
        ('04', None, None, None),  # original was SNOMED, code is missing

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('03', None, None, None),  # original was not SNOMED, code was missing
        ('03', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('03', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_finding_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            AnonFindings,
            FindingScheme=original_scheme,
            FindingCode=original_code,
            MapSnomedCTFindingCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingCode', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
        (77176002, None, None),  # snomed_code is known,  but no point in time
    ])
    def test_derive_master_snomed_ct_finding_term(snomed_code: int, point_in_time: date, expected: str):
        record = setup_structured_model(
            AnonFindings,
            ClinInterDate=point_in_time,
            MasterSnomedCTFindingCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingTerm', expected)


class TestFindingAndObservationMotherSnomedDerivations(object):
    """ Unit tests for SNOMED mappings / derivations for FindingAndObservationMother """

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('04', '27113001', None, 27113001),  # original was SNOMED
        ('04', None, None, None),  # original was SNOMED, code is missing

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('03', None, None, None),  # original was not SNOMED, code was missing
        ('03', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('03', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_finding_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            FindingAndObservationMother,
            FindingScheme=original_scheme,
            FindingCode=original_code,
            MapSnomedCTFindingCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingCode', expected)

    @staticmethod
    @pytest.mark.parametrize('original_scheme, original_code, mapped_code, expected', [
        ('03', '27113001', None, 27113001),  # original was SNOMED
        ('03', None, None, None),  # original was SNOMED, code is missing

        ('01', None, None, None),  # original was not SNOMED, code was missing
        ('01', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('01', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        ('02', None, None, None),  # original was not SNOMED, code was missing
        ('02', '1234', None, None),  # original was not SNOMED, code is not mapped
        ('02', '1234', 27113001, 27113001),  # original was not SNOMED, code was mapped as int

        (None, '27113001', None, None),  # original scheme was not specified
    ])
    def test_master_snomed_ct_obs_code(original_scheme, original_code, mapped_code, expected):
        record = setup_structured_model(
            FindingAndObservationMother,
            ObsScheme=original_scheme,
            ObsCode=original_code,
            MapSnomedCTObsCode=mapped_code
        )

        assert_derivation_result(record, 'MasterSnomedCTObsCode', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_finding_term(snomed_code: int, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            FindingAndObservationMother,
            root,
            MasterSnomedCTFindingCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTFindingTerm', expected)

    @staticmethod
    @pytest.mark.parametrize("snomed_code, point_in_time, expected", [
        (None, None, None),  # all input parameters missing
        (1650200, date(2019, 1, 1), None),  # snomed_code not match
        (77176002, date(2019, 1, 1), 'Smoker (finding)'),  # snomed_code match
    ])
    def test_derive_master_snomed_ct_obs_term(snomed_code: int, point_in_time: date, expected: str):
        root = setup_structured_model(
            PregnancyAndBookingDetails,
            AntenatalAppDate=point_in_time
        )

        record = setup_structured_model(
            FindingAndObservationMother,
            root,
            MasterSnomedCTObsCode=snomed_code
        )

        assert_derivation_result(record, 'MasterSnomedCTObsTerm', expected)
