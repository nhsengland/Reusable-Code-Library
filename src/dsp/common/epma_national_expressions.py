import datetime
from typing import Optional, Union, List

from dsp.validations.validation_hardcoded_lookup_values import (
    IMDVersion,
)
from dsp.model.imd_record import IMDRecordPaths
from dsp.model.ods_record_codes import ODSRoleScope, ODSRelType
from testdata.ref_data import providers
from dsp.common.expressions import (
    ModelExpression,
    MultiCase,
    Literal,
    AgeAtDate,
)
from dsp.udfs.organisation import (
    find_related_org_with_role,
    get_org_role_types,
    organisation_exists,
    organisation_exists_anytime
)


class ReportingPeriodStartMonth(ModelExpression):
    """
    Given the reporting period start date (as a date object) returns an integer
    value in the form yyyyMM.
    """

    def __init__(self, reporting_period_start_field: ModelExpression) -> None:
        self.reporting_period_start_field = reporting_period_start_field

    def resolve_value(self, model) -> Optional[int]:
        __rdt_value = self.reporting_period_start_field.resolve_value(model)
        if not __rdt_value:
            return None

        elif not isinstance(__rdt_value, (datetime.date, datetime.datetime)):
            return None

        return int(__rdt_value.strftime("%Y%m"))


class ValidateTreatmentSiteOdsCode(ModelExpression):
    """
    Validates a given OrganisationSiteIdentifierOfTreatment according to the
    ePMA prescription specification. Codes are validated with the
    following logic...
        01 = A valid ODS Code where the type is "NHS Trust Site"
        02 = A valid ODS Code where the type is not "NHS Trust Site"
        03 = An inactive ODS code - present in REF data but invalid at point in time.
        04 = The ODS code could not be found in the ODS reference data
        05 = No Code supplied
    """

    def __init__(
        self,
        orgSiteIdentifier_of_treatment: ModelExpression,
        point_in_time: ModelExpression,
    ) -> None:
        self.orgSiteIdentifier_of_treatment = orgSiteIdentifier_of_treatment
        self.point_in_time = point_in_time

    def resolve_value(self, model) -> Optional[str]:
        __osiot_value = self.orgSiteIdentifier_of_treatment.resolve_value(model)
        __point_in_time = self.point_in_time.resolve_value(model)

        if __osiot_value is None:
            return "05"

        if not organisation_exists_anytime(__osiot_value):
            return "04"

        if not organisation_exists(__osiot_value, __point_in_time):
            return "03"

        try:
            lookup_ods_role = get_org_role_types(__osiot_value, __point_in_time)[0]
        except IndexError:
            lookup_ods_role = None

        if lookup_ods_role != "RO198":
            return "02"
        else:
            return "01"


class TreatmentTrustODScode(ModelExpression):
    """
    Where a TREATMENT_SITE_ODS_CODE_CHECK is 01 or 02, then lookup the Trust
    level ODS code
    """

    def __init__(
        self,
        treatment_site_ods_code_check: ModelExpression,
        treatment_site_ods_code: ModelExpression,
        point_in_time: ModelExpression,
    ) -> None:
        self.treatment_site_ods_code_check = treatment_site_ods_code_check
        self.treatment_site_ods_code = treatment_site_ods_code
        self.point_in_time = point_in_time

    def resolve_value(self, model) -> Optional[str]:
        _tsocc: str = self.treatment_site_ods_code_check.resolve_value(model)

        if _tsocc == "02":
            return self.treatment_site_ods_code.resolve_value(model)
        elif _tsocc == "01":
            return find_related_org_with_role(
                code=self.treatment_site_ods_code.resolve_value(model),
                rel_type=ODSRelType.IS_OPERATED_BY,
                role_ids=[ODSRoleScope.NHS_TRUST],
                from_org_type=None,
                point_in_time=self.point_in_time.resolve_value(model)
            )
        else:
            return None


class CheckLegallyRestrictedDoB(ModelExpression):
    """
    Checks for legally restricted date of birth by checking the
    NHS_NUMBER_LEGALLY_RESTRICTED for literal value "Removed".
    """

    def __init__(
        self, nhs_no_legally_restricted: ModelExpression, patient_dob: ModelExpression
    ) -> None:
        self.nhs_no_legally_restricted = nhs_no_legally_restricted
        self.patient_dob = patient_dob

    def resolve_value(self, model) -> Optional[str]:
        __check_nhs_lr = self.nhs_no_legally_restricted.resolve_value(model)
        __patient_dob = self.patient_dob.resolve_value(model)

        if __check_nhs_lr == "Removed":
            return None
        else:
            return __patient_dob


class NHSNumberPdsCheck(ModelExpression):
    """
    Checks the NHS Number and assigns the following codes based on certain
    conditions...
        01 = Passed Cross-Check
        02 = Failed Cross-Check
        03 = PersonBirthDate is null
        04 = NHSNumber is null
        05 = Record contains legally restricted data
    """

    def __init__(
        self,
        matched_confidence_percentage: ModelExpression,
        person_birth_date: ModelExpression,
        nhs_number: ModelExpression,
        nhs_number_legally_restricted: ModelExpression,
    ) -> None:
        self.matched_confidence_percentage = matched_confidence_percentage
        self.person_birth_date = person_birth_date
        self.nhs_number = nhs_number
        self.nhs_number_legally_restricted = nhs_number_legally_restricted

    def resolve_value(self, model) -> str:
        __case_args_dict = {
            "matched_confidence_percentages": self.matched_confidence_percentage,
            "person_birth_date": self.person_birth_date,
            "nhs_number": self.nhs_number,
            "nhs_number_legally_restricted": self.nhs_number_legally_restricted,
        }

        return MultiCase(
            case_args_dict=__case_args_dict,
            branches=[
                (
                    lambda case_args: case_args["nhs_number_legally_restricted"]
                    == ("Removed"),
                    Literal("05"),
                ),
                (lambda case_args: case_args["nhs_number"] is None, Literal("04")),
                (
                    lambda case_args: case_args["person_birth_date"] is None,
                    Literal("03"),
                ),
                (
                    lambda case_args: case_args["matched_confidence_percentages"]
                    is None,
                    Literal("02"),
                ),
            ],
            default=Literal("01"),
        ).resolve_value(model)


class NotSensitiveAndPDSCheckIsOne(ModelExpression):
    """
    Checks that a given record is not sensitive and PDS Check is 01.
    """

    def __init__(
        self,
        nhs_number_legally_restricted: ModelExpression,
        nhs_number_pds_check: ModelExpression
    ) -> None:
        self.nhs_number_legally_restricted = nhs_number_legally_restricted
        self.nhs_number_pds_check = nhs_number_pds_check

    def resolve_value(self, model) -> bool:
        __restricted = self.nhs_number_legally_restricted.resolve_value(model)
        __pds_check = self.nhs_number_pds_check.resolve_value(model)

        return __restricted != "Removed" and __pds_check == "01"


class GetIMDDecileEPMA(ModelExpression):
    """
    Looks up the IMD Decile value based on a given LSOA
    """

    def __init__(
        self,
        lsoa_expr: ModelExpression,
        point_in_time_expr: ModelExpression,
        imd_year: ModelExpression,
        alt_point_in_time_expr: ModelExpression = Literal(None),
        lsoa_year: int = 2011,
    ):
        self.lsoa = lsoa_expr
        self.point_in_time = point_in_time_expr
        self.alt_point_in_time = alt_point_in_time_expr
        self.imd_year = imd_year
        self.lsoa_year = lsoa_year

    def resolve_value(self, model) -> Union[str, int, None]:
        __case_args_dict = {
            "lsoa": self.lsoa,
            "point_in_time": self.point_in_time,
            "alt_point_in_time": self.alt_point_in_time,
            "imd_year": self.imd_year,
        }
        __provider = providers.IMDProvider()

        return MultiCase(
            case_args_dict=__case_args_dict,
            branches=[
                (
                    lambda case_args: case_args["lsoa"] is None
                    or case_args["imd_year"] is None,
                    Literal(None),
                ),
                (
                    lambda case_args: case_args["point_in_time"] is None
                    and case_args["alt_point_in_time"] is not None,
                    Literal(
                        __provider.value_at(
                            lsoa=self.lsoa.resolve_value(model),
                            value_type=IMDRecordPaths.Decile,
                            point_in_time=self.alt_point_in_time.resolve_value(model),
                            imd_year=self.imd_year.resolve_value(model),
                            lsoa_year=self.lsoa_year,
                        )
                    ),
                ),
            ],
            default=Literal(
                __provider.value_at(
                    lsoa=self.lsoa.resolve_value(model),
                    value_type=IMDRecordPaths.Decile,
                    point_in_time=self.point_in_time.resolve_value(model),
                    imd_year=self.imd_year.resolve_value(model),
                    lsoa_year=self.lsoa_year,
                )
            ),
        ).resolve_value(model)


class GetIMDDecileYearEPMA(ModelExpression):
    """
    Derives the lowest possible IMD_YEAR for a given medication date(s). Option
    to provide a alternative date if the intial given date is None.
    """

    def __init__(
        self,
        lsoa: ModelExpression,
        date_used_to_derive_imd_decile: ModelExpression,
        alt_date_used_to_derive_imd_decile: ModelExpression = Literal(None),
    ) -> None:
        self.date_used_to_derive_imd_decile = date_used_to_derive_imd_decile
        self.alt_date_used_to_derive_imd_decile = alt_date_used_to_derive_imd_decile
        self.lsoa = lsoa

    def resolve_value(self, model) -> Optional[str]:
        __lsoa = self.lsoa.resolve_value(model)
        __pit = self.date_used_to_derive_imd_decile.resolve_value(model)
        __alt_pit = self.alt_date_used_to_derive_imd_decile.resolve_value(model)

        if __pit is None and __alt_pit is not None:
            __pit = __alt_pit

        __imd_years = sorted(IMDVersion.IMD_YEARS)
        __provider = providers.IMDProvider()

        for y in __imd_years:
            if (
                __provider.value_at(__lsoa, IMDRecordPaths.Decile, __pit, y, 2011)
                is not None
            ):
                return str(y)


class AgeAtDateEPMA(ModelExpression):
    """
    Calculates the Patients Age based on Date of Birth. Also checks that the
    record isn't sensitive and that the PDS check is 01 (successful cross-check)
    """

    def __init__(
        self,
        date_of_birth: ModelExpression,
        medication_date: ModelExpression,
        backup_medication_date: ModelExpression = Literal(None),
    ) -> None:
        self.date_of_birth = date_of_birth
        self.medication_date = medication_date
        self.backup_medication_date = backup_medication_date

    def resolve_value(self, model) -> int:
        __case_args_dict = {
            "date_of_birth": self.date_of_birth,
            "medication_date": self.medication_date,
            "backup_medication_date": self.backup_medication_date,
        }

        return MultiCase(
            case_args_dict=__case_args_dict,
            branches=[
                (lambda case_args: case_args["date_of_birth"] is None, Literal(None)),
                (
                    lambda case_args: case_args["medication_date"] is None
                    and case_args["backup_medication_date"] is not None,
                    AgeAtDate(self.date_of_birth, self.backup_medication_date),
                ),
            ],
            default=AgeAtDate(self.date_of_birth, self.medication_date),
        ).resolve_value(model)


class DeriveStandardFiltered(ModelExpression):
    """
    Checks a relevant Filtered check column to see if the value is valid for
    copying into the filtered column
    """

    def __init__(
        self,
        column_to_check: ModelExpression,
        non_filtered_value: ModelExpression,
        is_snomed_ct_filtered: bool = False,
    ):
        self.column_to_check = column_to_check
        self.non_filtererd_value = non_filtered_value
        self.is_snomed_ct_filtered = is_snomed_ct_filtered

    def resolve_value(self, model) -> Union[str, None]:
        __col_to_check = self.column_to_check.resolve_value(model)

        if self.is_snomed_ct_filtered:
            __valid_vals = ["01", "02"]
        else:
            __valid_vals = ["03", "04"]

        if __col_to_check in __valid_vals:
            __non_filtered_value = self.non_filtererd_value.resolve_value(model)
            return str(__non_filtered_value)
        else:
            return None


class DeriveDoseUnitFiltered(ModelExpression):
    """
    Checks a Dose Unit Check column to see if a dosage unit is valid for copying
    """

    def __init__(
        self,
        input_quantity_unit: ModelExpression,
        input_quantity_unit_check: ModelExpression,
    ):
        self.input_quantity_unit = input_quantity_unit
        self.input_quantity_unit_check = input_quantity_unit_check

    def resolve_value(self, model) -> Union[str, None]:
        __quantity_check = self.input_quantity_unit_check.resolve_value(model)

        if __quantity_check in ["01", "03", "04"]:
            __quantity_unit = self.input_quantity_unit.resolve_value(model)
            return __quantity_unit
        else:
            return None


class DeriveWhenFiltered(ModelExpression):
    """
    Checks the WHEN Filtered Check column and generates an array of filtered WHEN values
    """

    def __init__(
        self, when_values: ModelExpression, when_check_values: ModelExpression
    ):
        self.when_values = when_values
        self.when_check_values = when_check_values

    def resolve_value(self, model) -> List[Union[str, None]]:
        __when_values = self.when_values.resolve_value(model)
        __when_check_values = self.when_check_values.resolve_value(model)
        __when_filtered_values = []

        for i, v in enumerate(__when_values):
            if __when_check_values[i] in ["02", "04"]:
                __when_filtered_values.append(v)
            else:
                __when_filtered_values.append(None)
        return __when_filtered_values
