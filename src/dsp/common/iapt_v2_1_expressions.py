
from testdata.ref_data.providers import ODSProvider
from dsp.common.expressions import ModelExpression
from dsp.shared.constants import ReasonsForAccess
from dsp.udfs.organisation import find_related_org_with_role, is_valid_ccg, region_code_from_sub_icb_code
from dsp.model.ods_record_codes import ODSRoleScope, ODSRelType

__all__ = \
    [
        "ReasonsForAccessExpression",
        "CarePersonnelQualificationReasonsForAccessExpression",
        "DeriveCommissioningRegion"
    ]


class ReasonsForAccessExpression(ModelExpression):
    def __init__(self, referral_expression: ModelExpression):
        self.referral = referral_expression

    def resolve_value(self, model):
        orgs = ODSProvider()

        referral = self.referral.resolve_value(model)
        header = referral.Header
        rp_start_date = header.ReportingPeriodStartDate
        rp_end_date = header.ReportingPeriodEndDate

        patient = referral.Patient

        commissioners = {
            orgs.normalise_org_code(referral.OrgIDComm, rp_start_date)
        }

        if rp_start_date and rp_end_date:
            commissioners = commissioners | set(
                orgs.normalise_org_code(cc.OrgIDComm, rp_start_date)
                for cc in referral.CareContacts
                if cc.CareContDate and rp_start_date <= cc.CareContDate <= rp_end_date
            )

        rfas = [
            (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgIDProv, rp_start_date)),
            (ReasonsForAccess.SenderIdentity, orgs.normalise_org_code(header.OrgIDSubmit, rp_start_date)),
            (ReasonsForAccess.ResidenceCcgFromPatientPostcode,
             orgs.normalise_org_code(patient.OrgID_CCG_Residence, rp_start_date)),
        ]

        if rp_start_date and patient.GPPracticeRegistrations:
            gps = [
                gp for gp in patient.GPPracticeRegistrations
                if (gp.EndDateGMPRegistration or rp_start_date) >= rp_start_date
                   and gp.StartDateGMPRegistration is not None
            ]
            gps.sort(key=lambda gp: gp.StartDateGMPRegistration)

            if gps:
                current_gp = gps[-1]
                rfas.append(
                    (
                        ReasonsForAccess.ResponsibleCcgFromGeneralPractice,
                        orgs.normalise_org_code(current_gp.OrgID_CCG_GP, rp_start_date)
                    )
                )

        rfas.extend(
            (ReasonsForAccess.CommissionerCode, com) for com in commissioners if com
        )

        result = [
            ReasonsForAccess.format(rfa_type, rfa_value) for rfa_type, rfa_value in rfas if rfa_value
        ]

        result.sort()

        return result


class CarePersonnelQualificationReasonsForAccessExpression(ModelExpression):

    def resolve_value(self, model):
        orgs = ODSProvider()

        header = model.Header
        rp_start_date = header.ReportingPeriodStartDate

        rfas = [
            (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgIDProv, rp_start_date)),
        ]

        result = [
            ReasonsForAccess.format(rfa_type, rfa_value) for rfa_type, rfa_value in rfas if rfa_value
        ]

        result.sort()

        return result


class DeriveCommissioningRegion(ModelExpression):

    def __init__(self, org_code_expr, point_in_time_expr):
        self.org_code_expr = org_code_expr
        self.point_in_time_expr = point_in_time_expr

    def resolve_value(self, model):

        if is_valid_ccg(self.org_code_expr.resolve_value(model),
                        self.point_in_time_expr.resolve_value(model)):

            commissioning_region = region_code_from_sub_icb_code(
                code=self.org_code_expr.resolve_value(model),
                point_in_time=self.point_in_time_expr.resolve_value(model),
            )

            if commissioning_region is not None:
                return commissioning_region

            region_org = find_related_org_with_role(
                code=self.org_code_expr.resolve_value(model),
                point_in_time=self.point_in_time_expr.resolve_value(model),
                rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF,
                role_ids=[ODSRoleScope.REGION_GEOGRAPHY])

            if region_org is not None:
                commissioning_geography = find_related_org_with_role(
                    region_org,
                    point_in_time=self.point_in_time_expr.resolve_value(model),
                    rel_type=ODSRelType.IS_A_SUB_DIVISION_OF,
                    role_ids=[ODSRoleScope.NHS_ENGLAND_COMMISSIONING_REGION])

                if commissioning_geography:
                    return commissioning_geography


