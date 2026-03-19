from testdata.ref_data.providers import ODSProvider
from dsp.common.expressions import ModelExpression, FirstRanked
from dsp.shared.constants import ReasonsForAccess

__all__ = \
    [
        "ReasonsForAccessExpression",
        "CarePersonnelQualificationReasonsForAccessExpression"
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
            (ReasonsForAccess.ResidenceCcg, orgs.normalise_org_code(patient.OrgIDResidenceResp, rp_start_date)),
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


