from testdata.ref_data.providers import ODSProvider
from dsp.common.expressions import ModelExpression
from dsp.shared.constants import ReasonsForAccess

__all__ = \
    [
        "AnonymousTableReasonsForAccessExpression",
        "AnonymousStaffDetailsTableReasonsForAccessExpression",
        "ReasonsForAccessExpression"
    ]


class ReasonsForAccessExpression(ModelExpression):

    def __init__(self, mpi_expression: ModelExpression):
        self.mpi = mpi_expression

    def resolve_value(self, model):
        orgs = ODSProvider()

        mpi = self.mpi.resolve_value(model)
        header = mpi.Header
        rp_start_date = header.RP_StartDate
        rp_end_date = header.RP_EndDate
        result = set()

        for ref in mpi.Referrals:

            commissioners = {
                orgs.normalise_org_code(ref.OrgID_Commissioner, rp_start_date)
            }

            if rp_end_date and rp_start_date:
                commissioners |= set(
                    orgs.normalise_org_code(cc.OrgID_Commissioner, rp_start_date)
                    for cc in ref.CareContacts
                    if cc.Contact_Date and rp_start_date <= cc.Contact_Date <= rp_end_date
                )

            rfas = [
                (ReasonsForAccess.SenderIdentity, orgs.normalise_org_code(header.OrgID_Submitter, rp_start_date)),
                (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgID_Provider, rp_start_date)),
                (ReasonsForAccess.ResidenceCcg, orgs.normalise_org_code(mpi.OrgID_Responsible, rp_start_date)),
                (ReasonsForAccess.ResidenceCcgFromPatientPostcode,
                 orgs.normalise_org_code(mpi.OrgID_CCG_Residence, rp_start_date)),
                *((ReasonsForAccess.CommissionerCode, com) for com in commissioners if com)
            ]
            if rp_start_date and mpi.GPPracticeRegistrations:
                gps = [gp for gp in mpi.GPPracticeRegistrations
                       if (gp.GPRegistration_EndDate or rp_start_date) >= rp_start_date and
                       gp.GPRegistration_StartDate is not None]

                gps.sort(key=lambda gp: gp.GPRegistration_StartDate)

                if gps:
                    current_gp = gps[-1]
                    rfas.append(
                        (
                            ReasonsForAccess.ResponsibleCcgFromGeneralPractice,
                            orgs.normalise_org_code(current_gp.OrgID_CCG_GP, rp_start_date)
                        )
                    )

            result.update(set({
                ReasonsForAccess.format(rfa_type, rfa_value) for rfa_type, rfa_value in rfas if rfa_value
            }))
            sorted(result)
        return list(result)


class AnonymousTableReasonsForAccessExpression(ModelExpression):

    def __init__(
            self,
            anon_commissioner_expression: ModelExpression,
            anon_date_expression: ModelExpression
    ):
        self.commissioner = anon_commissioner_expression
        self.activity_date = anon_date_expression

    def resolve_value(self, model):
        orgs = ODSProvider()

        header = model.Header
        activity_date = self.activity_date.resolve_value(model)
        commissioner = self.commissioner.resolve_value(model)

        rp_start_date = header.RP_StartDate
        rp_end_date = header.RP_EndDate

        rfas = [
            (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgID_Provider, rp_start_date)),
            (ReasonsForAccess.SenderIdentity, orgs.normalise_org_code(header.OrgID_Submitter, rp_start_date))
        ]

        if activity_date and rp_start_date and rp_end_date and rp_start_date <= activity_date <= rp_end_date:
            rfas.append(
                (ReasonsForAccess.CommissionerCode, orgs.normalise_org_code(commissioner, activity_date))
            )

        result = [
            ReasonsForAccess.format(rfa_type, rfa_value) for rfa_type, rfa_value in rfas if rfa_value
        ]

        result.sort()

        return result


class AnonymousStaffDetailsTableReasonsForAccessExpression(ModelExpression):

    def resolve_value(self, model):
        orgs = ODSProvider()
        header = model.Header
        rp_start_date = header.RP_StartDate

        rfas = [
            (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgID_Provider, rp_start_date)),
        ]

        result = [
            ReasonsForAccess.format(rfa_type, rfa_value) for rfa_type, rfa_value in rfas if rfa_value
        ]

        result.sort()

        return result
