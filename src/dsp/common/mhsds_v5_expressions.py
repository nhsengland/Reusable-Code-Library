from datetime import datetime
from typing import List

from dateutil.relativedelta import relativedelta

from testdata.ref_data.providers import ODSProvider
from dsp.common.constants import (
    mh_ethnic_category_codes,
    MH_SERVICE_TEAM_TYPE,
    SPECIALISED_MH_SERVICE_TEAM_NAME,
    NATIONAL_CODES
)
from dsp.common.expressions import ModelExpression, MinFilterResolved
from dsp.udfs import ethnic_category_group, ethnic_category_description
from dsp.shared.constants import ReasonsForAccess

__all__ = [
    "FirstAttendedContactInRPDate",
    "EthnicityHigher",
    "EthnicityLow",
    "DMSEthnicity",
    "NHSDLegalStatusExpression",
    "ReasonsForAccessExpression",
    "AnonymousTableReasonsForAccessExpression",
    "BedTypeAdultDischargeExpression",
    "BedTypeAdultEndRPExpression",
    "CommIdsForTable",
    "ServiceTeamType",
    "SpecialisedMHService",
    "WardStayBedType",
    "NationalCodeDescription"
]


class FirstAttendedContactInRPDate(ModelExpression):

    def __init__(self, care_cont_expr: ModelExpression, 
    rp_start_expr: ModelExpression, rp_end_expr: ModelExpression):
        self.care_cont_expr = care_cont_expr
        self.rp_start_expr = rp_start_expr
        self.rp_end_expr = rp_end_expr

    def resolve_value(self, model):
        rp_start = self.rp_start_expr.resolve_value(model)
        rp_end = self.rp_end_expr.resolve_value(model)
        return MinFilterResolved(
            self.care_cont_expr, 
            lambda x: rp_start <= x.CareContDate <= rp_end and x.AttendOrDNACode in ["5", "6"],
            lambda x: x.CareContDate
        ).resolve_value(model)


class EthnicityHigher(ModelExpression):
    def __init__(self, ethnic_category_expr: ModelExpression):
        self.ethnic_category_expr = ethnic_category_expr

    def resolve_value(self, model):
        ethnic_category = self.ethnic_category_expr.resolve_value(model)
        try:
            ethnicity_higher = ethnic_category_group(ethnic_category, z_as_none=True)
        except ValueError:
            ethnicity_higher = None
        if ethnicity_higher:
            return ethnicity_higher
        return "Unknown"


class EthnicityLow(ModelExpression):
    def __init__(self, ethnic_category_expr: ModelExpression):
        self.ethnic_category_expr = ethnic_category_expr

    def resolve_value(self, model):
        ethnic_category = self.ethnic_category_expr.resolve_value(model)

        try:
            ethnicity_low = ethnic_category_description(ethnic_category)
        except ValueError:
            ethnicity_low = None
        if ethnicity_low:
            return ethnicity_low
        return "Unknown"


class DMSEthnicity(ModelExpression):

    def __init__(self, ethnic_category_expr: ModelExpression):
        self.ethnic_category_expr = ethnic_category_expr

    def resolve_value(self, model):
        """from the submitted EthnicCategory value return cleansed value

        Args:
            model (MasterPatientIndex)

        Returns:
            str: cleansed val
        """

        ethnic_category = self.ethnic_category_expr.resolve_value(model)

        if ethnic_category is None or not ethnic_category.strip():
            return None

        if ethnic_category[0] in mh_ethnic_category_codes:
            return ethnic_category[0]

        if ethnic_category == '99':
            return ethnic_category

        return '-1'


def _ward_stays_for_spell(ward_stay_list: List) -> dict:
    ward_stay_dict = {}  # type: dict
    for ward_stay in ward_stay_list:
        hpsn = ward_stay.HospProvSpellNum
        ward_start_date = ward_stay.StartDateWardStay
        ward_stay_date_list = ward_stay_dict.get(hpsn)  # type:List[datetime.datetime]
        if not ward_stay_date_list:
            ward_stay_dict.update({hpsn: [ward_start_date]})
        else:
            ward_stay_dict.update({hpsn: ward_stay_date_list.append(ward_start_date)})

    return ward_stay_dict


def _matched_careprof_wardstay(ward_stay_dict: dict, care_prof_list: List) -> List:
    matched_item_list = []
    for assigned_care_prof in care_prof_list:
        ward_stay_date_list = ward_stay_dict.get(assigned_care_prof.HospProvSpellNum)
        if ward_stay_date_list and assigned_care_prof.StartDateAssCareProf in ward_stay_date_list:
            matched_item_list.append(assigned_care_prof)

    return matched_item_list


def _get_matched_mhs503_list(assigned_care_prof_list, end_date):
    if assigned_care_prof_list and end_date:
        return [
            x for x in assigned_care_prof_list if (
                x.EndDateAssCareProf is None or
                x.EndDateAssCareProf >= end_date
            )
        ]

    return []


def _get_matched_mhs102_records(st_refer_to_list, rp_end_date):
    if st_refer_to_list:
        st_refer_to_list = [
            x for x in st_refer_to_list if (
                    (x.ReferClosureDate is None or x.ReferClosureDate >= rp_end_date)
                    or (x.ReferRejectionDate is None or x.ReferRejectionDate >= rp_end_date)
            )]

        return st_refer_to_list


def _is_valid_ws_end_rp_amh(intend_clin_code_mh: str, assigned_care_prof_list: List,
                            ward_type: str, ward_age: str) -> bool:
    if ward_type in ['01', '02', '05'] or intend_clin_code_mh in ['61', '62', '63']:
        return False

    for assigned_care_prof in assigned_care_prof_list:
        if assigned_care_prof.TreatFuncCodeMH in ['710', '711']:
            return False
    if ward_age in ['10', '11', '12']:
        return False

    return True


def _is_valid_ws_end_lda(intend_clin_code_mh: str, assigned_care_prof_list: List, ward_type: str) -> bool:
    if intend_clin_code_mh in ['61', '62', '63'] or ward_type == '05':
        return True

    for assigned_care_prof in assigned_care_prof_list:
        if assigned_care_prof.TreatFuncCodeMH == '711':
            return True

    return False


def _is_valid_ws_end_cyp(assigned_care_prof_list: List, ward_type: str, ward_age: str) -> bool:
    for assigned_care_prof in assigned_care_prof_list:
        if assigned_care_prof.TreatFuncCodeMH == '711':
            return True

    return ward_type in ['01', '02'] or ward_age in ['10', '11', '12']


def _get_matched_treat_func_code(care_prof_list: List, hosp_id):
    for assigned_care_prof in care_prof_list:
        if hosp_id == assigned_care_prof.HospProvSpellID:
            return assigned_care_prof.TreatFuncCodeMH
    return 'NULL'


class NHSDLegalStatusExpression(ModelExpression):

    def __init__(self, legal_status_code_expr: ModelExpression):
        self.legal_status_code = legal_status_code_expr

    def resolve_value(self, model):

        legal_status_code = self.legal_status_code.resolve_value(model)

        if legal_status_code is None or not legal_status_code.strip():
            return None

        if legal_status_code.zfill(2) in [
            '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '12', '13', '14',
            '15', '16', '17', '18', '19', '20', '31', '32', '35', '36', '37', '38', '98',
            '99'
        ]:

            return legal_status_code.zfill(2)

        return '-1'


class ReasonsForAccessExpression(ModelExpression):

    def __init__(self, referral_expression: ModelExpression):
        self.referral = referral_expression

    def resolve_value(self, model):
        orgs = ODSProvider()

        referral = self.referral.resolve_value(model)  # type: Referral
        header = referral.Header
        rp_start_date = header.ReportingPeriodStartDate
        rp_end_date = header.ReportingPeriodEndDate

        patient = referral.Patient

        commissioners = {
            orgs.normalise_org_code(referral.OrgIDComm, rp_start_date)
        }

        if rp_start_date and rp_end_date:
            commissioners = commissioners | set(
                orgs.normalise_org_code(hpsc.OrgIDComm, rp_start_date)
                for hps in referral.HospitalProviderSpells
                for hpsc in hps.HospitalProviderSpellCo
                if hpsc.StartDateOrgCodeComm and hpsc.StartDateOrgCodeComm <= rp_end_date
                and (hpsc.EndDateOrgCodeComm or rp_end_date) >= rp_start_date

            )

            commissioners = commissioners | set(
                orgs.normalise_org_code(cc.OrgIDComm, rp_start_date)
                for cc in referral.CareContacts
                if cc.CareContDate and rp_start_date <= cc.CareContDate <= rp_end_date
            )

            commissioners = commissioners | set(
                orgs.normalise_org_code(ia.OrgIDComm, rp_start_date)
                for ia in referral.IndirectActivities
                if ia.IndirectActDate and rp_start_date <= ia.IndirectActDate <= rp_end_date
            )

        rfas = [
            (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgIDProvider, rp_start_date)),
            (ReasonsForAccess.SenderIdentity, orgs.normalise_org_code(header.OrgIDSubmit, rp_start_date)),
            (ReasonsForAccess.ResidenceCcg, orgs.normalise_org_code(patient.OrgIDResidenceResp, rp_start_date)),
            (ReasonsForAccess.ResidenceCcgFromPatientPostcode, orgs.normalise_org_code(patient.OrgIDCCGRes, rp_start_date)),
        ]

        if rp_start_date and patient.GPs:
            # todo: this will only return the current gp if there is one .. ( should this be most recent ? )
            gps = [
                gp for gp in patient.GPs
                if (gp.EndDateGMPRegistration or rp_start_date) >= rp_start_date
                and gp.StartDateGMPRegistration is not None
            ]
            gps.sort(key=lambda gp: gp.StartDateGMPRegistration)

            if gps:
                current_gp = gps[-1]
                rfas.append(
                    (
                        ReasonsForAccess.ResponsibleCcgFromGeneralPractice,
                        orgs.normalise_org_code(current_gp.OrgIDCCGGPPractice, rp_start_date)
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

        rp_start_date = header.ReportingPeriodStartDate
        rp_end_date = header.ReportingPeriodEndDate

        rfas = [
            (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgIDProvider, rp_start_date)),
            (ReasonsForAccess.SenderIdentity, orgs.normalise_org_code(header.OrgIDSubmit, rp_start_date))
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


def _bed_type(ward_sec_level: str, intend_code_mh: str, treat_func_code_mh: str, ward_type: str):
    if ward_sec_level in ['1', '2', '3']:
        return 1
    if intend_code_mh == '53':
        return 3
    if treat_func_code_mh in ['724', '720']:
        return 1
    if ward_type == '06' and treat_func_code_mh == '715':
        return 3
    if treat_func_code_mh in ['715', '725', '727']:
        return 3
    if treat_func_code_mh in ['710', '712', '723']:
        return 2
    if ward_type in ['03', '06'] and intend_code_mh in ['51', '52']:
        return 2

    return 4


class BedTypeAdultEndRPExpression(ModelExpression):
    """
    Bed type of the ward stay at the end of the reporting period for adult mental health patients

    Logic as per DMS, will need modifying to account for TOS structure.

    CASE WHEN WardSecLevel IN ('1', '2', '3') THEN 1 --Specialist MH Services
        WHEN IntendClinCareIntenCodeMH = '53'  THEN 3 --Rehab and older adults organic
        WHEN TreatFuncCodeMH in ('724', '720') THEN 1 --Specialist MH Services
        WHEN WardType = '06' AND TreatFuncCodeMH = '715' THEN 3 --Rehab and older adults organic
        WHEN TreatFuncCodeMH IN ('715', '725', '727') THEN 3 --Rehab and older adults organic
        WHEN TreatFuncCodeMH IN ('710', '712', '723') THEN 2 --Adult Acute
        WHEN WardType IN ('03', '06') AND IntendClinCareIntenCodeMH IN ('51', '52') THEN 2 --Adult Acute
        ELSE 4 --Unknown
        END AS Bed_type
    where service_area = 'AMH'
    AND (EndDateWardStay IS NULL OR EndDateWardStay > period_end)
    AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf > period_end)
    """
    def __init__(self, rp_start_date: ModelExpression, rp_end_date: ModelExpression, care_prof_expr: ModelExpression,
                 end_date_ws: ModelExpression, ward_age_expr: ModelExpression, ward_type_expr: ModelExpression,
                 ward_sec_level_expr: ModelExpression, intend_clin_code_mh_expr: ModelExpression):
        self.rp_start_date = rp_start_date
        self.rp_end_date = rp_end_date
        self.care_prof_expr = care_prof_expr
        self.end_date_ws = end_date_ws
        self.ward_age_expr = ward_age_expr
        self.ward_type_expr = ward_type_expr
        self.ward_sec_level_expr = ward_sec_level_expr
        self.intend_clin_code_mh_expr = intend_clin_code_mh_expr

    def resolve_value(self, model):
        rp_end_dt = self.rp_end_date.resolve_value(model)
        end_date_ws = self.end_date_ws.resolve_value(model)

        if end_date_ws and end_date_ws <= rp_end_dt:
            return 4

        assigned_care_prof_list = self.care_prof_expr.resolve_value(model)
        matched_care_prof_records = _get_matched_mhs503_list(assigned_care_prof_list, rp_end_dt)

        if not matched_care_prof_records:
            return 4

        ward_age = self.ward_age_expr.resolve_value(model)
        ward_type = self.ward_type_expr.resolve_value(model)
        intend_clin_code_mh = self.intend_clin_code_mh_expr.resolve_value(model)

        amh_flag = _is_valid_ws_end_rp_amh(intend_clin_code_mh,
                                           matched_care_prof_records,
                                           ward_type,
                                           ward_age)

        if not amh_flag:
            return 4

        ward_sec_level = self.ward_sec_level_expr.resolve_value(model)

        for matched_care_prof in matched_care_prof_records:
            return _bed_type(
                ward_sec_level=ward_sec_level,
                ward_type=ward_type,
                intend_code_mh=intend_clin_code_mh,
                treat_func_code_mh=matched_care_prof.TreatFuncCodeMH
            )


def _is_valid_discharge_amh(ward_type: str, intend_cc_inten_code: str, ward_age: str,
                            assigned_care_prof_list: List) -> bool:
    if ward_type in ['01', '02', '05']:
        return False

    if intend_cc_inten_code in ['61', '62', '63']:
        return False

    for assigned_care_prof in assigned_care_prof_list:
        if assigned_care_prof.TreatFuncCodeMH in ['700', '711']:
            return False

    if ward_age in ['10', '11', '12']:
        return False

    if ward_age in ['13', '14', '15']:
        return True

    if intend_cc_inten_code in ['51', '52', '53']:
        return True

    for assigned_care_prof in assigned_care_prof_list:
        if assigned_care_prof.TreatFuncCodeMH in ['710', '712', '713', '715', '720', '721', '722', '723',
                                                  '724', '725', '726', '727']:
            return True

    if ward_type in ['03', '04', '06']:
        return True

    return True


class BedTypeAdultDischargeExpression(ModelExpression):
    """
    Bed type of the ward stay on discharge for adult mental health patients

    Logic as per DMS, will need modifying to account for TOS structure.

    CASE WHEN WardSecLevel IN ('1', '2', '3') THEN 1 --Specialist MH Services
        WHEN IntendClinCareIntenCodeMH = '53'  THEN 3 --Rehab and older adults organic
        WHEN TreatFuncCodeMH in ('724', '720') THEN 1 --Specialist MH Services
        WHEN WardType = '06' AND TreatFuncCodeMH = '715' THEN 3 --Rehab and older adults organic
        WHEN TreatFuncCodeMH IN ('715', '725', '727') THEN 3 --Rehab and older adults organic
        WHEN TreatFuncCodeMH IN ('710', '712', '723') THEN 2 --Adult Acute
        WHEN WardType IN ('03', '06') AND IntendClinCareIntenCodeMH IN ('51', '52') THEN 2 --Adult Acute
        ELSE 4 --Unknown
        END AS Bed_type
    where service_area = 'AMH'
    AND (EndDateWardStay BETWEEN period_start AND period_end)
    AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf >= EndDateWardStay)
    """
    def __init__(self, rp_start_date: ModelExpression, rp_end_date: ModelExpression, care_prof_expr: ModelExpression,
                 end_date_ws: ModelExpression, ward_age_expr: ModelExpression, ward_type_expr: ModelExpression,
                 ward_sec_level_expr: ModelExpression, intend_clin_code_mh_expr: ModelExpression):
        self.rp_start_date = rp_start_date
        self.rp_end_date = rp_end_date
        self.care_prof_expr = care_prof_expr
        self.end_date_ws = end_date_ws
        self.ward_age_expr = ward_age_expr
        self.ward_type_expr = ward_type_expr
        self.ward_sec_level_expr = ward_sec_level_expr
        self.intend_clin_code_mh_expr = intend_clin_code_mh_expr

    def resolve_value(self, model):
        rp_start_dt = self.rp_start_date.resolve_value(model)
        rp_end_dt = self.rp_end_date.resolve_value(model)
        end_date_ws = self.end_date_ws.resolve_value(model)

        if not end_date_ws or rp_start_dt > end_date_ws or end_date_ws > rp_end_dt:
            return 4

        assigned_care_prof_list = self.care_prof_expr.resolve_value(model)
        matched_care_prof_records = _get_matched_mhs503_list(assigned_care_prof_list, end_date_ws)

        if not matched_care_prof_records:
            return 4

        ward_type = self.ward_type_expr.resolve_value(model)
        intend_clin_code_mh = self.intend_clin_code_mh_expr.resolve_value(model)
        ward_age = self.ward_age_expr.resolve_value(model)

        amh_flag = _is_valid_discharge_amh(ward_type,
                                           intend_clin_code_mh,
                                           ward_age,
                                           matched_care_prof_records)

        if not amh_flag:
            return 4

        ward_sec_level = self.ward_sec_level_expr.resolve_value(model)

        for assigned_care_prof in matched_care_prof_records:
            return _bed_type(
                ward_sec_level=ward_sec_level,
                ward_type=ward_type,
                intend_code_mh=intend_clin_code_mh,
                treat_func_code_mh=assigned_care_prof.TreatFuncCodeMH
            )


class CommIdsForTable(ModelExpression):
    def __init__(self, referral_comm_id_expr: ModelExpression = None,
                 care_contacts_expr: ModelExpression = None,
                 indirect_activities_expr: ModelExpression = None,
                 hosp_prov_spells_expr: ModelExpression = None):
        self.referral_comm_id_expr = referral_comm_id_expr
        self.care_contacts_expr = care_contacts_expr
        self.indirect_activities_expr = indirect_activities_expr
        self.hosp_prov_spells_expr = hosp_prov_spells_expr

    def resolve_value(self, model):
        commissioner_ids = set()
        if self.referral_comm_id_expr:
            referral_commissioner_id = self.referral_comm_id_expr.resolve_value(model)
            commissioner_ids.add(referral_commissioner_id)

        if self.care_contacts_expr:
            care_contacts = self.care_contacts_expr.resolve_value(model)
            for care_contact in care_contacts:
                commissioner_ids.add(care_contact.OrgIDComm)

        if self.indirect_activities_expr:
            indirect_activites = self.indirect_activities_expr.resolve_value(model)
            for indirect_activity in indirect_activites:
                commissioner_ids.add(indirect_activity.OrgIDComm)

        if self.hosp_prov_spells_expr:
            hosp_prov_spells = self.hosp_prov_spells_expr.resolve_value(model)
            for hosp_prov_spell in hosp_prov_spells:
                for hospital_prov_comm in hosp_prov_spell.HospitalProviderSpellCommissioners:
                    commissioner_ids.add(hospital_prov_comm.OrgIDComm)

        commissioner_ids = commissioner_ids - {None}
        return list(commissioner_ids)


class ServiceTeamType(ModelExpression):
    def __init__(self, service_team_ref_expr: ModelExpression):
        self.service_team_ref_expr = service_team_ref_expr

    def resolve_value(self, model):
        service_team_ref = self.service_team_ref_expr.resolve_value(model)
        return MH_SERVICE_TEAM_TYPE.get(service_team_ref, None)


class SpecialisedMHService(ModelExpression):
    def __init__(self, service_code: ModelExpression):
        self.service_code = service_code

    def resolve_value(self, model):
        service_code = self.service_code.resolve_value(model)
        return SPECIALISED_MH_SERVICE_TEAM_NAME.get(service_code, None)


class WardStayBedType(ModelExpression):
    def __init__(self, hosp_id, ward_sec, clin_care_code, ward_type, care_prof_expr):
        self.hosp_id = hosp_id
        self.ward_sec = ward_sec
        self.clin_care_code = clin_care_code
        self.ward_type = ward_type
        self.care_prof_expr = care_prof_expr

    def resolve_value(self, model):
        hosp_id = self.hosp_id.resolve_value(model)
        ward_sec = self.ward_sec.resolve_value(model)
        clin_care_code = self.clin_care_code.resolve_value(model)
        ward_type = self.ward_type.resolve_value(model)
        assigned_care_prof_list = self.care_prof_expr.resolve_value(model)
        treat_func_code = _get_matched_treat_func_code(assigned_care_prof_list, hosp_id)

        if ward_sec in ('1', '2', '3'):
            return 1

        if clin_care_code == '53':
            return 3

        if treat_func_code in ('724', '720'):
            return 1

        if ward_type == '06' and treat_func_code == '715':
            return 3

        if treat_func_code in ('715', '725', '727'):
            return 3

        if treat_func_code in ('710', '712', '723'):
            return 2

        if ward_type in ('03', '06') and clin_care_code in ('51', '52'):
            return 2

        return 4

class NationalCodeDescription(ModelExpression):
    def __init__(self, national_code: ModelExpression):
        self.national_code = national_code

    def resolve_value(self, model):
        national_code = self.national_code.resolve_value(model)
        return NATIONAL_CODES.get(national_code, None)
