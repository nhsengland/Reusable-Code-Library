import abc
import collections
from decimal import Decimal
from typing import Type, List, TypeVar, Optional, Any

from dsp.datasets.models.msds_constants import SnomedCodeSystemConcept, UnitsOfMeasure
from testdata.ref_data.providers import ODSProvider
from dsp.common.expressions import ModelExpression, If, NotNull, AgeAtDate, Select, AddToDate, Literal, \
    GetAttr, FindFirst, as_type, is_non_negative_numeric
from dsp.common.model_accessor import ModelAccessor
from dsp.common.structured_model import DSPStructuredModel, SubmittedAttribute
from dsp.shared.constants import ReasonsForAccess

T = TypeVar('T')

__all__ = [
    "AnonymousTableReasonsForAccessExpression",
    "AnonymousStaffDetailsTableReasonsForAccessExpression",
    "CareActivityAttributeAtAppointmentDate",
    "CareActivityAttributeAtDeliveryDate",
    "CareActivityAttributeAtDischargeDateMatService",
    "CommIdsForMsdsTable",
    "DeriveAlcoholUnitsPerWeek",
    "DeriveBirthWeight",
    "DeriveCOMonReading",
    "DeriveCigarettesPerDay",
    "DerivePersonHeight",
    "DerivePersonWeight",
    "DeriveSmokingStatus",
    "GestationalAge",
    "ReasonsForAccessExpression",
    "SnomedProcedureCodeMatches",
    "DerivePostpartumBloodLoss"
]


class GestationalAge(ModelExpression):

    def __init__(self, date_at: ModelAccessor, pregnancy_model: Type[DSPStructuredModel]):
        self.date_at = date_at
        self.pregnancy_model = pregnancy_model

    def resolve_value(self, model):
        return If(
            condition=NotNull(Select(self.pregnancy_model.EDDAgreed)),
            then=AgeAtDate(
                date_for_age_expr=Select(self.date_at),
                date_of_birth_expr=AddToDate(
                    input_date_expr=Select(self.pregnancy_model.EDDAgreed),
                    days_to_add_expr=Literal(-(40 * 7)),
                ),
                time_unit=AgeAtDate.DAYS,
                ignore_time=True,
            ),
            otherwise=If(
                condition=NotNull(Select(self.pregnancy_model.LastMenstrualPeriodDate)),
                then=AgeAtDate(
                    date_for_age_expr=Select(self.date_at),
                    date_of_birth_expr=Select(self.pregnancy_model.LastMenstrualPeriodDate),
                    time_unit=AgeAtDate.DAYS,
                    ignore_time=True,
                ),
                otherwise=Literal(None),
            ),
        ).resolve_value(model)


class _CareActivityAttributeAtDate(ModelExpression):
    __metaclass__ = abc.ABCMeta

    def __init__(self, pregnancy_model: Type[DSPStructuredModel], care_activitiy_attribute_name: str):
        self.pregnancy_model = pregnancy_model
        self.attribute_name = care_activitiy_attribute_name

    @abc.abstractmethod
    def _get_dates(self) -> List[SubmittedAttribute]:
        """ Each pregnancy_model contains multiple care_contacts, each with a different date and set of values.
        Return a set of dates to search for the specified value at the first of these particular date e.g. Antenatal Appointment Date """
        return

    def flatten(self, input_list: List) -> List:
        for element in input_list:
            if isinstance(element, collections.Iterable) and not isinstance(element, (str, bytes)):
                yield from self.flatten(element)
            else:
                yield element

    def resolve_value(self, model):
        return GetAttr(
            FindFirst(
                iterable=GetAttr(
                    FindFirst(
                        iterable=Select(self.pregnancy_model.CareContacts),
                        predicate=lambda x, *y: x.CContactDate in self.flatten(y),
                        extra_args=[Select(date) for date in self._get_dates()],
                    ),
                    Literal('CareActivities'),
                ),
                predicate=lambda x: getattr(x, self.attribute_name) is not None,
            ),
            Literal(self.attribute_name)
        ).resolve_value(model)


class CareActivityAttributeAtAppointmentDate(_CareActivityAttributeAtDate):
    def _get_dates(self) -> List[SubmittedAttribute]:
        return [self.pregnancy_model.AntenatalAppDate]


class CareActivityAttributeAtDischargeDateMatService(_CareActivityAttributeAtDate):
    def _get_dates(self) -> List[SubmittedAttribute]:
        return [self.pregnancy_model.DischargeDateMatService]


class CareActivityAttributeAtDeliveryDate(_CareActivityAttributeAtDate):
    def _get_dates(self) -> List[SubmittedAttribute]:
        return [self.pregnancy_model.LaboursAndDeliveries.LabourOnsetDate,
                self.pregnancy_model.LaboursAndDeliveries.CaesareanDate]


class ReasonsForAccessExpression(ModelExpression):

    def __init__(self, pregnancy_and_booking_details_expression: ModelExpression):
        self.pregnancy_and_booking_details_expression = pregnancy_and_booking_details_expression

    def resolve_value(self, model):
        from dsp.datasets.models.msds import PregnancyAndBookingDetails

        orgs = ODSProvider()

        pregnancy_and_booking_details = self.pregnancy_and_booking_details_expression.resolve_value(
            model)  # type: PregnancyAndBookingDetails

        header = pregnancy_and_booking_details.Header
        mother = pregnancy_and_booking_details.Mother  # type: Mother

        rp_start_date = header.RPStartDate
        rp_end_date = header.RPEndDate

        commissioners = {
            orgs.normalise_org_code(pregnancy_and_booking_details.OrgIDComm, rp_start_date)
        }

        if rp_end_date and rp_start_date:
            commissioners |= set(
                orgs.normalise_org_code(hpsc.OrgIDComm, rp_start_date)
                for hps in pregnancy_and_booking_details.HospitalProviderSpells
                for hpsc in hps.HospitalSpellCommissioners
                if hpsc.StartDateOrgCodeComm and hpsc.StartDateOrgCodeComm <= rp_end_date
                and (hpsc.EndDateOrgCodeComm or rp_end_date) >= rp_start_date
            )
            commissioners |= set(
                orgs.normalise_org_code(cc.OrgIDComm, rp_start_date)
                for cc in pregnancy_and_booking_details.CareContacts
                if cc.CContactDate and rp_start_date <= cc.CContactDate <= rp_end_date
            )

        rfas = [
            (ReasonsForAccess.SenderIdentity, orgs.normalise_org_code(header.OrgIDSubmit, rp_start_date)),
            (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgIDProvider, rp_start_date)),
            (ReasonsForAccess.ResidenceCcg, orgs.normalise_org_code(mother.OrgIDResidenceResp, rp_start_date)),
            (ReasonsForAccess.ResidenceCcgFromPatientPostcode, orgs.normalise_org_code(mother.CCGResidenceMother, rp_start_date)),
            *((ReasonsForAccess.CommissionerCode, com) for com in commissioners if com)
        ]

        if rp_start_date and mother.GPPracticeRegistrations:
            # todo: this will only return the current gp if there is one .. ( should this be most recent ? )
            gps = [
                gp for gp in mother.GPPracticeRegistrations
                if (gp.EndDateGMPReg or rp_start_date) >= rp_start_date and gp.StartDateGMPReg is not None
            ]

            gps.sort(key=lambda gp: gp.StartDateGMPReg)

            if gps:
                current_gp = gps[-1]
                rfas.append(
                    (
                        ReasonsForAccess.ResponsibleCcgFromGeneralPractice,
                        orgs.normalise_org_code(current_gp.CCGResponsibilityMother, rp_start_date)
                    )
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

        rp_start_date = header.RPStartDate
        rp_end_date = header.RPEndDate

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

class AnonymousStaffDetailsTableReasonsForAccessExpression(ModelExpression):

    def resolve_value(self, model):
        orgs = ODSProvider()
        header = model.Header
        rp_start_date = header.RPStartDate

        rfas = [
            (ReasonsForAccess.ProviderCode, orgs.normalise_org_code(header.OrgIDProvider, rp_start_date)),
        ]

        result = [
            ReasonsForAccess.format(rfa_type, rfa_value) for rfa_type, rfa_value in rfas if rfa_value
        ]

        result.sort()

        return result


class CommIdsForMsdsTable(ModelExpression):
    def __init__(self, booking_comm_id_expr: ModelExpression = None,
                 care_contact_comm_id_expr: ModelExpression = None,
                 hospital_spell_commissioner_comm_id_expr: ModelExpression = None):
        self.booking_comm_id_expr = booking_comm_id_expr
        self.care_contact_comm_id_expr = care_contact_comm_id_expr
        self.hospital_spell_commissioner_comm_id_expr = hospital_spell_commissioner_comm_id_expr

    def resolve_value(self, model):
        commissioner_ids = set()

        # msd101
        booking_comm_id = self.booking_comm_id_expr.resolve_value(model)
        if booking_comm_id:
            commissioner_ids.add(booking_comm_id)

        # msd201
        care_contacts = self.care_contact_comm_id_expr.resolve_value(model)
        if care_contacts:
            for care_contact in care_contacts:
                commissioner_ids.add(care_contact.OrgIDComm)

        # msd502
        hospital_spell_commissioners = self.hospital_spell_commissioner_comm_id_expr.resolve_value(model)
        if hospital_spell_commissioners:
            for hospital_spell_commissioner in hospital_spell_commissioners:
                commissioner_ids.add(hospital_spell_commissioner.OrgIDComm)

        commissioner_ids -= {None}
        return list(commissioner_ids)


class SnomedProcedureCodeMatches(ModelExpression):
    """ Tests if the procedure code matches one of the specified values """
    def __init__(self, procedure_code: ModelExpression, values: List[str]):
        self.procedure_code = procedure_code
        self.values = values

    def resolve_value(self, model: T) -> bool:
        procedure_code = self.procedure_code.resolve_value(model)  # type: Optional[str]
        return procedure_code in self.values


class DeriveSmokingStatus(ModelExpression):
    def __init__(self, finding_code_expr: ModelExpression, obs_code_expr: ModelExpression,
                 cigs_per_day_expr: ModelExpression, com_reading_expr: ModelExpression):
        self.finding_code_expr = finding_code_expr
        self.obs_code_expr = obs_code_expr
        self.cigs_per_day_expr = cigs_per_day_expr
        self.com_reading_expr = com_reading_expr

    def resolve_value(self, model):
        finding_code = self.finding_code_expr.resolve_value(model)
        obs_code = self.obs_code_expr.resolve_value(model)
        cigs_per_day = as_type(float, self.cigs_per_day_expr.resolve_value(model))
        com_reading = as_type(float, self.com_reading_expr.resolve_value(model))

        if finding_code in SnomedCodeSystemConcept.SMOKING_CODES:
            return '{}'.format(finding_code)
        elif obs_code == SnomedCodeSystemConcept.DATE_CEASED_SMOKING_CODE:
            return 'NON/EX-SMOKER'
        elif cigs_per_day and cigs_per_day > 0:
            return 'SMOKER'
        elif cigs_per_day == 0:
            return 'NON/EX-SMOKER'
        elif com_reading and com_reading >= 4:
            return 'SMOKER'
        elif com_reading and com_reading < 4:
            return 'NON/EX-SMOKER'


class DeriveCOMonReading(ModelExpression):
    """ Derives CO MONITORING READING """
    def __init__(self, observation_code, observation_units, observation_value, finding_code):
        self.observation_code = observation_code
        self.observation_units = observation_units
        self.observation_value = observation_value
        self.finding_code = finding_code

    def resolve_value(self, model) -> Optional[str]:
        observation_code = self.observation_code.resolve_value(model)
        observation_units = self.observation_units.resolve_value(model)
        observation_value = self.observation_value.resolve_value(model)

        if observation_code == SnomedCodeSystemConcept.CO2_READING_OBSERVATION_CODE \
            and observation_units \
                and (observation_units.lower().strip() in UnitsOfMeasure.CO_PPM
                     or observation_units.lower().strip() in UnitsOfMeasure.PPM) \
                and is_non_negative_numeric(observation_value):
            return observation_value

        finding_code = self.finding_code.resolve_value(model)
        if finding_code == SnomedCodeSystemConcept.CO2_READING_FINDING_CODE:
            return 'Test Declined'

        return None


class DeriveAlcoholUnitsPerWeek(ModelExpression):
    """ Derives ALCOHOL UNITS PER WEEK """
    def __init__(self, observation_code, observation_value):
        self.observation_code = observation_code
        self.observation_value = observation_value

    def resolve_value(self, model) -> Optional[str]:
        observation_code = self.observation_code.resolve_value(model)
        observation_value = self.observation_value.resolve_value(model)

        if observation_code in SnomedCodeSystemConcept.ALCOHOL_INTAKE_CODES and \
                is_non_negative_numeric(observation_value):
            return observation_value

        return None


class DerivePersonHeight(ModelExpression):
    """ Derives PERSON HEIGHT (in centimeters) """
    def __init__(self, observation_code, observation_units, observation_value):
        self.observation_code = observation_code
        self.observation_units = observation_units
        self.observation_value = observation_value

    def resolve_value(self, model) -> Optional[str]:
        observation_code = self.observation_code.resolve_value(model)
        observation_units = self.observation_units.resolve_value(model)
        observation_value = self.observation_value.resolve_value(model)

        if observation_code == SnomedCodeSystemConcept.BODY_HEIGHT_MEASURE_CODE and \
                is_non_negative_numeric(observation_value):
            if observation_units in UnitsOfMeasure.CENTIMETER:
                return observation_value
            elif observation_units in UnitsOfMeasure.METER:
                value_in_cm = Decimal(observation_value) * 100
                return '{}'.format(value_in_cm)

        return None


class DerivePersonWeight(ModelExpression):
    """ Derives PERSON WEIGHT (in kg) """
    def __init__(self, observation_code, observation_units, observation_value):
        self.observation_code = observation_code
        self.observation_units = observation_units
        self.observation_value = observation_value

    def resolve_value(self, model) -> Optional[str]:
        observation_code = self.observation_code.resolve_value(model)
        observation_units = self.observation_units.resolve_value(model)
        observation_value = self.observation_value.resolve_value(model)

        if observation_code == SnomedCodeSystemConcept.BODY_WEIGHT_CODE and \
                is_non_negative_numeric(observation_value) and \
                observation_units in UnitsOfMeasure.KILOGRAMS:
            return observation_value

        return None


class DeriveCigarettesPerDay(ModelExpression):
    """ Derives CIGARETTES PER DAY """

    def __init__(self, observation_code, observation_value):
        self.observation_code = observation_code
        self.observation_value = observation_value

    def resolve_value(self, model) -> Optional[str]:
        observation_code = self.observation_code.resolve_value(model)
        observation_value = self.observation_value.resolve_value(model)

        if observation_code == SnomedCodeSystemConcept.CIGARETTE_CONSUMPTION_CODE and \
                is_non_negative_numeric(observation_value):
            return observation_value

        return None


class DeriveBirthWeight(ModelExpression):
    """ Derives BIRTH WEIGHT (in g) """
    def __init__(self, observation_code, observation_units, observation_value):
        self.observation_code = observation_code
        self.observation_units = observation_units
        self.observation_value = observation_value

    def resolve_value(self, model) -> Optional[str]:
        observation_code = self.observation_code.resolve_value(model)
        observation_units = self.observation_units.resolve_value(model)
        observation_value = self.observation_value.resolve_value(model)

        if observation_code == SnomedCodeSystemConcept.BIRTH_WEIGHT and is_non_negative_numeric(observation_value):
            if observation_units in UnitsOfMeasure.GRAM:
                return observation_value
            elif observation_units in UnitsOfMeasure.KILOGRAMS:
                value_in_g = Decimal(observation_value) * 1000
                return '{}'.format(value_in_g)

        return None


class DerivePostpartumBloodLoss(ModelExpression):
    def __init__(self, master_snomed_ct_obs_code, obs_value, ucum_unit):
        self.master_snomed_ct_obs_code = master_snomed_ct_obs_code
        self.obs_value = obs_value
        self.ucum_unit = ucum_unit

    def resolve_value(self, model: T):
        obs_code = self.master_snomed_ct_obs_code.resolve_value(model)
        ucum = self.ucum_unit.resolve_value(model)
        obs_value = self.obs_value.resolve_value(model)

        if obs_code == 719051004 and ucum and ucum.lower() == 'ml' \
                and is_non_negative_numeric(obs_value):
            return obs_value
