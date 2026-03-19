from dsp.common.expressions import ModelExpression, Literal, MultiCase
from typing import List
from decimal import Decimal

__all__ = \
    [
        "And",
        "Or",
        "PaidIndicator",
        "ItemCount"
    ]


class And(ModelExpression):
    def __init__(self, conditions: List, then: ModelExpression = Literal(True),
                 otherwise: ModelExpression = Literal(False)):
        self.conditions = conditions
        self.then = then
        self.otherwise = otherwise

    def resolve_value(self, model):
        conditions = [conds.resolve_value(model)
                      for conds in self.conditions]
        then = self.then.resolve_value(model)
        otherwise = self.otherwise.resolve_value(model)

        if all(conditions):
            return then
        else:
            return otherwise


class Or(ModelExpression):
    def __init__(self, conditions: List, then: ModelExpression = Literal(True),
                 otherwise: ModelExpression = Literal(False)):
        self.conditions = conditions
        self.then = then
        self.otherwise = otherwise

    def resolve_value(self, model):
        conditions = [conds.resolve_value(model) for conds in self.conditions]
        then = self.then.resolve_value(model)
        otherwise = self.otherwise.resolve_value(model)

        if any(conditions):
            return then
        else:
            return otherwise


class PaidIndicator(ModelExpression):
    def __init__(self, prescribedbnfcode: ModelExpression, paidquantity: ModelExpression,
                 itemactualcost: ModelExpression, paiddissallowedindicator: ModelExpression,
                 notdispensedindicator: ModelExpression, privateprescriptionindicator: ModelExpression,
                 outofhoursindicator: ModelExpression
                 ):
        self.prescribedbnfcode = prescribedbnfcode
        self.paidquantity = paidquantity
        self.itemactualcost = itemactualcost
        self.paiddissallowedindicator = paiddissallowedindicator
        self.notdispensedindicator = notdispensedindicator
        self.privateprescriptionindicator = privateprescriptionindicator
        self.outofhoursindicator = outofhoursindicator

    def resolve_value(self, model):
        case_args_dict = {
            'prescribedbnfcode': self.prescribedbnfcode,
            'paidquantity': self.paidquantity,
            'itemactualcost': self.itemactualcost,
            'paiddissallowedindicator': self.paiddissallowedindicator,
            'notdispensedindicator': self.notdispensedindicator,
            'privateprescriptionindicator': self.privateprescriptionindicator,
            'outofhoursindicator': self.outofhoursindicator, }

        return MultiCase(case_args_dict=case_args_dict,
                         branches=[
                             (lambda case_args: all([case_args['prescribedbnfcode'][0:2] in ('19', '21'),
                                                     case_args['paidquantity'] == 0]), Literal('N')),
                             (lambda case_args: all([not case_args['prescribedbnfcode'][0:2] in ('19', '21'),
                                                     case_args['itemactualcost'] == 0]), Literal('N')),
                             (lambda case_args: not all([case_args['paiddissallowedindicator'] == 'N',
                                                         case_args['notdispensedindicator'] == 'N',
                                                         case_args['privateprescriptionindicator'] == 0,
                                                         case_args['outofhoursindicator'] == 0]), Literal('N')),
                         ],
                         default=Literal('Y')).resolve_value(model)


class ItemCount(ModelExpression):
    def __init__(self, highvolvaccineindicator: ModelExpression, dispensedpharmacytype: ModelExpression,
                 paidformulation: ModelExpression, paidquantity: ModelExpression):
        self.highvolvaccineindicator = highvolvaccineindicator
        self.dispensedpharmacytype = dispensedpharmacytype
        self.paidformulation = paidformulation
        self.paidquantity = paidquantity

    def resolve_value(self, model):
        case_args_dict = {
            'highvolvaccineindicator': self.highvolvaccineindicator,
            'dispensedpharmacytype': self.dispensedpharmacytype,
            'paidformulation': self.paidformulation,
            'paidquantity': self.paidquantity
        }
        paidquantityvalue = Decimal(self.paidquantity.resolve_value(model))

        return MultiCase(case_args_dict=case_args_dict,
                         branches=[
                             (lambda case_args: all(
                                 [case_args['highvolvaccineindicator'] == 'Y', case_args['dispensedpharmacytype'] == 7,
                                  case_args['paidformulation'] == '0020']), Literal(paidquantityvalue / 3)),
                             (lambda case_args: case_args['highvolvaccineindicator'] == 'Y' and not all(
                                 [case_args['dispensedpharmacytype'] == 7,
                                  case_args['paidformulation'] == '0020']), Literal(paidquantityvalue))
                         ],
                         default=Literal(Decimal(1))).resolve_value(model)
