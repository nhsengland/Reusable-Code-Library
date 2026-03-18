from dsp.datasets.models.pcaremeds import PrimaryCareMedicineModel
from dsp.datasets.models.pcaremeds_tests.pcaremeds_helper_tests import pcaremeds_test_data
from decimal import Decimal
import pytest

@pytest.mark.parametrize(
    "prescribedbnfcode, paidquantity, itemactualcost, paiddissallowed, notdispensed, private, outofhours, expected_paidindicator",
    [
        ('1915', Decimal(0), Decimal(0), 'N', 'N', 0, 0, 'N'),
        ('1915', Decimal(0), Decimal(1), 'N', 'N', 0, 0, 'N'),
        ('1915', Decimal(0), Decimal(0), 'Y', 'N', 0, 0, 'N'),
        ('1915', Decimal(0), Decimal(1), 'Y', 'N', 0, 0, 'N'),

        ('1915', Decimal(1), Decimal(0), 'Y', 'N', 0, 0, 'N'),
        ('1915', Decimal(1), Decimal(1), 'Y', 'N', 0, 0, 'N'),

        ('2715', Decimal(0), Decimal(0), 'N', 'N', 0, 0, 'N'),
        ('2715', Decimal(1), Decimal(0), 'N', 'N', 0, 0, 'N'),
        ('2715', Decimal(0), Decimal(0), 'Y', 'N', 0, 0, 'N'),
        ('2715', Decimal(1), Decimal(0), 'Y', 'N', 0, 0, 'N'),

        ('2715', Decimal(0), Decimal(1), 'Y', 'N', 0, 0, 'N'),
        ('2715', Decimal(1), Decimal(1), 'Y', 'N', 0, 0, 'N'),

        ('1915', Decimal(1), Decimal(0), 'N', 'N', 0, 0, 'Y'),
        ('1915', Decimal(1), Decimal(1), 'N', 'N', 0, 0, 'Y'),

        ('2715', Decimal(0), Decimal(1), 'N', 'N', 0, 0, 'Y'),
        ('2715', Decimal(1), Decimal(1), 'N', 'N', 0, 0, 'Y'),
    ])
def test_paid_indicator(prescribedbnfcode, paidquantity, itemactualcost, paiddissallowed, notdispensed, private,
                        outofhours, expected_paidindicator):
    record = pcaremeds_test_data()
    record['META']['EVENT_ID'] = '1:1'
    record['PrescribedBNFCode'] = prescribedbnfcode
    record['PaidQuantity'] = paidquantity
    record['ItemActualCost'] = itemactualcost
    record['PaidDissallowedIndicator'] = paiddissallowed
    record['NotDispensedIndicator'] = notdispensed
    record['PrivatePrescriptionIndicator'] = private
    record['OutOfHoursIndicator'] = outofhours
    post_derivs = PrimaryCareMedicineModel(record)

    assert post_derivs.PaidIndicator == expected_paidindicator


@pytest.mark.parametrize("highvolvaccineindicator, dispensedpharmacytype, paidformulation, paidquantity, exp_itemcount",
                         [('Y', 7, '0020', Decimal(15), Decimal(5)),
                          ('Y', 0, '0020', Decimal(15), Decimal(15)),
                          ('N', 7, '0020', Decimal(15), Decimal(1)),
                          ('N', 0, '0020', Decimal(15), Decimal(1))])
def test_item_count(highvolvaccineindicator, dispensedpharmacytype, paidformulation, paidquantity, exp_itemcount):
    record = pcaremeds_test_data()
    record['META']['EVENT_ID'] = '1:1'
    record['HighVolVaccineIndicator'] = highvolvaccineindicator
    record['DispensedPharmacyType'] = dispensedpharmacytype
    record['PaidFormulation'] = paidformulation
    record['PaidQuantity'] = paidquantity

    post_derivs = PrimaryCareMedicineModel(record)
    assert post_derivs.ItemCount == exp_itemcount
