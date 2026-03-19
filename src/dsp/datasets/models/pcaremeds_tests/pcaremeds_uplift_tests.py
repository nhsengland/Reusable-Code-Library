from dsp.datasets.models.uplift.pcaremeds.out.uplifts import getageband, getitemcount, getpaidindicator
import pytest
from dsp.datasets.models.pcaremeds import PrimaryCareMedicineModel
from dsp.datasets.models.pcaremeds_tests.pcaremeds_helper_tests import pcaremeds_test_data
from decimal import Decimal


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
def test_rdd_paid_indicator(prescribedbnfcode, paidquantity, itemactualcost, paiddissallowed, notdispensed, private,
                            outofhours, expected_paidindicator):
    record = pcaremeds_test_data()
    record['PaidIndicator'] = getpaidindicator(prescribedbnfcode, paidquantity, itemactualcost, paiddissallowed,
                                               notdispensed, private,
                                               outofhours)

    assert record.get('PaidIndicator') == expected_paidindicator


@pytest.mark.parametrize("highvolvaccineindicator, dispensedpharmacytype, paidformulation, paidquantity, exp_itemcount",
                         [('Y', 7, '0020', Decimal(15), Decimal(5)),
                          ('N', 7, '0020', Decimal(15), Decimal(1)),
                          ('Y', 8, '0021', Decimal(15), Decimal(15)),
                          ('Y', 8, '0020', Decimal(15), Decimal(15)),
                          ('Y', 7, '0022', Decimal(15), Decimal(15)),
                          ('Y', 0, '0020', Decimal(15), Decimal(15)),
                          ('N', 0, '0020', Decimal(15), Decimal(1))
                          ])
def test_rdd_item_count(highvolvaccineindicator, dispensedpharmacytype, paidformulation, paidquantity, exp_itemcount):
    record = pcaremeds_test_data()
    record['ItemCount'] = getitemcount(highvolvaccineindicator, dispensedpharmacytype, paidformulation, paidquantity, )

    assert record.get('ItemCount') == exp_itemcount


@pytest.mark.parametrize("patientage, exp_agebands",
                         [(25, "25-29"),
                          (0, "0-4"),
                          (250, None),
                          (-3, None),
                          (None, None)])
def test_rdd_age_bands(patientage, exp_agebands):
    record = pcaremeds_test_data()
    record['AgeBands'] = getageband(patientage)

    assert record.get('AgeBands') == exp_agebands
