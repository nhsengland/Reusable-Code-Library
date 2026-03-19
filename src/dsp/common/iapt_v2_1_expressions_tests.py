from datetime import date
import pytest
from dsp.common.iapt_v2_1_expressions import DeriveCommissioningRegion
from dsp.common.structured_model import DSPStructuredModel, DerivedAttribute, SubmittedAttribute
from dsp.common.expressions import Select


@pytest.mark.parametrize("org_id_comm, reporting_period_end_date, expected_commissioning_region", [
    ('27T', date(2020, 12, 12), 'Y62'),
    # org 27T has a role RO98 and a relationship (role RO209) so should return CommissioningRegion (org code) Y62
    ('27T', date(2019, 1, 1), None),  # org 27T did not have a valid role RO98 at this date so None should be returned
    ('32T', date(2020, 12, 12), 'Y62'),
    # org 32T has a role RO98 and a relationship (role RO209) so should return CommissioningRegion (org code) Y62
    ('00L', date(2019, 12, 12), 'Y63'),
    # org 00L has a role RO98 and  relationship (role RO210) at this point in time. so should return
    # CommissioningRegion (org code) Y63
    ('00L', date(2020, 12, 12), 'Y63'),
    # org 00L has a role RO98 and a relationship (role RO261) so should return CommissioningRegion (org code) Y63
    ('08F', date(2020, 12, 12), 'Y56'),
    # org 08F has a role RO98 and a relationship (role RO261) so should return CommissioningRegion (org code) Y56
    ('08H', date(2020, 12, 12), 'Y56'),
    # org 08H has a role RO98 and a relationship (role RO210) so should return CommissioningRegion (org code) Y56
    ('08Q', date(2020, 12, 12), 'Y56'),
    # org 08H has a role RO98 and a relationship (role RO210) so should return CommissioningRegion (org code) Y56
    ('00WCC', date(2020, 12, 12), None),
    # org 00WCC has a role RO98 but does not have relationship roles RO209 nor RO2010 so should return None
    ('LSP03', date(2020, 12, 12), None),
    # org LSP03 has a relationship role(RO209) but does not have a role RO98 so should return None
])
def test_derive_commissioning_region(org_id_comm, reporting_period_end_date, expected_commissioning_region):
    class _Info(DSPStructuredModel):
        OrgIDComm = SubmittedAttribute('OrgIDComm', str)  # type: SubmittedAttribute
        ReportingPeriodEndDate = SubmittedAttribute('ReportingPeriodEndDate', date)  # type: SubmittedAttribute

    class Info(_Info):
        __concrete__ = True

        DeriveCommissioningRegion = DerivedAttribute('CommissioningRegion', str, DeriveCommissioningRegion(
            org_code_expr=Select(_Info.OrgIDComm),
            point_in_time_expr=Select(_Info.ReportingPeriodEndDate))
        )  # type: DerivedAttribute

    info1 = Info({
        'OrgIDComm': org_id_comm,
        'ReportingPeriodEndDate': reporting_period_end_date
    })

    assert info1.DeriveCommissioningRegion == expected_commissioning_region
