from datetime import date, datetime
from dsp.datasets.common import Fields as CommonFields
from dsp.common.structured_model import _META, DerivedAttribute, DerivedAttributePlaceholder, \
    DSPStructuredModel, META, SubmittedAttribute, AssignableAttribute
from dsp.common.expressions import Literal


class _EPMAWellSkyAdministrationModel(DSPStructuredModel):
    META = SubmittedAttribute(CommonFields.META, _META)

    ODS = SubmittedAttribute('ODS', str)
    SPR = SubmittedAttribute('SPR', str)
    ADMLink = SubmittedAttribute('ADMLink', str)
    NHS = SubmittedAttribute('NHS', str)
    Drug = SubmittedAttribute('Drug', str)
    Route = SubmittedAttribute('Route', str)
    PRN = SubmittedAttribute('PRN', str)
    OrderID = SubmittedAttribute('OrderID', int)
    ScheduledAdmin = SubmittedAttribute('ScheduledAdmin', datetime)
    GivenAdminDateTime = SubmittedAttribute('GivenAdminDateTime', datetime)
    AdminReasonNotGiven = SubmittedAttribute('AdminReasonNotGiven', str)
    TransactionDateTime = SubmittedAttribute('TransactionDateTime', datetime)
    SourceSystem = DerivedAttributePlaceholder('SourceSystem', str)
    ReportedDateTime = AssignableAttribute('ReportedDateTime', datetime)
    ReportedDate = AssignableAttribute('ReportedDate', date)
    ReportingPeriodStartDate = AssignableAttribute('ReportingPeriodStartDate', date)
    ReportingPeriodEndDate = AssignableAttribute('ReportingPeriodEndDate', date)


class EPMAWellSkyAdministrationModel(_EPMAWellSkyAdministrationModel):
    __concrete__ = True

    META = SubmittedAttribute('META', META)

    SourceSystem = DerivedAttribute(
        'SourceSystem', str,
        Literal('WellSky'))
