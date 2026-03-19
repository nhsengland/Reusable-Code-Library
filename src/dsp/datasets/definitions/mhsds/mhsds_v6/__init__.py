from typing import Tuple

from pyspark.sql.functions import col

from dsp.datasets.definitions.mhsds.mhsds_v6.output import Order as MHS101Order
from dsp.loaders import mhsds_v6_df_loader
from dsp.datasets.models.mhsds_v6 import Referral, GroupSession, MentalHealthDropInContact,\
    AnonymousSelfAssessment, StaffDetails, ServiceOrTeamDetails, WardDetails
from dsp.datasets.models.mhsds_v6_base import _Referral
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.shared.constants import FT, DS, RT

DATATYPE_LOADERS = {
    FT.ACCESS: mhsds_v6_df_loader.generate_mhsds_v6
}

# pylint: disable=no-member
RESULT = [(field, lambda field=field: col(field)) for field in Referral.get_field_names()]

MHS301_RESULT = tuple(
    (field, lambda field=field: col(field)) for field in GroupSession.get_field_names()
)
MHS302_RESULT = tuple(
    (field, lambda field=field: col(field)) for field in MentalHealthDropInContact.get_field_names()
)
MHS608_RESULT = tuple(
    (field, lambda field=field: col(field)) for field in AnonymousSelfAssessment.get_field_names()
)
MHS901_RESULT = tuple(
    (field, lambda field=field: col(field)) for field in StaffDetails.get_field_names()
)
MHS902_RESULT = tuple(
    (field, lambda field=field: col(field)) for field in ServiceOrTeamDetails.get_field_names()
)
MHS903_RESULT = tuple(
    (field, lambda field=field: col(field)) for field in WardDetails.get_field_names()
)

DELTA_JOIN_COLUMNS = [
    str(_Referral.Header.OrgIDProvider),
    str(_Referral.UniqMonthID),
]

DELTA_MERGES = (
    DeltaMergeParameters(DS.MHSDS_V6, 'mhs101', '{}.{}'.format(DS.MHSDS_V6, RT.MHSDS_V6),
                         DELTA_JOIN_COLUMNS, MHS101Order),
    DeltaMergeParameters(DS.MHSDS_V6, 'mhs301', '{}.{}'.format(DS.MHSDS_V6, 'mhs301'),
                         DELTA_JOIN_COLUMNS, GroupSession.get_field_names()),
    DeltaMergeParameters(DS.MHSDS_V6, 'mhs302', '{}.{}'.format(DS.MHSDS_V6, 'mhs302'),
                         DELTA_JOIN_COLUMNS, MentalHealthDropInContact.get_field_names()),
    DeltaMergeParameters(DS.MHSDS_V6, 'mhs608', '{}.{}'.format(DS.MHSDS_V6, 'mhs608'),
                         DELTA_JOIN_COLUMNS, AnonymousSelfAssessment.get_field_names()),
    DeltaMergeParameters(DS.MHSDS_V6, 'mhs901', '{}.{}'.format(DS.MHSDS_V6, 'mhs901'),
                         DELTA_JOIN_COLUMNS, StaffDetails.get_field_names()),
    DeltaMergeParameters(DS.MHSDS_V6, 'mhs902', '{}.{}'.format(DS.MHSDS_V6, 'mhs902'),
                         DELTA_JOIN_COLUMNS, ServiceOrTeamDetails.get_field_names()),
    DeltaMergeParameters(DS.MHSDS_V6, 'mhs903', '{}.{}'.format(DS.MHSDS_V6, 'mhs903'),
                         DELTA_JOIN_COLUMNS, WardDetails.get_field_names()),
)  # type: Tuple[DeltaMergeParameters, ...]

