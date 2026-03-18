from dsp.datasets.models.epma_constants import OdsOldMddfUsers, MDDFLegallyExcludedCodes
from typing import Tuple
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import StructType
from dsp.datasets.common import Fields as CommonFields


def convert_mddf(ods, mddf):
    if ods in OdsOldMddfUsers.Ods_legacy_Mddf_Lookup:
        try:
            x = 1000000
            end = int(mddf[-3:])
            front = int(mddf[:-3])
            return str((end * x + front))
        except:
            return mddf
    else:
        return mddf


def add_temp_mddf_and_redact_check(df: DataFrame, version_from: int,
                  version_to: int, target_schema: StructType) -> Tuple[int, DataFrame]:
    def uplift_row(row: Row) -> Row:
        row_dict = row.asDict(recursive=True)
        assert row_dict[CommonFields.META][CommonFields.RECORD_VERSION] == version_from
        row_dict['TempMddf'] = convert_mddf(row_dict.get('ODS'), row_dict.get('MDDF'))
        if row_dict['TempMddf'] in MDDFLegallyExcludedCodes.MDDF_Legally_Sensitive_Lookup:
            row_dict['NHSorCHINumber'] = 'Removed'
            row_dict['LocalPatientIdentifier'] = 'Removed'
        row_dict[CommonFields.META][CommonFields.RECORD_VERSION] = version_to
        return Row(**row_dict)

    uplifted_rdd = df.rdd.map(uplift_row)
    df = uplifted_rdd.toDF(target_schema)

    return version_to, df


