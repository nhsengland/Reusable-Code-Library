from typing import Callable, Iterable, List, Tuple, Type, Any, MutableMapping, FrozenSet
from typing import Dict
from pyspark.sql import SparkSession, DataFrame

# import dsp.datasets.ahas
# import dsp.datasets.covid19_pcr_lfd_home_orders.delta_merge.main
# import dsp.datasets.covid19_testing.delta_merge.main
# import dsp.datasets.covid19_testing.enrichments.main
# import dsp.datasets.sgss.enrichments
# import dsp.datasets.sgss.delta_merge.main
import nhs_dq_rules_library.business_rules.cquin
# import dsp.datasets.csds
# import dsp.datasets.csds_v1_6
import nhs_dq_rules_library.business_rules.dids
# import dsp.datasets.enrichments.ahas
# import dsp.datasets.enrichments.csds
# import dsp.datasets.enrichments.csds_v1_6
# import dsp.datasets.enrichments.ofh
# import dsp.datasets.enrichments.iapt
# import dsp.datasets.enrichments.iapt_v2_1
# import dsp.datasets.enrichments.mpsaas
# import dsp.datasets.enrichments.msds
# import dsp.datasets.enrichments.phe_cancer
# import dsp.datasets.enrichments.rapid_cancer_diag
# import dsp.datasets.enrichments.digitrials_dcm
# import dsp.datasets.enrichments.digitrials_coms
import dsp.enrichments.dids
# import dsp.datasets.enrichments.epmanationaladm
# import dsp.datasets.enrichments.epmanationalpres
import dsp.datasets.epma.epmawsad.epmawsad
import dsp.datasets.epma.epmawsad2.epmawsad2
import dsp.datasets.epma.epmawspc.epmawspc
import dsp.datasets.epma.epmawspc2.epmawspc2
import dsp.datasets.epma_national.epmanationalpres.epmanationalpres
import dsp.datasets.epma_national.epmanationaladm.epmanationaladm
# import dsp.datasets.fields.cquin.output
# import dsp.datasets.fields.csds.output
# import dsp.datasets.fields.csds_v1_6.output
import dsp.outputs.dids.output
# import dsp.datasets.fields.mhsds_v5.output
# import dsp.datasets.fields.msds.output
# import dsp.datasets.fields.p2c.output
# import dsp.datasets.fields.randox.output
# import dsp.datasets.fields.iapt.output
# import dsp.datasets.fields.iapt_v2_1.output
# import dsp.datasets.iapt
# import dsp.datasets.iapt_v2_1
# import dsp.datasets.mhsds_v5
import dsp.datasets.models.cquin
import dsp.datasets.models.mhsds_v5
import nhs_dq_rules_library.business_rules.mps
# import dsp.datasets.mpsaas
# import dsp.datasets.msds
# import dsp.datasets.p2c
# import dsp.datasets.pcaremeds
# import dsp.datasets.pos.delta_merge.main
# import dsp.datasets.randox
# import dsp.datasets.randox.schema
# import dsp.datasets.csms
# import dsp.datasets.covid19_pass.delta_merge.main
# from dsp.datasets.ahas import AHAS_OUTPUT_DATAFRAME_NAMES
from dsp.datasets.common import DQRule, reject_on_zero_dq_pass, reject_on_error
from dsp.pipeline import Metadata, ValidationResult
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.common.structured_model import DSPStructuredModel
from dsp.shared.constants import DS, PATHS
import dsp.datasets.definitions.mhsds.mhsds_v6
import dsp.datasets.models.mhsds_v6
import dsp.datasets.definitions.mhsds.mhsds_v6.output

_rejection_evaluators = {
    DS.DIDS: reject_on_error,
    DS.MHSDS_V1_TO_V5_AS_V6: reject_on_zero_dq_pass,
    DS.MHSDS_V6: reject_on_zero_dq_pass
}

_data_format_dq_rules = {
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.DQ_FORMAT_RULES,
}

_type_coercions = {
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.TYPE_COERCIONS,
    'mps': nhs_dq_rules_library.business_rules.mps.TYPE_COERCIONS,
}

_typed_dq_rules = {
    DS.CQUIN: nhs_dq_rules_library.business_rules.cquin.DQ_RULES,
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.DQ_RULES,
}

_uniqueness_dq_rules = {
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.DQ_UNIQUENESS,
}

_dq_message_meta_fields = {
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.DQ_META_FIELDS,
}

_dq_field_values = {
    DS.DIDS: True,
}

_derivations = {
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.DERIVATIONS,
}

_derivations_struct = {
    DS.CQUIN: dsp.datasets.models.cquin.CQUIN,
    DS.MHSDS_V1_TO_V5_AS_V6: dsp.datasets.models.mhsds_v5.Referral,
    DS.MHSDS_V6: dsp.datasets.models.mhsds_v6.Referral,
}

_meta_enrichments = {
    #DS.CQUIN: dsp.datasets.cquin.ENRICHMENTS,
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.ENRICHMENTS,
}

_mps_enrichments = {
#    # DS.AHAS: dsp.datasets.ahas.ENRICHMENTS,
#     DS.CSDS_GENERIC: dsp.datasets.csds.ENRICHMENTS,
#     DS.CSDS_V1_6: dsp.datasets.csds_v1_6.ENRICHMENTS,
#     DS.IAPT: dsp.datasets.iapt.ENRICHMENTS,
#     DS.IAPT_V2_1: dsp.datasets.iapt_v2_1.ENRICHMENTS,
#     DS.MHSDS_V5: dsp.datasets.mhsds_v5.ENRICHMENTS,
#     DS.MSDS: dsp.datasets.msds.ENRICHMENTS
}

_result_selections = {
    DS.CQUIN: nhs_dq_rules_library.business_rules.cquin.RESULT,
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.RESULT,
#     DS.MHSDS_V5: dsp.datasets.mhsds_v5.RESULT,
#     DS.RANDOX: dsp.datasets.randox.schema.RESULT,
#     DS.P2C: dsp.datasets.p2c.schema.RESULT,
#     DS.MHSDS_V6: dsp.datasets.mhsds_v6.RESULT,
 }

_loaders = {
    DS.CQUIN: nhs_dq_rules_library.business_rules.cquin.DATATYPE_LOADERS,
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.DATATYPE_LOADERS,
#     DS.MHSDS_V5: dsp.datasets.mhsds_v5.DATATYPE_LOADERS,
#     'mps': dsp.datasets.mps.DATATYPE_LOADERS,
#     DS.MHSDS_V6: dsp.datasets.mhsds_v6.DATATYPE_LOADERS,
#     DS.MHSDS: dsp.datasets.mhsds_v6.DATATYPE_LOADERS,
 }

_delta_join_cols = {
    DS.CQUIN: nhs_dq_rules_library.business_rules.cquin.DELTA_JOIN_COLUMNS,
    # DS.CSDS: dsp.datasets.csds.DELTA_JOIN_COLUMNS,
    # DS.CSDS_V1_6: dsp.datasets.csds_v1_6.DELTA_JOIN_COLUMNS,
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.DELTA_JOIN_COLUMNS,
    # DS.MHSDS_V5: dsp.datasets.mhsds_v5.DELTA_JOIN_COLUMNS,
    # DS.IAPT: dsp.datasets.iapt.DELTA_JOIN_COLUMNS,
    # DS.IAPT_V2_1: dsp.datasets.iapt_v2_1.DELTA_JOIN_COLUMNS,
    # DS.RANDOX: dsp.datasets.randox.DELTA_JOIN_COLUMNS,
    DS.EPMAWSPC: dsp.datasets.epma.epmawspc.epmawspc.DELTA_JOIN_COLUMNS,
    # DS.P2C: dsp.datasets.p2c.DELTA_JOIN_COLUMNS,
    DS.EPMAWSAD: dsp.datasets.epma.epmawsad.epmawsad.DELTA_JOIN_COLUMNS,
    DS.EPMAWSPC2: dsp.datasets.epma.epmawspc2.epmawspc2.DELTA_JOIN_COLUMNS,
    DS.EPMAWSAD2: dsp.datasets.epma.epmawsad2.epmawsad2.DELTA_JOIN_COLUMNS,
    # DS.CSMS: dsp.datasets.csms.DELTA_JOIN_COLUMNS,
    # DS.MHSDS_V6: dsp.datasets.mhsds_v6.DELTA_JOIN_COLUMNS,
    # DS.MHSDS: dsp.datasets.mhsds_v6.DELTA_JOIN_COLUMNS,
}

_output_cols = {
    # DS.CQUIN: dsp.datasets.fields.cquin.output.Order,
    # DS.CSDS: dsp.datasets.fields.csds.output.Order,
    # DS.CSDS_V1_6: dsp.datasets.fields.csds_v1_6.output.Order,
    DS.DIDS: dsp.outputs.dids.output.Order,
    # DS.MHSDS_V5: dsp.datasets.fields.mhsds_v5.output.Order,
    # DS.MSDS: dsp.datasets.fields.msds.output.Order,
    # DS.IAPT: dsp.datasets.fields.iapt.output.Order,
    # DS.IAPT_V2_1: dsp.datasets.fields.iapt_v2_1.output.Order,
    # DS.RANDOX: dsp.datasets.fields.randox.output.Order,
    # DS.P2C: dsp.datasets.fields.p2c.output.Order,
    # DS.MHSDS_V6: dsp.datasets.fields.mhsds_v6.output.Order,
    # DS.MHSDS: dsp.datasets.fields.mhsds_v6.output.Order,
}

_delta_merges = {
    # DS.IAPT: dsp.datasets.iapt.DELTA_MERGES,
    # DS.IAPT_V2_1: dsp.datasets.iapt_v2_1.DELTA_MERGES,
    # DS.MHSDS_V5: dsp.datasets.mhsds_v5.DELTA_MERGES,
    # DS.MSDS: dsp.datasets.msds.DELTA_MERGES,
    # DS.PCAREMEDS: dsp.datasets.pcaremeds.DELTA_MERGES,
    # DS.CSDS: dsp.datasets.csds.DELTA_MERGES,
    # DS.CSDS_V1_6: dsp.datasets.csds_v1_6.DELTA_MERGES,
    # DS.EPMAWSPC: dsp.datasets.epma.epmawspc.epmawspc.DELTA_MERGES,
    # DS.EPMAWSAD: dsp.datasets.epma.epmawsad.epmawsad.DELTA_MERGES,
    # DS.P2C: dsp.datasets.p2c.DELTA_MERGES,
    # DS.EPMAWSPC2: dsp.datasets.epma.epmawspc2.epmawspc2.DELTA_MERGES,
    # DS.EPMAWSAD2: dsp.datasets.epma.epmawsad2.epmawsad2.DELTA_MERGES,
    # DS.EPMANATIONALPRES: dsp.datasets.epma_national.epmanationalpres.epmanationalpres.DELTA_MERGES,
    # DS.EPMANATIONALADM: dsp.datasets.epma_national.epmanationaladm.epmanationaladm.DELTA_MERGES,
    # DS.MHSDS_V6: dsp.datasets.mhsds_v6.DELTA_MERGES,
    # DS.MHSDS: dsp.datasets.mhsds_v6.DELTA_MERGES,
}  # type: Dict[str, Tuple[DeltaMergeParameters, ...]]

_outputs = {
    # DS.DEID: [],
    # DS.DMS_UPLOAD: [],
    # DS.DAE_DATA_IN: [],
    # DS.AHAS: AHAS_OUTPUT_DATAFRAME_NAMES,
    # DS.DUMMY_PIPELINE: [],
    # DS.DIGITRIALS: [],
    # DS.DIGITRIALS_DCM: [],
    # DS.DIGITRIALS_OUT: [],
    # DS.DIGITRIALS_OPTOUTS: [],
    # DS.DIGITRIALS_COMS: [],
    # DS.DIGITRIALS_REC: [],
    # DS.OXI_HOME: [],
    # DS.NDRS_RDC: [],
    # DS.GRAIL_KCL: [],
    # DS.GRAIL_OPTOUT: [],
    # **dsp.datasets.pos.delta_merge.main.get_dataset_output_tables(),
    # **dsp.datasets.covid19_testing.delta_merge.main.get_dataset_output_tables(),
    # **dsp.datasets.sgss.delta_merge.main.get_dataset_output_tables(),
    # **dsp.datasets.covid19_pcr_lfd_home_orders.delta_merge.main.get_dataset_output_tables(),
    # **dsp.datasets.covid19_pass.delta_merge.main.get_dataset_output_tables()
}

_primary_output = {
    # DS.CSDS: 'cyp001',
    # DS.CSDS_V1_6: 'cyp001',
    # DS.DEID: '',
    # DS.IAPT: 'ids101',
    # DS.IAPT_V2_1: 'ids101',
    # DS.MHSDS_V5: 'mhs101',
    # DS.MSDS: 'msd101',
    # DS.MHSDS_V6: 'mhs101',
    # DS.MHSDS: 'mhs101',
}

_strip_whitespace_fields = {
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.STRIP_FIELDS,
    # DS.CQUIN: dsp.datasets.cquin.STRIP_FIELDS,
    # DS.MPSAAS: dsp.datasets.mpsaas.STRIP_FIELDS,
}

_extra_field_cleansing = {
    DS.DIDS: nhs_dq_rules_library.business_rules.dids.EXTRA_FIELD_CLEANSING
}

_mps_requests = {
    # DS.AHAS: dsp.datasets.enrichments.ahas.mps_request,
    # DS.CSDS: dsp.datasets.enrichments.csds.mps_request,
    # DS.CSDS_V1_6: dsp.datasets.enrichments.csds_v1_6.mps_request,
    DS.DIDS: dsp.enrichments.dids.mps_request,
    # DS.DM_MPS: lambda x: x,
    # DS.IAPT: dsp.datasets.enrichments.iapt.mps_request,
    # DS.IAPT_V2_1: dsp.datasets.enrichments.iapt_v2_1.mps_request,
    # DS.MHSDS_V5: dsp.datasets.mhsds_v5.mps_request,
    # DS.MSDS: dsp.datasets.enrichments.msds.mps_request,
    # DS.MPSAAS: dsp.datasets.enrichments.mpsaas.mps_request,
    # DS.PCAREMEDS: dsp.datasets.pcaremeds.mps_request,
    # DS.NDRS_RDC: dsp.datasets.enrichments.rapid_cancer_diag.mps_request,
    # DS.PHE_CANCER: dsp.datasets.enrichments.phe_cancer.mps_request,
    # DS.OFH: dsp.datasets.enrichments.ofh.mps_request,
    # DS.DIGITRIALS_DCM: dsp.datasets.enrichments.digitrials_dcm.mps_request,
    # DS.DIGITRIALS_OUT: dsp.datasets.enrichments.digitrials_dcm.mps_request,
    # DS.DIGITRIALS_COMS: dsp.datasets.enrichments.digitrials_coms.mps_request,
    # DS.DIGITRIALS_OPTOUTS: lambda x: x,
    # DS.GRAIL_OPTOUT: lambda x: x,
    # **dsp.datasets.covid19_testing.enrichments.main.get_dataset_mps_requests(),
    # **dsp.datasets.sgss.enrichments.get_dataset_mps_requests(),
    # DS.EPMANATIONALADM: dsp.datasets.enrichments.epmanationaladm.mps_request,
    # DS.EPMANATIONALPRES: dsp.datasets.enrichments.epmanationalpres.mps_request,
}

_dataset_versions = {
    DS.AHAS: "1",
    DS.CQUIN: "1",
    DS.CSDS_GENERIC: "1.5",
    DS.CSDS_V1_6: "1.6",
    DS.DEID: "1",
    DS.DIDS: "1",
    DS.MHSDS_V1_TO_V5_AS_V6: "5",
    DS.MSDS: "2",
    DS.PCAREMEDS: "1",
    DS.IAPT_GENERIC: "2",
    DS.IAPT_V2_1: "2.1",
    DS.RANDOX: "1",
    DS.EPMAWSPC: "1",
    DS.EPMAWSPC2: "1",
    DS.EPMAWSAD: "1",
    DS.EPMAWSAD2: "1",
    DS.EPMANATIONALPRES: "1",
    DS.EPMANATIONALADM: "1",
    DS.GENERIC: "1",
    DS.P2C: "1",
    DS.NDRS_RDC: "1",
    DS.PHE_CANCER: "1",
    #DS.OFH: "1",
    DS.OXI_HOME: "1",
    DS.SGSS: "1",
    DS.SGSS_DELTA: "1",
    DS.DIGITRIALS_DCM: "1",
    DS.DIGITRIALS_OUT: "1",
    DS.DIGITRIALS_COMS: "1",
    DS.DIGITRIALS_OPTOUTS: "1",
    DS.DIGITRIALS_REC: "1",
    DS.COVID19_PCR_LFD_HOME_ORDERS: "1",
    DS.GRAIL_KCL: "1",
    DS.GRAIL_OPTOUT: "1",
    DS.CSMS: "1",
    #**dsp.datasets.pos.delta_merge.main.get_dataset_versions(),
    #**dsp.datasets.covid19_testing.delta_merge.main.get_dataset_versions(),
    #**dsp.datasets.covid19_pass.delta_merge.main.get_dataset_versions(),
    DS.NDRS_GERMLINE: "1",
    DS.VIMS: "1",
    DS.MHSDS_V6: "6",
    DS.MHSDS_GENERIC: "6",
}

_record_versions = {
    DS.AHAS: 1,
    DS.CQUIN: 1,
    DS.CSDS_GENERIC: 3,
    DS.CSDS_V1_6: 1,
    DS.DEID: 1,
    DS.DIDS: 1,
    DS.MHSDS_V1_TO_V5_AS_V6: 4,
    DS.MSDS: 6,
    DS.PCAREMEDS: 4,
    DS.IAPT_GENERIC: 2,
    DS.IAPT_V2_1: 5,
    DS.RANDOX: 1,
    DS.EPMAWSPC: 1,
    DS.EPMAWSPC2: 3,
    DS.EPMAWSAD: 1,
    DS.EPMAWSAD2: 3,
    DS.EPMANATIONALPRES: 2,
    DS.EPMANATIONALADM: 2,
    DS.GENERIC: 1,
    DS.P2C: 2,
    DS.NDRS_RDC: 1,
    DS.PHE_CANCER: 1,
    DS.OXI_HOME: 1,
    #DS.OFH: 1,
    DS.SGSS: 1,
    DS.SGSS_DELTA: 1,
    DS.DIGITRIALS_DCM: 1,
    DS.DIGITRIALS_OUT: 1,
    DS.DIGITRIALS_COMS: 1,
    DS.DIGITRIALS_OPTOUTS: 1,
    DS.DIGITRIALS_REC: 1,
    DS.COVID19_PCR_LFD_HOME_ORDERS: 1,
    DS.GRAIL_KCL: 1,
    DS.GRAIL_OPTOUT: 1,
    DS.CSMS: 1,
    #**dsp.datasets.pos.delta_merge.main.get_dataset_record_versions(),
    #**dsp.datasets.covid19_testing.delta_merge.main.get_dataset_record_versions(),
    #**dsp.datasets.covid19_pass.delta_merge.main.get_dataset_record_versions(),
    DS.NDRS_GERMLINE: 1,
    DS.VIMS: 1,
    DS.MHSDS_V6: 2,
    DS.MHSDS_GENERIC: 1,
}

_canonicalise_fields = {
    DS.DIDS,
    DS.CQUIN
}

_automatic_validation_summary_extract = {
    DS.MHSDS_V1_TO_V5_AS_V6,
    DS.MHSDS_V6,
    DS.MSDS,
    DS.IAPT_GENERIC,
    DS.IAPT_V2_1,
    DS.CSDS_GENERIC,
    DS.CSDS_V1_6
}

_datasets_without_table_structure = {
    DS.DIDS
}


def evaluate_rejection(dataset_id: str) -> Callable[[ValidationResult], bool]:
    return _rejection_evaluators.get(dataset_id, lambda _: False)


def canonicalise_fields(dataset_id: str) -> bool:
    return dataset_id in _canonicalise_fields


def strip_whitespace_fields(dataset_id: str) -> List[str]:
    return _strip_whitespace_fields.get(dataset_id, [])


def extra_field_cleansing(dataset_id: str) -> List[Tuple[str, Callable]]:
    return _extra_field_cleansing.get(dataset_id, [])


def ds_typed_dq_rules(dataset_id: str) -> Iterable[DQRule]:
    return _typed_dq_rules.get(dataset_id, [])


def data_format_dq_rules(dataset_id: str) -> Iterable[DQRule]:
    return _data_format_dq_rules.get(dataset_id, [])


def uniqueness_dq_rules(dataset_id: str) -> Tuple[List[str], Callable]:
    return _uniqueness_dq_rules.get(dataset_id, [])


def dq_message_meta_fields(dataset_id: str) -> Tuple[List[str], Callable]:
    """Returns a list of any extra fields whose values need adding to the DQ message FIELD_VALUES column"""
    return _dq_message_meta_fields.get(dataset_id, [])


def dq_field_values(dataset_id: str) -> Tuple[List[str], Callable]:
    """Returns a boolean that controls whether values are added to FIELD_VALUES column. Default is an empty map"""
    return _dq_field_values.get(dataset_id, [])


def data_type_coercions(dataset_id: str) -> List[Tuple[str, Callable, Callable]]:
    return _type_coercions.get(dataset_id, [])


def derivations(dataset_id: str) -> List[Tuple[str, Callable]]:
    return _derivations.get(dataset_id, [])


def derivations_struct(dataset_id: str) -> Type[DSPStructuredModel]:
    return _derivations_struct.get(dataset_id)


def meta_enrichments(dataset_id: str) -> List[Callable[[SparkSession, DataFrame, MutableMapping[str, Any]], DataFrame]]:
    return _meta_enrichments.get(dataset_id, [])


def mps_enrichments(dataset_id: str) -> List[Callable[[int, DataFrame, DataFrame], DataFrame]]:
    return _mps_enrichments.get(dataset_id, [])


def result_selections(dataset_id: str) -> List[Tuple[str, Callable]]:
    return _result_selections.get(dataset_id, [])


def dataset_loader(dataset_id: str, file_type: str) -> Callable[
    [SparkSession, str, Metadata], Tuple[DataFrame, ValidationResult]
]:
    return _loaders[dataset_id][file_type]


def delta_join_columns(dataset_id: str) -> List[str]:
    return _delta_join_cols[dataset_id]


def output_columns(dataset_id: str) -> List[str]:
    return _output_cols[dataset_id]


def delta_merges(dataset_id: str) -> Tuple[DeltaMergeParameters, ...]:
    if dataset_id in _delta_merges:
        return _delta_merges[dataset_id]
    return (DeltaMergeParameters(dataset_id, PATHS.OUT, "{dataset_id}.{dataset_id}".format(dataset_id=dataset_id),
                                 delta_join_columns(dataset_id), output_columns(dataset_id)),)


def outputs(dataset_id: str) -> FrozenSet[str]:
    if dataset_id in _outputs:
        return frozenset(_outputs[dataset_id])

    return frozenset(delta_merge_parameter.source_directory for delta_merge_parameter in delta_merges(dataset_id))


def primary_output(dataset_id: str) -> str:
    return _primary_output.get(dataset_id, PATHS.OUT)


def current_dataset_version(dataset_id: str) -> str:
    """
    Retrieve the current public data model version of the requested dataset

    Args:
        dataset_id (str): The ID of the dataset to retrieve the version of

    Returns:
        str: The current version of the dataset data model
    """
    return _dataset_versions[dataset_id]


def current_record_version(dataset_id: str) -> int:
    """
    Retrieve the current internal version number for records of the requested dataset

    This does not necessarily correlate with updates to the dataset specification, but rather represents internal
    changes to the data, which would require an uplift to existing data as changes take place.

    Args:
        dataset_id (str): The ID of the dataset to retrieve the version of

    Returns:
        int: The current record version of the dataset
    Raises:
        KeyError: If the argument is not a recognised dataset ID
    """
    return _record_versions[dataset_id]


def mps_request(dataset_id: str) -> Callable[[DataFrame], DataFrame]:
    return _mps_requests[dataset_id]


def automatic_validation_summary_extract(dataset_id: str) -> bool:
    """
    Args:
        dataset_id (str): The ID of the dataset being processed

    Returns:
        bool: Whether the given dataset requires an automatic validation summary to be scheduled
    """
    return dataset_id in _automatic_validation_summary_extract


def dataset_without_table_structure(dataset_id: str) -> bool:
    """
    Args:
        dataset_id  (str): The ID of the dataset being processed

    Returns:
        bool: Whether the given dataset has table structure
    """
    return dataset_id in _datasets_without_table_structure
