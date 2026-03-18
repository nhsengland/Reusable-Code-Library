from typing import Any, Set

from dsp.datasets.models.csds import CodedImmunisation, MedicalHistory, ProvisionalDiagnosis, PrimaryDiagnosis, \
    SecondaryDiagnosis, CareActivity
from dsp.datasets.models.filtering_extract_definition_decorator import FilteringExtractDefinitionDecorator
from dsp.datasets.models.csds_base import _CareActivity, _CodedImmunisation, \
    _MedicalHistory, _ProvisionalDiagnosis, _PrimaryDiagnosis, _SecondaryDiagnosis
from dsp.pipeline.spark_state import feature_toggle
from dsp.shared.constants import FeatureToggles


CYP202SnomedDerivationFields = {
    _CareActivity.MapSnomedCTProcedureCode,
    _CareActivity.MasterSnomedCTProcedureCode,
    _CareActivity.MasterSnomedCTProcedureTerm,
    _CareActivity.MapOPCS4ProcedureCode,
    _CareActivity.MapOPCS4ProcedureDesc,
    _CareActivity.MapSnomedCTFindingCode,
    _CareActivity.MasterSnomedCTFindingCode,
    _CareActivity.MasterSnomedCTFindingTerm,
    _CareActivity.MapICD10FindingCode,
    _CareActivity.MasterICD10FindingCode,
    _CareActivity.MasterICD10FindingDesc,
    _CareActivity.MapSnomedCTObsCode,
    _CareActivity.MasterSnomedCTObsCode,
    _CareActivity.MasterSnomedCTObsTerm
}

CYP501SnomedDerivationFields = {
    _CodedImmunisation.MapSnomedCTProcedureCode,
    _CodedImmunisation.MasterSnomedCTProcedureCode,
    _CodedImmunisation.MasterSnomedCTProcedureTerm,
    _CodedImmunisation.MapOPCS4ProcedureCode,
    _CodedImmunisation.MapOPCS4ProcedureDesc
}

CYP601SnomedDerivationFields = {
    _MedicalHistory.MapSnomedCTPrevDiagCode,
    _MedicalHistory.MasterSnomedCTPrevDiagCode,
    _MedicalHistory.MasterSnomedCTPrevDiagTerm,
    _MedicalHistory.MapICD10PrevCode,
    _MedicalHistory.MasterICD10PrevCode,
    _MedicalHistory.MasterICD10PrevDesc
}

CYP606SnomedDerivationFields = {
    _ProvisionalDiagnosis.MapSnomedCTProvDiagCode,
    _ProvisionalDiagnosis.MasterSnomedCTProvDiagCode,
    _ProvisionalDiagnosis.MasterSnomedCTProvDiagTerm,
    _ProvisionalDiagnosis.MapICD10ProvCode,
    _ProvisionalDiagnosis.MasterICD10ProvCode,
    _ProvisionalDiagnosis.MasterICD10ProvDesc
}

CYP607SnomedDerivationFields = {
    _PrimaryDiagnosis.MapSnomedCTPrimDiagCode,
    _PrimaryDiagnosis.MasterSnomedCTPrimDiagCode,
    _PrimaryDiagnosis.MasterSnomedCTPrimDiagTerm,
    _PrimaryDiagnosis.MapICD10PrimCode,
    _PrimaryDiagnosis.MasterICD10PrimCode,
    _PrimaryDiagnosis.MasterICD10PrimDesc
}

CYP608SnomedDerivationFields = {
    _SecondaryDiagnosis.MapSnomedCTSecDiagCode,
    _SecondaryDiagnosis.MasterSnomedCTSecDiagCode,
    _SecondaryDiagnosis.MasterSnomedCTSecDiagTerm,
    _SecondaryDiagnosis.MapICD10SecCode,
    _SecondaryDiagnosis.MasterICD10SecCode,
    _SecondaryDiagnosis.MasterICD10SecDesc
}

DerivationFieldsByTable = {
    CareActivity.__table__: CYP202SnomedDerivationFields,
    CodedImmunisation.__table__: CYP501SnomedDerivationFields,
    MedicalHistory.__table__: CYP601SnomedDerivationFields,
    ProvisionalDiagnosis.__table__: CYP606SnomedDerivationFields,
    PrimaryDiagnosis.__table__: CYP607SnomedDerivationFields,
    SecondaryDiagnosis.__table__: CYP608SnomedDerivationFields
}


class DefinitionWithSnomedToggle(FilteringExtractDefinitionDecorator):

    def __init__(self, delegate_definition: Any):
        super().__init__(delegate_definition)

    def _should_apply_filter(self) -> bool:
        return not feature_toggle(FeatureToggles.CSDS_SNOMED_DERIVATIONS)

    def _filtered_fields(self) -> Set[str]:
        return {str(field) for field in DerivationFieldsByTable.get(self.__name__, set())}
