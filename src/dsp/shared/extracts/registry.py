from typing import MutableMapping, Type

# from shared.extracts.ahas.data_pump.ahas_data_pump_extract import AHASDataPumpExtract
# from shared.extracts.ahas.data_quality.ahas_data_quality_extract import (
#     AHASDataQualityProdExtract,
#     AHASDataQualityRawExtract,
# )
# from shared.extracts.ahas.data_quality.ahas_data_quality_report_extract import AHASDataQualityReportExtract
from dsp.shared.extracts.base import BaseExtract
# from shared.extracts.code_promotion.code_promotion_mesh_extract import CodePromotionMeshExtract
# from shared.extracts.code_promotion.code_promotion_s3_extract import CodePromotionS3Extract
# from shared.extracts.code_promotion.code_promotion_seft_extract import CodePromotionSeftExtract
# from shared.extracts.covid19_pcr_lfd_home_orders.covid19_home_orders_da_extract import Covid19HomeOrdersDAExtract
# from shared.extracts.covid19_testing.covid_antibody_testing.delta_extract import CovidAntibodyTestingDeltaExtract
# from shared.extracts.covid19_testing.covid_antibody_testing.keystone_extract import CovidAntibodyTestingKeystoneExtract
# from shared.extracts.covid19_testing.nhs_lfd_tests.dq_extract import NHSLFDTestsDqExtract
# from shared.extracts.covid19_testing.npex.delta_extract import NPExDeltaExtract
# from shared.extracts.covid19_testing.npex.keystone_extract import NPExKeystoneExtract
# from shared.extracts.covid19_testing.npex_mps.delta_extract import NPExMpsDeltaExtract
# from shared.extracts.covid19_testing.reflex_assay_results.delta_extract import ReflexAssayResultsDeltaExtract
# from shared.extracts.csds.csds_commissioning_extract import CSDSCommExtract
# from shared.extracts.csds.csds_historic_commissioning_extract import CSDSHistoricCommExtract
# from shared.extracts.csds.csds_person_id_extract import CSDSPersonIDExtract
# from shared.extracts.csds.csds_provider_post_deadline_extract import CSDSProviderPostDeadlineExtract
# from shared.extracts.csds.csds_provider_pre_deadline_extract import CSDSProviderPreDeadlineExtract
# from shared.extracts.csds.csds_summary_extract import CSDSSummaryExtract
# from shared.extracts.csds_v1_6.csds_v1_6_commissioning_extract import CSDS_V1_6_CommExtract
# from shared.extracts.csds_v1_6.csds_v1_6_person_id_extract import CSDS_V1_6_PersonIDExtract
# from shared.extracts.csds_v1_6.csds_v1_6_historic_commissioning_extract import CSDS_V1_6_HistoricCommExtract
# from shared.extracts.csds_v1_6.csds_v1_6_provider_pre_deadline_extract import CSDS_V1_6_ProviderPreDeadlineExtract
# from shared.extracts.csds_v1_6.csds_v1_6_provider_post_deadline_extract import CSDS_V1_6_ProviderPostDeadlineExtract
# from shared.extracts.csds_v1_6.csds_v1_6_summary_extract import CSDS_V1_6_SummaryExtract
# from shared.extracts.data_managers.data_manager_extract import DataManagerExtract
# from shared.extracts.de_id.de_id_domain_one_extract import DeIdDomainOneExtract
# from shared.extracts.de_id.de_id_target_domain_extract import DeIdTargetDomainExtract
from dsp.shared.extracts.dids.dids_extract import DidsNhseExtract
from dsp.shared.extracts.dids.dids_extract import DidsNcrasDailyExtract
from dsp.shared.extracts.dids.dids_extract import DidsNcrasMonthlyExtract
# from shared.extracts.gdppr.gdppr_dscro_extract import GDPPRDscroExtract
# from shared.extracts.gp_data.gp_data_cqrs_extract import GPDataCQRSExtract
# from shared.extracts.gp_data.gp_data_udal_extract import GPDataUDALExtract
# from shared.extracts.iapt.iapt_commissioning_extract import IAPTCommExtract
# from shared.extracts.iapt.iapt_historic_commissioning_extract import IAPTHistoricCommExtract
# from shared.extracts.iapt.iapt_person_id_extract import IAPTPersonIDExtract
# from shared.extracts.iapt.iapt_provider_post_deadline_extract import IAPTProviderPostDeadlineExtract
# from shared.extracts.iapt.iapt_provider_pre_deadline_extract import IAPTProviderPreDeadlineExtract
# from shared.extracts.iapt.iapt_summary_extract import IAPTSummaryExtract
# from shared.extracts.iapt_v2_1.iapt_v2_1_person_id_extract import IAPT_V2_1_PersonIDExtract
# from shared.extracts.iapt_v2_1.iapt_v2_1_provider_pre_deadline_extract import IAPT_V2_1_ProviderPreDeadlineExtract
# from shared.extracts.iapt_v2_1.iapt_v2_1_provider_post_deadline_extract import IAPT_V2_1_ProviderPostDeadlineExtract
# from shared.extracts.iapt_v2_1.iapt_v2_1_summary_extract import IAPT_V2_1_SummaryExtract
# from shared.extracts.iapt_v2_1.iapt_v2_1_commissioning_extract import IAPT_V2_1_CommExtract
# from shared.extracts.mhsds_v5.mhsds_v5_commissioning_flat_extract import MHSDSV5CommFlatExtract
# from shared.extracts.mhsds_v5.mhsds_v5_dars_extract import MHSDSV5DarsExtract
# from shared.extracts.mhsds_v5.mhsds_v5_commissioning_extract import MHSDSV5CommExtract
# from shared.extracts.mhsds_v5.mhsds_v5_person_id_extract import MHSDSV5PersonIDExtract
# from shared.extracts.mhsds_v5.mhsds_v5_provider_extract import MHSDSV5ProviderPreDeadlineExtract
# from shared.extracts.mhsds_v5.mhsds_v5_provider_post_extract import MHSDSV5ProviderPostDeadlineExtract
# from shared.extracts.mhsds_v5.mhsds_v5_summary_extract import MHSDSV5SummaryExtract
# from shared.extracts.mhsds_v6.mhsds_v6_provider_extract import MHSDSV6ProviderPreDeadlineExtract
# from shared.extracts.mhsds_v6.mhsds_v6_commissioning_extract import MHSDSV6CommExtract
# from shared.extracts.msds.msds_commissioning_extract import MSDSCommExtract, MSDSDscroExtract
# from shared.extracts.msds.msds_dscro_person_id_extract import MSDSDscroPersonIDExtract
# from shared.extracts.msds.msds_person_id_extract import MSDSPersonIDExtract
# from shared.extracts.msds.msds_provider_mid_window_extract import MSDSProviderMidWindowExtract
# from shared.extracts.msds.msds_provider_post_extract import MSDSProviderPostDeadlineExtract
# from shared.extracts.msds.msds_provider_pre_extract import MSDSProviderPreDeadlineExtract
# from shared.extracts.msds.msds_summary_extract import MSDSSummaryExtract
# from shared.extracts.p2c.p2c_tableau_extract import P2CTableauExtract
# from shared.extracts.sgss.sgss_delta_extract import SGSSDeltaExtract
# from shared.extracts.sgss.sgss_delta_keystone_extract import SGSSDeltaKeystoneExtract
from dsp.shared.extracts.validation_summary import ValidationSummaryExtract


class _ExtractsRegistry:
    def __init__(self):
        self._extracts = {}  # type: MutableMapping[str, Type[BaseExtract]]

    def register(self, extract_class: Type[BaseExtract]):
        extract_type = extract_class.extract_type

        if extract_type in self._extracts:
            raise KeyError("{} is already registered".format(extract_type))

        self._extracts[extract_type] = extract_class

    def get(self, extract_type: str) -> BaseExtract:
        extract_class = self._extracts.get(extract_type)
        return extract_class() if extract_class else None


ExtractsRegistry = _ExtractsRegistry()

ExtractsRegistry.register(ValidationSummaryExtract)

# ExtractsRegistry.register(AHASDataPumpExtract)
# ExtractsRegistry.register(AHASDataQualityReportExtract)
# ExtractsRegistry.register(AHASDataQualityRawExtract)
# ExtractsRegistry.register(AHASDataQualityProdExtract)

# ExtractsRegistry.register(MHSDSV5ProviderPreDeadlineExtract)
# ExtractsRegistry.register(MHSDSV5ProviderPostDeadlineExtract)
# ExtractsRegistry.register(MHSDSV5SummaryExtract)
# ExtractsRegistry.register(MHSDSV5DarsExtract)
# ExtractsRegistry.register(MHSDSV5CommExtract)
# ExtractsRegistry.register(MHSDSV5PersonIDExtract)
# ExtractsRegistry.register(MHSDSV5CommFlatExtract)

# ExtractsRegistry.register(MHSDSV6ProviderPreDeadlineExtract)
# ExtractsRegistry.register(MHSDSV6CommExtract)

# ExtractsRegistry.register(IAPTProviderPreDeadlineExtract)
# ExtractsRegistry.register(IAPTProviderPostDeadlineExtract)
# ExtractsRegistry.register(IAPTSummaryExtract)
# ExtractsRegistry.register(IAPTCommExtract)
# ExtractsRegistry.register(IAPTPersonIDExtract)
# ExtractsRegistry.register(IAPTHistoricCommExtract)

# ExtractsRegistry.register(IAPT_V2_1_SummaryExtract)
# ExtractsRegistry.register(IAPT_V2_1_ProviderPreDeadlineExtract)
# ExtractsRegistry.register(IAPT_V2_1_ProviderPostDeadlineExtract)
# ExtractsRegistry.register(IAPT_V2_1_CommExtract)
# ExtractsRegistry.register(IAPT_V2_1_PersonIDExtract)

# ExtractsRegistry.register(CSDSProviderPreDeadlineExtract)
# ExtractsRegistry.register(CSDSProviderPostDeadlineExtract)
# ExtractsRegistry.register(CSDSSummaryExtract)
# ExtractsRegistry.register(CSDSCommExtract)
# ExtractsRegistry.register(CSDSPersonIDExtract)
# ExtractsRegistry.register(CSDSHistoricCommExtract)

# ExtractsRegistry.register(CSDS_V1_6_CommExtract)
# ExtractsRegistry.register(CSDS_V1_6_PersonIDExtract)
# ExtractsRegistry.register(CSDS_V1_6_HistoricCommExtract)
# ExtractsRegistry.register(CSDS_V1_6_SummaryExtract)
# ExtractsRegistry.register(CSDS_V1_6_ProviderPreDeadlineExtract)
# ExtractsRegistry.register(CSDS_V1_6_ProviderPostDeadlineExtract)

# ExtractsRegistry.register(MSDSCommExtract)
# ExtractsRegistry.register(MSDSPersonIDExtract)
# ExtractsRegistry.register(MSDSDscroPersonIDExtract)
# ExtractsRegistry.register(MSDSDscroExtract)
# ExtractsRegistry.register(MSDSProviderPreDeadlineExtract)
# ExtractsRegistry.register(MSDSSummaryExtract)
# ExtractsRegistry.register(MSDSProviderPostDeadlineExtract)
# ExtractsRegistry.register(MSDSProviderMidWindowExtract)

# ExtractsRegistry.register(DataManagerExtract)
# ExtractsRegistry.register(DeIdTargetDomainExtract)
# ExtractsRegistry.register(DeIdDomainOneExtract)

# ExtractsRegistry.register(NPExDeltaExtract)
# ExtractsRegistry.register(NPExKeystoneExtract)
# ExtractsRegistry.register(NPExMpsDeltaExtract)
# ExtractsRegistry.register(CovidAntibodyTestingDeltaExtract)
# ExtractsRegistry.register(CovidAntibodyTestingKeystoneExtract)
# ExtractsRegistry.register(ReflexAssayResultsDeltaExtract)

# ExtractsRegistry.register(P2CTableauExtract)

# ExtractsRegistry.register(SGSSDeltaExtract)
# ExtractsRegistry.register(SGSSDeltaKeystoneExtract)

# ExtractsRegistry.register(GDPPRDscroExtract)
# ExtractsRegistry.register(GPDataCQRSExtract)
# ExtractsRegistry.register(GPDataUDALExtract)

# ExtractsRegistry.register(NHSLFDTestsDqExtract)

# ExtractsRegistry.register(Covid19HomeOrdersDAExtract)

ExtractsRegistry.register(DidsNhseExtract)
ExtractsRegistry.register(DidsNcrasDailyExtract)
ExtractsRegistry.register(DidsNcrasMonthlyExtract)
# ExtractsRegistry.register(CodePromotionMeshExtract)
# ExtractsRegistry.register(CodePromotionS3Extract)
# ExtractsRegistry.register(CodePromotionSeftExtract)
