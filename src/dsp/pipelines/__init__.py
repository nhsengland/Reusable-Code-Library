from typing import Dict

import dsp.datasets.sgss.pipelines.main
#import dsp.datasets.covid19_testing.pipelines.main
#import dsp.datasets.pos.pipelines.main
#import dsp.datasets.covid19_pass.pipelines.main
#import dsp.datasets.digitrials.pipelines.main
from dsp.pipelines.pipeline import PipelineKey
from dsp.pipelines import (
   #cquin, deid, 
   dids 
   #mhsds_v5, mpsaas, msds, pcaremeds, dae_data_in, dms_upload,
    #iapt, dm_mps, ahas, csds, randox, generic, dummy_pipeline, p2c, phe_cancer, oxi_home, ofh,
    #covid19_pcr_lfd_home_orders, csms, iapt_v2_1, csds_v1_6, ndrs_germline, vims_invitation_config, mhsds_v6
    )
#from dsp.pipelines import rapid_cancer_diag
#from dsp.datasets.grail import grail_kcl, grail_optout
#from dsp.datasets.digitrials.optouts import digitrials_optouts
from dsp.datasets.epma import epmawspc, epmawsad, epmawspc2, epmawsad2
from dsp.datasets.epma_national import epmanationalpres, epmanationaladm
from dsp.pipelines.mpsaas import MPSaaSPipelineRejectionHandler
from dsp.pipeline import Metadata
from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import Pipeline
from dsp.shared.constants import DS

_DSP_PIPELINES = {
    # PipelineKey.init(DS.CQUIN): Pipeline(cquin.STAGES),
    # PipelineKey.init(DS.CSDS): Pipeline(csds.STAGES),
    # PipelineKey.init(DS.CSDS_V1_6): Pipeline(csds_v1_6.STAGES),
    # PipelineKey.init(DS.DEID): Pipeline(deid.STAGES),
    PipelineKey.init(DS.DIDS): Pipeline(dids.STAGES),
    # PipelineKey.init(DS.MHSDS_V5): Pipeline(mhsds_v5.STAGES,
    #                                         undefined_suspension_stage_handler=lambda: 'feature_toggle_ingestion_mps'),
    # PipelineKey.init(DS.MHSDS): Pipeline(mhsds_v6.STAGES,
    #                                         undefined_suspension_stage_handler=lambda: 'feature_toggle_ingestion_mps'),
    # PipelineKey.init(DS.MHSDS_V6): Pipeline(mhsds_v6.STAGES,
    #                                      undefined_suspension_stage_handler=lambda: 'feature_toggle_ingestion_mps'),
    # PipelineKey.init(DS.MPSAAS): Pipeline(mpsaas.STAGES, pipeline_rejection_handler=MPSaaSPipelineRejectionHandler(),
    #                                       output_loader=lambda spark, path: spark.read.csv(path)),
    # PipelineKey.init(DS.MSDS): Pipeline(msds.STAGES,
    #                                     undefined_suspension_stage_handler=lambda: 'feature_toggle_ingestion_mps'),
    # PipelineKey.init(DS.PCAREMEDS): Pipeline(pcaremeds.STAGES),
    # PipelineKey.init(DS.DAE_DATA_IN): Pipeline(dae_data_in.STAGES),
    # PipelineKey.init(DS.DMS_UPLOAD): Pipeline(dms_upload.STAGES),
    # PipelineKey.init(DS.IAPT): Pipeline(iapt.STAGES),
    # PipelineKey.init(DS.IAPT_V2_1): Pipeline(iapt_v2_1.STAGES),
    # PipelineKey.init(DS.DM_MPS): Pipeline(dm_mps.STAGES),
    # PipelineKey.init(DS.AHAS): Pipeline(ahas.STAGES),
    # PipelineKey.init(DS.RANDOX): Pipeline(randox.STAGES),
    # PipelineKey.init(DS.EPMAWSPC): Pipeline(epmawspc.STAGES),
    # PipelineKey.init(DS.GENERIC): Pipeline(generic.STAGES),
    # PipelineKey.init(DS.P2C): Pipeline(p2c.STAGES),
    # PipelineKey.init(DS.EPMAWSAD): Pipeline(epmawsad.STAGES),
    # PipelineKey.init(DS.DUMMY_PIPELINE): Pipeline(dummy_pipeline.STAGES),
    # PipelineKey.init(DS.EPMAWSPC2): Pipeline(epmawspc2.STAGES),
    # PipelineKey.init(DS.EPMAWSAD2): Pipeline(epmawsad2.STAGES),
    # PipelineKey.init(DS.NDRS_RDC): Pipeline(rapid_cancer_diag.STAGES),
    # PipelineKey.init(DS.PHE_CANCER): Pipeline(phe_cancer.STAGES),
    # PipelineKey.init(DS.OXI_HOME): Pipeline(oxi_home.STAGES),
    # PipelineKey.init(DS.OFH): Pipeline(ofh.STAGES),
    # PipelineKey.init(DS.GRAIL_KCL): Pipeline(grail_kcl.STAGES),
    # PipelineKey.init(DS.GRAIL_OPTOUT): Pipeline(grail_optout.STAGES),
    # PipelineKey.init(DS.DIGITRIALS_OPTOUTS): Pipeline(digitrials_optouts.STAGES),
    # PipelineKey.init(DS.COVID19_PCR_LFD_HOME_ORDERS): Pipeline(covid19_pcr_lfd_home_orders.STAGES),
    # PipelineKey.init(DS.CSMS): Pipeline(csms.STAGES),
    # PipelineKey.init(DS.EPMANATIONALPRES): Pipeline(epmanationalpres.STAGES),
    # PipelineKey.init(DS.EPMANATIONALADM): Pipeline(epmanationaladm.STAGES),
    # PipelineKey.init(DS.NDRS_GERMLINE): Pipeline(ndrs_germline.STAGES),
    # PipelineKey.init(DS.VIMS): Pipeline(vims_invitation_config.STAGES),
}  # type: Dict[PipelineKey, Pipeline]

_PIPELINES = {
    **_DSP_PIPELINES,
    # **dsp.datasets.pos.pipelines.main.get_dataset_pipelines(),
    # **dsp.datasets.covid19_testing.pipelines.main.get_dataset_pipelines(),
    # **dsp.datasets.sgss.pipelines.main.get_dataset_pipelines(),
    # **dsp.datasets.covid19_pass.pipelines.main.get_dataset_pipelines(),
    # **dsp.datasets.digitrials.pipelines.main.get_dataset_pipelines()
}


def has_pipeline_for_metadata(metadata: Metadata) -> bool:
    """
    Determine whether a Pipeline 2.0 specification exists appropriate for the given metadata

    Args:
        metadata (Metadata): The metadata for a Pipeline 1.0 run

    Returns:
        bool: True if a Pipeline 2.0 specification exists
    """
    return _has_pipeline_for_key(PipelineKey.from_metadata(metadata))


def has_pipeline_for_context(context: PipelineContext) -> bool:
    """
    Determine whether a Pipeline 2.0 specification exists appropriate for the given context

    Args:
        context (PipelineContext): The context for a Pipeline 2.0 run

    Returns:
        bool: True if a Pipeline 2.0 specification exists
    """
    return _has_pipeline_for_key(PipelineKey.from_context(context))


def _has_pipeline_for_key(key: PipelineKey) -> bool:
    return key in _PIPELINES


def get_pipeline(context: PipelineContext) -> Pipeline:
    """
    Retrieve the pipeline corresponding to a given context

    Args:
        context (PipelineContext): The context to retrieve a pipeline for

    Returns:
        Pipeline: The corresponding pipeline definition

    Raises:
        ValueError: If no pipeline is registered that corresponds to the given context
    """
    pipeline_key = PipelineKey.from_context(context)
    if pipeline_key in _PIPELINES:
        return _PIPELINES[pipeline_key]

    raise ValueError("No pipeline for {}".format(pipeline_key))
