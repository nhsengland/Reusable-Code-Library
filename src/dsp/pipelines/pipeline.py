from pyspark.sql.types import StringType
from schematics import Model

from dsp.pipeline import Metadata
from dsp.pipeline.models import PipelineContext


class PipelineKey(Model):
    """
    Hashable type to use as key for pipeline lookup
    """
    dataset_id = StringType()

    def __hash__(self):
        return hash((self.dataset_id,))

    @classmethod
    def from_metadata(cls, metadata: Metadata) -> 'PipelineKey':
        """
        Adapt metadata for a Pipeline 1.0 run to a key

        Args:
            metadata (Metadata): Pipeline metadata to adapt

        Returns:
            _PiplineKey: Corresponding pipeline key for given metadata parameters
        """
        return cls.from_context(PipelineContext.from_metadata(metadata))

    @classmethod
    def from_context(cls, context: PipelineContext) -> 'PipelineKey':
        """
        Adapt context for a Pipeline 2.0 run to a key

        Args:
            context (PipelineContext): Pipeline context to adapt

        Returns:
            PipelineKey: Corresponding pipeline key for given context parameters
        """
        return cls.init(context.primitives['dataset_id'])

    @classmethod
    def init(cls, dataset_id: str) -> 'PipelineKey':
        """
        User-friendly initialiser for keys given arguments

        Args:
            dataset_id (str): The ID of the dataset being processed

        Returns:
            PipelineKey: Corresponding pipeline key for given parameters
        """
        pipeline_key = PipelineKey()
        pipeline_key.dataset_id = dataset_id
        pipeline_key.validate()
        return pipeline_key
