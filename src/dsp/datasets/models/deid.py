from pyspark.sql.types import *

from dsp.common.structured_model import DSPStructuredModel, SubmittedAttribute

SCHEMA = StructType(
    [
        StructField('correlation_id', StringType()),
        StructField('clear', StringType()),
        StructField('pseudo_type', StringType())
    ]
)

class BasePseudo(DSPStructuredModel):
    Root = None  # type: ModelAttribute
    Parent = None  # type: ModelAttribute
    __table__ = None  # type: str

    RowNumber = SubmittedAttribute('RowNumber', int)  # type: SubmittedAttribute


class _Pseudo(BasePseudo):
    """pseudo"""

    # str
    id = SubmittedAttribute('id', str)  # type: SubmittedAttribute

    # str
    domain = SubmittedAttribute('domain', str)  # type: SubmittedAttribute

    # str
    pseudo_type = SubmittedAttribute('pseudo_type', str)  # type: SubmittedAttribute

    # str
    pseudo = SubmittedAttribute('pseudo', str)  # type: SubmittedAttribute

    # str
    clear = SubmittedAttribute('clear', str)  # type: SubmittedAttribute

    # str
    job_id = SubmittedAttribute('job_id', str)  # type: SubmittedAttribute


class Pseudo(_Pseudo):
    """pseudo"""

    __table__ = "pseudo"
    __concrete__ = True
