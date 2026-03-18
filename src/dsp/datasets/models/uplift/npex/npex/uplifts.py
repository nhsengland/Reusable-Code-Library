from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.npex.npex.version_2 import schema as NPEX_V2
from dsp.datasets.models.uplift.npex.npex.version_3 import schema as NPEX_V3
from dsp.datasets.models.uplift.npex.npex.version_4 import schema as NPEX_V4
from dsp.datasets.models.uplift.npex.npex.version_5 import schema as NPEX_V5

UPLIFTS = {
    1: lambda df: simple_uplift(
        df, 1, 2, NPEX_V2, new_fields=["AdministrationMethod", "Landline", "MPSConfidence", "Person_ID", "GPCode"]
    ),
    2: lambda df: simple_uplift(
        df, 2, 3, NPEX_V3, new_fields=[
            "MPSFirstName",
            "MPSLastName",
            "MPSDateOfBirth",
            "MPSGender",
            "MPSPostcode",
            "MPSEmailAddress",
            "MPSMobileNumber",
        ],
        add_if_missing_fields=["MPSGPCode"],
    ),
    3: lambda df: simple_uplift(
        df, 3, 4, NPEX_V4, new_fields=("WorkStatus", "IndustrySector", "EmployerName", "Occupation", "OccupationTitle"),
    ),
    4: lambda df: simple_uplift(
        df, 4, 5, NPEX_V5, new_fields=["TestReason", "DateOfOnset"]
    )
}
