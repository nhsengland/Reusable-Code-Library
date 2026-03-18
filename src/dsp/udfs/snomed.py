from testdata.ref_data.providers import SnomedTermProvider


def snomedct_description_from_snomedct_id(snomedct_id: str, point_in_time: str) -> str:
    """
    Returns a Snomed CT description for a given Snomed CT ID and look-up date

    """
    snomed_term_provider = SnomedTermProvider()
    snomedct_description = snomed_term_provider.lookup_term(snomedct_id, point_in_time)
    return snomedct_description
