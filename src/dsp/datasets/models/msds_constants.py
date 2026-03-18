from collections import OrderedDict


class ObservationSchemes:
    """The code scheme basis of an observation within the maternity dataset """
    READ_2 = '01'
    READ_CTV3 = '02'
    SNOMED_CT = '03'


class FindingSchemes:
    """The code scheme basis of a finding within the maternity dataset """
    ICD_10 = '01'
    READ_2 = '02'
    READ_CTV3 = '03'
    SNOMED_CT = '04'


class ProcedureSchemes:
    """The code scheme basis of a procedure within the maternity dataset """
    OPCS_4 = '02'
    READ_2 = '04'
    READ_CTV3 = '05'
    SNOMED_CT = '06'


class SnomedCodeSystemConcept:
    """The Snomed Code system concepts within the maternity dataset

    Procedure codes are defined as str - this is because the associated fields in the data-sets are defined as str
    and may contain multiple code separates with a delimiter.

    Other codes are defined as int - the corresponding dataset fields are also defined as ints.
    """
    APGAR_SCORE_AT_5_MINUTES = 169909004
    BIRTH_WEIGHT = 364589006

    EPISIOTOMY_REASON_CODES = ['236992007', '40219000', '26313002', '63407004', '15413009', '17860005', '25828002',
                               '288194000', '85548006']

    LABOUR_ANAESTHESIA_TYPE_CODES = ['16388003', '241717009', '50697003', '67716003', '231249005', '68248001']

    LABOUR_INDUCTION_METHOD_CODES = ['177136006', '177135005', '408818004', '308037008', '177136006',
                                     '1105781000000106', '1105791000000108', '236971007', '288189000', '288190009',
                                     '288191008']

    ROM_REASON_CODES = ['408816000', '408818004']

    GENITAL_TRACT_TRAUMATIC_PROCEDURE_CODES = ['85548006', '236992007', '40219000', '26313002', '63407004', '15413009',
                                               '17860005', '25828002', '288194000']

    GENITAL_TRACT_TRAUMATIC_FINDING_CODES = [249221003, 262935001, 57759005, 6234006, 10217006,  399031001, 199972004,
                                             237329006, 1105801000000107]

    OXYTOCIN_ADMINISTERED_INDUCTION_CODES = ['177135005']

    MCI_TYPE_PROCEDURE_CODES = ['236886002', '305351004']
    MCI_TYPE_FINDING_CODES = [237235009, 33211000, 429098002, 59282003, 274126009, 17263003]

    ALCOHOL_INTAKE_CODES = [160573003, 1082641000000106]

    BODY_HEIGHT_MEASURE_CODE = 50373000
    BODY_WEIGHT_CODE = 27113001
    BODY_MASS_INDEX_CODE = 60621009
    CIGARETTE_CONSUMPTION_CODE = 230056004
    CO2_READING_OBSERVATION_CODE = 251900003
    CO2_READING_FINDING_CODE = 1110631000000106
    DATE_CEASED_SMOKING_CODE = 160625004

    SMOKING_CODES = [77176002, 8517006, 405746006, 266919005, 266927001, 203191000000107]


class UnitOfMeasure(object):
    """ Represents a single unit of measure (e.g. meters, kilograms, etc).

    A given unit of measure may be identified by multiple symbols/code values - typically defined by
    UCOM (http://unitsofmeasure.org/ucum.html). This class supports case-insensitive in / __contains__ checks for
    supported code values, and the canonical value of the code can be obtained via unit['some_code']
    (also case-insensitive).
    """
    def __init__(self, *codes):
        self._codes = OrderedDict((self._normalise_key(value), value) for value in codes)

    @classmethod
    def _normalise_key(cls, code):
        return code.lower().strip() if code else code

    def __contains__(self, item):
        """ Tests if the specified code identifies this unit of measure. This check is
        case-insensitive. """
        return self._normalise_key(item) in self._codes

    def __getitem__(self, item):
        """ Returns the canonical code (i.e. registered case) associated with a unit of measure code. """
        return self._codes[self._normalise_key(item)]

    def __getattr__(self, item):
        key = self._normalise_key(item)
        if key in self._codes:
            return self._codes[key]

        raise AttributeError(item)

    def __str__(self):
        return str(list(self._codes.values()))

    def __repr__(self):
        return repr(list(self._codes.values()))

    def __bool__(self):
        return bool(self._codes)

    def __len__(self):
        return len(self._codes)

    def __iter__(self):
        return iter(self._codes.values())


class UnitsOfMeasure(object):
    """ Constants for units of measure.

    These are typically values defined by UCOM (http://unitsofmeasure.org/ucum.html).
    """
    PPM = UnitOfMeasure('ppm')
    CO_PPM = UnitOfMeasure('coppm')
    CENTIMETER = UnitOfMeasure('cm')
    METER = UnitOfMeasure('m')
    GRAM = UnitOfMeasure('g')
    KILOGRAMS = UnitOfMeasure('kg', 'kilograms')
