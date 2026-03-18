# pattern for hh:mm:ss
TIME_PATTERN = "^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$"

# pattern for yyyy-mm-dd that recognises leap years
YYYY_MM_DD_PATTERN = r"""(?x)
    ^ (
        ( # non-leap days
            \d{4} # year
            -
            (
                (0[13578] | 1[02]) - (0[1-9] | [1-2][0-9] | 3[01]) | # long months - any day
                (0[469] | 11) - (0[1-9] | [1-2][0-9] | 30) |         # short months - any day
                02 - ([01][0-9] | 2[0-8])                   # February - day in non-leap year
            )
        ) |
        ( # leap day
            (
                \d{2} (                                     # regular leap years: dividable by 4, but not by 100
                            0[48] |
                            [2468][048] |
                            [13579][26]
                ) |
                2000                                        # multiple of 400 leap years
            )
            - 02 - 29                                       # February 29th
        )
    ) $"""

# pattern for regex
POSTCODE_PATTERN = r"^[a-zA-Z]{1,2}\d[a-zA-Z\d]?\s+\d[a-zA-Z]{2}$"
POSTCODE_PATTERN_INCLUDING_GIROBANK = r"^([a-zA-Z]{3}|[a-zA-Z]{1,2}\d[a-zA-Z\d]?)\s+\d[a-zA-Z]{2}$"

# pattern for personal score  MSD104 and MSD203
PERSON_SCORE_PATTERN = r"^\d{1,2}$"

# pattern for YYYY-MM-DDThh:mm:ss
YYYY_MM_DD_T_HH_MM_SS_PATTERN = r"""(?x)
    ^ (
        ( # non-leap days
            \d{4} # year
            -
            (
                (0[13578] | 1[02]) - (0[1-9] | [1-2][0-9] | 3[01]) | # long months - any day
                (0[469] | 11) - (0[1-9] | [1-2][0-9] | 30) |         # short months - any day
                02 - ([01][0-9] | 2[0-8])                   # February - day in non-leap year
            )
        ) |
        ( # leap day
            (
                \d{2} (                                     # regular leap years: dividable by 4, but not by 100
                            0[48] |
                            [2468][048] |
                            [13579][26]
                ) |
                2000                                        # multiple of 400 leap years
            )
            - 02 - 29                                       # February 29th
        )
    )T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$
"""

# pattern for YYYY-MM-DDThh:mm:ss_Offset for MHSDS_V5
YYYY_MM_DD_T_HH_MM_SS_OFFSET_PATTERN = r"""(?x)
    ^ (
        ( # non-leap days
            \d{4} # year
            -
            (
                (0[13578] | 1[02]) - (0[1-9] | [1-2][0-9] | 3[01]) | # long months - any day
                (0[469] | 11) - (0[1-9] | [1-2][0-9] | 30) |         # short months - any day
                02 - ([01][0-9] | 2[0-8])                   # February - day in non-leap year
            )
        ) |
        ( # leap day
            (
                \d{2} (                                     # regular leap years: dividable by 4, but not by 100
                            0[48] |
                            [2468][048] |
                            [13579][26]
                ) |
                2000                                        # multiple of 400 leap years
            )
            - 02 - 29                                       # February 29th
        )
    )T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]
    (Z | [+][0][0|1]:00 | -00:00)$
"""


