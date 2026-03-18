import os
from typing import Union, Type

import typing_inspect


class ENVIRONMENT:
    LOCAL = 'local'
    DEV = 'dev'
    REF = 'ref'
    PROD = 'prod'


# Environments to which DSP is deployed (i.e. AWS)
DSP_ENVIRONMENTS = [ENVIRONMENT.DEV, ENVIRONMENT.REF, ENVIRONMENT.PROD]

# Non-prod environments
PTL_ENVIRONMENTS = [ENVIRONMENT.LOCAL, ENVIRONMENT.DEV, ENVIRONMENT.REF]

# all
ALL_ENVIRONMENTS = [ENVIRONMENT.LOCAL, ENVIRONMENT.DEV, ENVIRONMENT.REF, ENVIRONMENT.PROD]

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

SRC_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

METRICS_PORT = 9010


def local_path(*sub_path):
    return os.path.join(PROJECT_ROOT, *sub_path)


def safe_issubclass(test_class: Union[type, Type], expected_class: Union[type, Type]) -> bool:
    """
        in python 3.7 typing types are now passthrough when getting type
        this gets the origin type for types that are typing type ...

        potential for enhancing this to support generics
    """

    test_origin = typing_inspect.get_origin(test_class)
    expected_origin = typing_inspect.get_origin(expected_class)

    resolved_test_class = test_origin if test_origin else test_class
    resolved_expected_class = expected_origin if expected_origin else expected_class

    return issubclass(resolved_test_class, resolved_expected_class)


def safe_isinstance(test_object: object, expected_class: Union[type, Type]) -> bool:
    """
        in python 3.7 typing types are now passthrough when getting type
        this gets the origin type for types that are typing type ...

        potential for enhancing this to better support generics
    """

    expected_origin = typing_inspect.get_origin(expected_class)

    resolved_expected_class = expected_origin if expected_origin else expected_class

    return isinstance(test_object, resolved_expected_class)
