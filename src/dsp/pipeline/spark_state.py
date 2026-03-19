from typing import Dict, Any
from contextlib import contextmanager

import os
from pyspark import Broadcast
from pyspark.sql import SparkSession

from dsp.shared.aws import get_toggles

_broadcast_state_items = {}


def create_broadcast_state(
    spark: SparkSession,
    log_params: Dict[str, Any],
    set_feature_toggles: Dict[str, Any] = None,
    **kwargs
) -> Broadcast:

    assert log_params
    assert "pipeline_name" in log_params

    state = standard_broadcast_state(
        log_params, set_feature_toggles=set_feature_toggles, **kwargs
    )

    return spark.sparkContext.broadcast(state)


def standard_broadcast_state(
    log_params: Dict[str, Any],
    set_feature_toggles: Dict[str, Any] = None,
    **kwargs
) -> Dict[str, Any]:

    state = kwargs or {}

    toggles = get_toggles()

    if set_feature_toggles:
        for feature, value in set_feature_toggles.items():
            toggles["feature"][feature] = value

    state['__standard__'] = {
        "env": os.environ["env"],
        "log_params": log_params,
        "toggles": toggles
    }

    return state


@contextmanager
def temporary_spark_state(values: Dict[str, Any]):
    """ setup spark state in a context manager for broadcast state.

    Args:
        values (Dict[str, Any]): current broadcast values state to track
    """
    previous_values = _broadcast_state_items
    set_broadcast_state(values)
    yield
    set_broadcast_state(previous_values)


def set_broadcast_state(broadcast_state: Dict[str, Any]):
    """ set the broadcast state service locator

    Args:
        broadcast_state (Dict[str, Any]): spark broadcast state ...  with dict like value

    """
    global _broadcast_state_items
    _broadcast_state_items = broadcast_state


def get_broadcast_state() -> Dict[str, Any]:
    """ get the broadcast state service locator
    Returns:
        dict: broadcast state
    """
    return _broadcast_state_items


def get(key, default=None):
    """ get an item from the broadcast state

    Args:
        key (str): key to try and get from the state dict
        default (any): default value if key not found

    Returns:
        any: item from broadcast state
    """
    return _broadcast_state_items.get(key, default)


def log_params() -> Dict[str, Any]:

    return _broadcast_state_items['__standard__']['log_params']


def env() -> Dict[str, Any]:

    return _broadcast_state_items['__standard__']['env']


def feature_toggle(feature: str) -> bool:
    """
        gets the state of a feature toggle
    Args:
        feature (str): feature toggle state

    Returns:
        bool: toggle state
    """

    return _broadcast_state_items['__standard__']['toggles']['feature'][feature]


def set_feature_toggle(feature: str, state: bool):
    """
        gets the state of a feature toggle
    Args:
        feature (str): feature toggle state
        state (bool): the feature toggle state
    """
    _broadcast_state_items['__standard__']['toggles']['feature'][feature] = state


def feature_toggles() -> Dict[str, Any]:
    """
        gets the state of all feature toggles
    Returns:
        dict[str: bool]: a dictionary of feature toggles - feature toggle name : bool
    """
    return _broadcast_state_items['__standard__']['toggles']['feature']


class FakeBroadcastState(Broadcast):

    __slots__ = ['value']

    def __init__(self, set_feature_toggles: Dict[str, Any] = None, **kwargs):

        self.value = standard_broadcast_state({}, set_feature_toggles=set_feature_toggles, **kwargs)
