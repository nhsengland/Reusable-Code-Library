from unittest.mock import patch, Mock
from uuid import uuid4

import pytest

from dsp.pipeline.stages.feature_toggle_suspend import FeatureToggleSuspendStage


@pytest.mark.parametrize(['toggle_value', 'enabled'], [
    (True, True),
    (False, False)
])
def test_feature_state_assigned_by_toggle(toggle_value: str, enabled: bool):
    feature_name = str(uuid4())

    with patch.object(FeatureToggleSuspendStage, '_is_feature_enabled', return_value=toggle_value):
        stage = FeatureToggleSuspendStage(feature_name)
        assert stage._preconditions_met(Mock(), Mock()) == enabled


def test_stage_is_no_op():
    stage = FeatureToggleSuspendStage(str(uuid4()))
    context = Mock()
    assert stage._run(Mock(), context) == context
    context.assert_not_called()
