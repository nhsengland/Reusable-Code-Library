from abc import ABC, abstractmethod
from typing import AbstractSet
from dsp.validations.common import AssessmentToolName

import re


class PersonScoreValidator(ABC):
    """
    Base class for instances validating person scores
    """

    def __init__(self, format_pattern: str):
        self._format_pattern = format_pattern

    @abstractmethod
    def validate_person_score(self, person_score: str) -> bool:
        """
        Validate the submitted person score

        Args:
            person_score (str): The submitted value to be validated

        Returns:
            bool: Whether the submitted value is valid
        """

    def is_valid_format(self, person_score: str) -> bool:
        """
        Checks if it valid format for person score.

        Args:
            person_score: person score to validate
        Returns:
            True if it is valid format else false
        """
        return self._format_pattern is None or re.match(r"{}".format(self._format_pattern), person_score) is not None


class PersonScoreInSetValidator(PersonScoreValidator):
    """
    A validator which requires that a submitted person score be one of a set of acceptable values
    """

    def __init__(self, acceptable_values: AbstractSet[str],
                 assessment_tool_name: AssessmentToolName, format_pattern: str = None):
        """
        Args:
            acceptable_values (AbstractSet[str]): The set of values accepted by this validator
        """
        super().__init__(format_pattern)
        self._acceptable_values = acceptable_values
        self._assessment_tool_name = assessment_tool_name

    @property
    def assessment_tool_name(self):
        return self._assessment_tool_name

    def validate_person_score(self, person_score: str) -> bool:
        return person_score in self._acceptable_values


class PersonScoreWithinRangeValidator(PersonScoreValidator):
    """
    A validator which requires that a submitted person score be a numeric value within a given range
    """

    def __init__(
            self, lower_bound: float, upper_bound: float, assessment_tool_name: AssessmentToolName = None,
            format_pattern: str = None):
        """
        Args:
            lower_bound (float): The lower bound of the acceptable range, inclusive
            upper_bound (float): The upper bound of the acceptable range, inclusive
        """
        super().__init__(format_pattern)
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound
        self._assessment_tool_name = assessment_tool_name

    @property
    def lower_bound(self) -> float:
        return self._lower_bound

    @property
    def upper_bound(self) -> float:
        return self._upper_bound

    @property
    def assessment_tool_name(self):
        return self._assessment_tool_name

    def validate_person_score(self, person_score: str) -> bool:
        try:
            person_score_float = float(person_score)
            return self.lower_bound <= person_score_float <= self.upper_bound
        except (ValueError, TypeError):
            return False
