"""minos.common.snapshot.queries module."""

from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from enum import (
    Enum,
    auto,
)
from typing import (
    Any,
)


class Ordering:
    """TODO"""

    def __init__(self, key: str, reverse: bool = False):
        self.key = key
        self.reverse = reverse


class Condition(ABC):
    """TODO"""


class TRUECondition(Condition):
    """TODO"""


class FALSECondition(Condition):
    """TODO"""


class ComposedCondition(Condition):
    """TODO"""

    def __init__(self, operator: ComposedOperator, conditions: set[Condition]):
        self.operator = operator
        self.conditions = conditions


class SimpleCondition(Condition):
    """TODO"""

    def __init__(self, field: Any, operator: SimpleOperator, value: Any):
        self.field = field
        self.operator = operator
        self.value = value


class ComposedOperator(Enum):
    """TODO"""

    AND = auto()
    OR = auto()


class SimpleOperator(Enum):
    """TODO"""

    LOWER = auto()
    LOWER_EQUAL = auto()
    GREATER = auto()
    GREATER_EQUAL = auto()
    EQUAL = auto()
    NOT_EQUAL = auto()
    IN = auto()
