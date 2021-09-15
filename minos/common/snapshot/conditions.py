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


class Condition(ABC):
    pass


class TRUECondition(Condition):
    pass


class FALSECondition(Condition):
    pass


class ComposedCondition(Condition):
    def __init__(self, operator: ComposedOperator, conditions: set[Condition]):
        self.operator = operator
        self.conditions = conditions


class ANDCondition(ComposedCondition):
    def __init__(self, conditions: set[Condition]):
        super().__init__(ComposedOperator.AND, conditions)


class ORCondition(ComposedCondition):
    def __init__(self, conditions: set[Condition]):
        super().__init__(ComposedOperator.AND, conditions)


class SimpleCondition(Condition):
    def __init__(self, field: Any, operator: SimpleOperator, value: Any):
        self.field = field
        self.operator = operator
        self.value = value


class ComposedOperator(Enum):
    AND = auto()
    OR = auto()


class SimpleOperator(Enum):
    LOWER = auto()
    LOWER_EQUAL = auto()
    GREATER = auto()
    GREATER_EQUAL = auto()
    EQUAL = auto()
    NOT_EQUAL = auto()
    IN = auto()
