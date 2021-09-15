"""minos.common.snapshot.queries module."""

from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from functools import (
    partial,
)
from typing import (
    Any,
    Iterable,
)


class _Ordering:
    """TODO"""

    def __init__(self, by: str, reverse: bool):
        self.by = by
        self.reverse = reverse


class _Condition(ABC):
    """TODO"""


class TrueCondition(_Condition):
    """TODO"""


class FalseCondition(_Condition):
    """TODO"""


class _ComposedCondition(_Condition, ABC):
    """TODO"""

    def __init__(self, *parts: Iterable[_Condition]):
        self.parts = set(parts)

    def __iter__(self):
        yield from self.parts


class AndCondition(_ComposedCondition):
    """TODO"""


class OrCondition(_ComposedCondition):
    """TODO"""


class NotCondition(_Condition):
    def __init__(self, inner: _Condition):
        self.inner = inner


class _SimpleCondition(_Condition, ABC):
    """TODO"""

    def __init__(self, field: Any, value: Any):
        self.field = field
        self.value = value


class LowerCondition(_SimpleCondition):
    """TODO"""


class LowerEqualCondition(_SimpleCondition):
    """TODO"""


class GreaterCondition(_SimpleCondition):
    """TODO"""


class GreaterEqualCondition(_SimpleCondition):
    """TODO"""


class EqualCondition(_SimpleCondition):
    """TODO"""


class NotEqualCondition(_SimpleCondition):
    """TODO"""


class InCondition(_SimpleCondition):
    """TODO"""


_TRUE_CONDITION = TrueCondition()
_FALSE_CONDITION = FalseCondition()


class Ordering:
    ASC = partial(_Ordering, reverse=False)
    DESC = partial(_Ordering, reverse=True)


class Condition:
    TRUE = _TRUE_CONDITION
    FALSE = _FALSE_CONDITION
    AND = AndCondition
    OR = OrCondition
    NOT = NotCondition
    LOWER = LowerCondition
    LOWER_EQUAL = LowerEqualCondition
    GREATER = GreaterCondition
    GREATER_EQUAL = GreaterEqualCondition
    EQUAL = EqualCondition
    NOT_EQUAL = NotEqualCondition
    IN = InCondition
