"""minos.common.queries module."""

from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from functools import (
    partial,
)
from operator import (
    attrgetter,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
)

if TYPE_CHECKING:
    from .model import (
        Model,
    )


class _Condition(ABC):
    def evaluate(self, value: Model) -> bool:
        """

        :param value:
        :return:
        """
        return self._evaluate(value)

    @abstractmethod
    def _evaluate(self, value: Model) -> bool:
        """TODO"""

    def __eq__(self, other) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable[Any]:
        yield from tuple()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(map(repr, self))})"


class _TrueCondition(_Condition):
    def _evaluate(self, value: Model) -> bool:
        return True


class _FalseCondition(_Condition):
    def _evaluate(self, value: Model) -> bool:
        return False


class _ComposedCondition(_Condition, ABC):
    def __init__(self, *parts: _Condition):
        self.parts = tuple(parts)

    def __iter__(self):
        yield from self.parts


class _AndCondition(_ComposedCondition):
    def _evaluate(self, value: Model) -> bool:
        return all(c.evaluate(value) for c in self.parts)


class _OrCondition(_ComposedCondition):
    def _evaluate(self, value: Model) -> bool:
        return any(c.evaluate(value) for c in self.parts)


class _NotCondition(_Condition):
    def __init__(self, inner: _Condition):
        self.inner = inner

    def _evaluate(self, value: Model) -> bool:
        return not self.inner.evaluate(value)

    def __iter__(self) -> Iterable[Any]:
        yield from (self.inner,)


class _SimpleCondition(_Condition, ABC):
    def __init__(self, field: str, value: Model):
        self.field = field
        self.parameter = value

    def __iter__(self) -> Iterable[Any]:
        yield from (
            self.field,
            self.parameter,
        )

    @property
    def _get_field(self) -> Callable[[Any], Any]:
        return attrgetter(self.field)


class _LowerCondition(_SimpleCondition):
    def _evaluate(self, value: Model) -> bool:
        return self._get_field(value) < self.parameter


class _LowerEqualCondition(_SimpleCondition):
    def _evaluate(self, value: Model) -> bool:
        return self._get_field(value) <= self.parameter


class _GreaterCondition(_SimpleCondition):
    def _evaluate(self, value: Model) -> bool:
        return self._get_field(value) > self.parameter


class _GreaterEqualCondition(_SimpleCondition):
    def _evaluate(self, value: Model) -> bool:
        return self._get_field(value) >= self.parameter


class _EqualCondition(_SimpleCondition):
    def _evaluate(self, value: Model) -> bool:
        return self._get_field(value) == self.parameter


class _NotEqualCondition(_SimpleCondition):
    def _evaluate(self, value: Model) -> bool:
        return self._get_field(value) != self.parameter


class _InCondition(_SimpleCondition):
    def _evaluate(self, value: Model) -> bool:
        return self._get_field(value) in self.parameter


_TRUE_CONDITION = _TrueCondition()
_FALSE_CONDITION = _FalseCondition()


class Condition:
    TRUE = _TRUE_CONDITION
    FALSE = _FALSE_CONDITION
    AND = _AndCondition
    OR = _OrCondition
    NOT = _NotCondition
    LOWER = _LowerCondition
    LOWER_EQUAL = _LowerEqualCondition
    GREATER = _GreaterCondition
    GREATER_EQUAL = _GreaterEqualCondition
    EQUAL = _EqualCondition
    NOT_EQUAL = _NotEqualCondition
    IN = _InCondition


class _Ordering:
    def __init__(self, by: str, reverse: bool):
        self.by = by
        self.reverse = reverse

    def __eq__(self, other) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable[Any]:
        yield from (
            self.by,
            self.reverse,
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(map(repr, self))})"


class Ordering:
    ASC = partial(_Ordering, reverse=False)
    DESC = partial(_Ordering, reverse=True)
