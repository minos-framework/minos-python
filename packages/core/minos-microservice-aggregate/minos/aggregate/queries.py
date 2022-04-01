"""minos.common.queries module."""

from __future__ import (
    annotations,
)

import re
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
    Any,
    Callable,
    Iterable,
)

from cached_property import (
    cached_property,
)

from minos.common import (
    Model,
)


class _Condition(ABC):
    def evaluate(self, value: Model) -> bool:
        """Evaluate if given value satisfied this condition.

        :param value: The value to be evaluated.
        :return: A boolean value.
        """
        return self._evaluate(value)

    @abstractmethod
    def _evaluate(self, value: Model) -> bool:
        pass

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
    def __init__(self, field: str, parameter: Any):
        self.field = field
        self.parameter = parameter

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


class _LikeCondition(_SimpleCondition):
    def _evaluate(self, value: Model) -> bool:
        return bool(self._pattern.fullmatch(self._get_field(value)))

    @cached_property
    def _pattern(self):
        return re.compile(self.parameter.replace("%", ".*").replace("_", "."))


_TRUE_CONDITION = _TrueCondition()
_FALSE_CONDITION = _FalseCondition()


class Condition:
    """Condition class.

    This class provides the way to create filtering conditions for ``Model`` instances based on the following operators:

    * `TRUE`: Always evaluates as `True`.
    * `FALSE`: Always evaluates as `True`.
    * `AND`: Only evaluates as `True` if all the given conditions are evaluated as `True`.
    * `OR`: Evaluates as `True` if at least one of the given conditions are evaluated as `True`.
    * `NOT`: Evaluates as `True` only if the inner condition is evaluated as `False`.
    * `LOWER`: Evaluates as `True` only if the field of the given model is lower (<) than the parameter.
    * `LOWER_EQUAL`: Evaluates as `True` only if the field of the given model is lower or equal (<=) to the parameter.
    * `GREATER`: Evaluates as `True` only if the field of the given model is greater (>) than the parameter.
    * `GREATER_EQUAL`: Evaluates as `True` only if the field of the given model is greater or equal (>=) to the
        parameter.
    * `EQUAL`: Evaluates as `True` only if the field of the given model is equal (==) to the parameter.
    * `NOT_EQUAL`: Evaluates as `True` only if the field of the given model is not equal (!=) to the parameter.
    * `IN`: Evaluates as `True` only if the field of the given model belongs (in) to the parameter (which must be a
        collection).
    * `LIKE`: Evaluates as `True` only if the field of the given model matches to the parameter _pattern.


    For example, to define a condition in which the `year` must be between `1994` and `2003` or the `color` must be
    `blue`, the condition can be writen as:

    .. code-block::

        Condition.OR(
            Condition.AND(Condition.GREATER_EQUAL("year", 1994), Condition.LOWER("year", 2003)),
            Condition.EQUAL("color", "blue")
        )

    """

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
    LIKE = _LikeCondition


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
    """Ordering class.

    This class provides the way to define ordering strategies for ``Model`` instances through the ``ASC`` and ``DESC``
    class methods, which retrieves instances containing the given information.

    For example, to define a descending ordering strategy by the `name` field:

    .. code-block::

        Ordering.DESC("name")

    """

    ASC = partial(_Ordering, reverse=False)
    DESC = partial(_Ordering, reverse=True)
