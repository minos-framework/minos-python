"""minos.common.queries module."""

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
        return f"{type(self).__name__}({', '.join(map(str, self))})"


class _Condition(ABC):
    def __eq__(self, other) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable[Any]:
        yield from tuple()

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"


class _TrueCondition(_Condition):
    pass


class _FalseCondition(_Condition):
    pass


class _ComposedCondition(_Condition, ABC):
    pass

    def __init__(self, *parts: Iterable[_Condition]):
        self.parts = tuple(parts)

    def __iter__(self):
        yield from self.parts


class _AndCondition(_ComposedCondition):
    pass


class _OrCondition(_ComposedCondition):
    pass


class _NotCondition(_Condition):
    def __init__(self, inner: _Condition):
        self.inner = inner

    def __iter__(self) -> Iterable[Any]:
        yield from (self.inner,)


class _SimpleCondition(_Condition, ABC):
    pass

    def __init__(self, field: str, value: Any):
        self.field = field
        self.value = value

    def __iter__(self) -> Iterable[Any]:
        yield from (
            self.field,
            self.value,
        )


class _LowerCondition(_SimpleCondition):
    pass


class _LowerEqualCondition(_SimpleCondition):
    pass


class _GreaterCondition(_SimpleCondition):
    pass


class _GreaterEqualCondition(_SimpleCondition):
    pass


class _EqualCondition(_SimpleCondition):
    pass


class _NotEqualCondition(_SimpleCondition):
    pass


class _InCondition(_SimpleCondition):
    pass


_TRUE_CONDITION = _TrueCondition()
_FALSE_CONDITION = _FalseCondition()


class Ordering:
    ASC = partial(_Ordering, reverse=False)
    DESC = partial(_Ordering, reverse=True)


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
