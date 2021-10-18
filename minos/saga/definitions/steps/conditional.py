from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Iterable,
    Optional,
    Union,
)

from minos.common import (
    classname,
)

from ...exceptions import (
    EmptySagaStepException,
    MultipleElseThenException,
)
from ..operations import (
    SagaOperation,
)
from .abc import (
    SagaStep,
)

if TYPE_CHECKING:
    from ...context import (
        SagaContext,
    )
    from ..saga import (
        Saga,
    )


class ConditionalSagaStep(SagaStep):
    """TODO"""

    def __init__(
        self,
        if_then: Optional[Union[IfThenAlternative, Iterable[IfThenAlternative]]] = None,
        else_then: Optional[ElseThenAlternative] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        if isinstance(if_then, IfThenAlternative):
            if_then = [if_then]

        if if_then is None:
            if_then = list()

        self.if_then_alternatives = list(if_then)
        self.else_then_alternative = else_then

    @classmethod
    def _from_raw(cls, raw: dict[str, Any], **kwargs) -> ConditionalSagaStep:
        current = raw | kwargs

        current["if_then"] = [IfThenAlternative.from_raw(alternative) for alternative in current["if_then"]]
        current["else_then"] = ElseThenAlternative.from_raw(current["else_then"])

        return cls(**current)

    def if_then(
        self, condition: Callable[[SagaContext], Union[bool, Awaitable[bool]]], saga: Saga
    ) -> ConditionalSagaStep:
        """TODO"""

        alternative = IfThenAlternative(condition, saga)
        self.if_then_alternatives.append(alternative)
        return self

    def else_then(self, saga: Saga) -> ConditionalSagaStep:
        """TODO"""

        if self.else_then_alternative is not None:
            raise MultipleElseThenException()

        alternative = ElseThenAlternative(saga)
        self.else_then_alternative = alternative
        return self

    def validate(self) -> None:
        """TODO"""

        if not len(self.if_then_alternatives) or self.else_then_alternative is None:
            raise EmptySagaStepException()

        for alternative in self.if_then_alternatives:
            alternative.validate()

        if self.else_then_alternative is not None:
            self.else_then_alternative.validate()

    @property
    def raw(self) -> dict[str, Any]:
        """TODO"""

        return {
            "cls": classname(type(self)),
            "if_then": [alternative.raw for alternative in self.if_then_alternatives],
            "else_then": None if self.else_then_alternative is None else self.else_then_alternative.raw,
        }

    def __iter__(self) -> Iterable:
        yield from (
            self.if_then_alternatives,
            self.else_then_alternative,
        )


class IfThenAlternative:
    """TODO"""

    def __init__(
        self, condition: Union[SagaOperation, Callable[[SagaContext], Union[bool, Awaitable[bool]]]], saga: Saga
    ):
        if not isinstance(condition, SagaOperation):
            condition = SagaOperation(condition)

        self.condition = condition
        self.saga = saga

    @classmethod
    def from_raw(cls, raw: Optional[Union[dict[str, Any], IfThenAlternative]], **kwargs) -> IfThenAlternative:
        """TODO"""

        from ..saga import (
            Saga,
        )

        if isinstance(raw, cls):
            return raw
        current = raw | kwargs

        current["condition"] = SagaOperation.from_raw(current["condition"])
        current["saga"] = Saga.from_raw(current["saga"])

        return cls(**current)

    def validate(self) -> None:
        """TODO"""
        return self.saga.validate()

    @property
    def raw(self) -> dict[str, Any]:
        """TODO"""

        return {
            "condition": self.condition.raw,
            "saga": self.saga.raw,
        }

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self):
        yield from (
            self.condition,
            self.saga,
        )


class ElseThenAlternative:
    """TODO"""

    def __init__(self, saga: Saga):
        self.saga = saga

    @classmethod
    def from_raw(cls, raw: Optional[Union[dict[str, Any], ElseThenAlternative]], **kwargs) -> ElseThenAlternative:
        """TODO"""

        from ..saga import (
            Saga,
        )

        if isinstance(raw, cls):
            return raw
        current = raw | kwargs

        current["saga"] = Saga.from_raw(current["saga"])

        return cls(**current)

    def validate(self) -> None:
        """TODO"""
        return self.saga.validate()

    @property
    def raw(self) -> dict[str, Any]:
        """TODO"""

        return {
            "saga": self.saga.raw,
        }

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self):
        yield from (self.saga,)
