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
        self, if_then: Optional[set[IfThenCondition]] = None, else_then: Optional[ElseThenCondition] = None, **kwargs
    ):
        super().__init__(**kwargs)

        if if_then is None:
            if_then = set()

        self.if_then_conditions = if_then
        self.else_then_condition = else_then

    @classmethod
    def _from_raw(cls, raw: dict[str, Any], **kwargs) -> ConditionalSagaStep:
        current = raw | kwargs

        current["if_then"] = {SagaOperation.from_raw(condition) for condition in current["if_then"]}
        current["else_then"] = ElseThenCondition.from_raw(current["else_then"])

        return cls(**current)

    def if_then(
        self, condition: Callable[[SagaContext], Union[bool, Awaitable[bool]]], saga: Saga
    ) -> ConditionalSagaStep:
        """TODO"""

        condition = SagaOperation(condition)
        condition = IfThenCondition(condition, saga)
        self.if_then_conditions.add(condition)
        return self

    def else_then(self, saga: Saga) -> ConditionalSagaStep:
        """TODO"""

        condition = ElseThenCondition(saga)
        self.else_then_condition = condition
        return self

    def validate(self) -> None:
        """TODO"""

    @property
    def raw(self) -> dict[str, Any]:
        """TODO"""

        return {
            "cls": classname(type(self)),
            "if_then": [condition.raw for condition in self.if_then_conditions],
            "else_then": None if self.else_then_condition is None else self.else_then_condition.raw,
        }

    def __iter__(self) -> Iterable:
        pass


class IfThenCondition:
    """TODO"""

    def __init__(self, condition: SagaOperation, saga: Saga):
        self.condition = condition
        self.saga = saga

    @classmethod
    def from_raw(cls, raw: Optional[Union[dict[str, Any], IfThenCondition]], **kwargs) -> IfThenCondition:
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

    @property
    def raw(self) -> dict[str, Any]:
        """TODO"""

        return {
            "condition": self.condition.raw,
            "saga": self.saga.raw,
        }


class ElseThenCondition:
    """TODO"""

    def __init__(self, saga: Saga):
        self.saga = saga

    @classmethod
    def from_raw(cls, raw: Optional[Union[dict[str, Any], ElseThenCondition]], **kwargs) -> ElseThenCondition:
        """TODO"""

        from ..saga import (
            Saga,
        )

        if isinstance(raw, cls):
            return raw
        current = raw | kwargs

        current["saga"] = Saga.from_raw(current["saga"])

        return cls(**current)

    @property
    def raw(self) -> dict[str, Any]:
        """TODO"""

        return {
            "saga": self.saga.raw,
        }
